import os
import re
import json
import logging
from datetime import timedelta
import itertools
from typing import Dict, List, Tuple

import tenacity
from tenacity import retry

import airflow
from airflow import DAG
from airflow.sensors.http_sensor import HttpSensor
from airflow.hooks.http_hook import HttpHook
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator

import requests

from mongoengine import connect

from opac_schema.v1 import models

from operations.sync_kernel_to_website_operations import (
    try_register_documents,
    ArticleFactory,
    ArticleRenditionFactory,
    try_register_documents_renditions,
)
from common.hooks import mongo_connect, kernel_connect

failure_recipients = os.environ.get("EMIAL_ON_FAILURE_RECIPIENTS", None)
EMIAL_ON_FAILURE_RECIPIENTS = (
    failure_recipients.split(",") if failure_recipients else []
)

EMAIL_SPLIT_REGEX = re.compile("[;\\/]+")

default_args = {
    "owner": "airflow",
    "start_date": airflow.utils.dates.days_ago(2),
    "provide_context": True,
    "email_on_failure": True,
    "email_on_retry": True,
    "depends_on_past": False,
    "email": EMIAL_ON_FAILURE_RECIPIENTS,
}

dag = DAG(
    dag_id="sync_kernel_to_website",
    default_args=default_args,
    schedule_interval=None,
)


class EnqueuedState:
    task = "get"

    def on_event(self, event):
        if event == "deleted":
            return DeletedState()

        return self


class DeletedState:
    task = "delete"

    def on_event(self, event):
        if event == "modified":
            return EnqueuedState()

        return self


class Machine:
    def __init__(self):
        self.state = EnqueuedState()

    def on_event(self, event):
        self.state = self.state.on_event(event)

    def task(self):
        return self.state.task


class Reader:
    def read(self, log):
        entities, timestamp = self._process_events(log)
        return (
            [{"id": id, "task": state.task()} for id, state in entities.items()],
            timestamp,
        )

    def _process_events(self, log):
        entities = {}
        last_timestamp = None
        for entry in log:
            last_timestamp = entry["timestamp"]
            id = entities.setdefault(entry["id"], Machine())
            if entry.get("deleted", False):
                event = "deleted"
            else:
                event = "modified"
            id.on_event(event)

        return entities, last_timestamp


def fetch_data(endpoint):
    """
    Obtém o JSON do endpoint do Kernel
    """
    return kernel_connect(endpoint=endpoint, method="GET").json()


def fetch_changes(since):
    """
         Obtém o JSON das mudanças do Kernel com base no parametro 'since'
    """
    return fetch_data("/changes?since=%s" % (since))


def fetch_journal(journal_id):
    """
         Obtém o JSON do Journal do Kernel com base no parametro 'journal_id'
    """
    return fetch_data("/journals/%s" % (journal_id))


def fetch_bundles(bundle_id):
    """
         Obtém o JSON do DocumentBundle do Kernel com base no parametro 'bundle_id'
    """
    return fetch_data("/bundles/%s" % (bundle_id))


def fetch_documents_front(document_id):
    """
         Obtém o JSON do Document do Kernel com base no parametro 'document_id'
    """
    return fetch_data("/documents/%s/front" % (document_id))


def fetch_documents_renditions(document_id: str) -> List[Dict]:
    """Obtém o uma lista contendo as representações de um documento

    Args:
        document_id (str): Identificador único de um documento

    Returns:
        renditions (List[Dict])
    """
    return fetch_data("/documents/%s/renditions" % (document_id))


def changes(since=""):
    """Verifies if change's endpoint has new modifications.
    If none modification was found returns an empty generator. If
    modifications are found returns a generator that produces
    a list with every modification as dictionary
    {'id: '...', 'timestamp': '..'}"""

    last_yielded = None

    while True:
        resp_json = fetch_changes(since)
        has_changes = False

        for result in resp_json["results"]:
            last_yielded = result
            has_changes = True
            yield result

        if not has_changes:
            return
        else:
            since = last_yielded["timestamp"]


def read_changes(ds, **kwargs):
    """Looks for newly modifications since `change timestamp`.
    If modifications are found this function push a list of task
    to `xcom` workspace. If none modifications are found the
    change_timestamp variable would not be updated."""

    reader = Reader()
    variable_timestamp = Variable.get("change_timestamp", "")
    tasks, timestamp = reader.read(changes(since=variable_timestamp))

    if timestamp is None or timestamp == variable_timestamp:
        return False

    kwargs["ti"].xcom_push(key="tasks", value=tasks)
    Variable.set("change_timestamp", timestamp)
    return timestamp


def get_entity(endpoint):
    """
    Return the entity of a kernel endpoint

    Example param: /journals/8767-8766-12-32-2

    Return: journals
    """
    _entity, _id = parser_endpoint(endpoint)

    return _entity


def get_id(endpoint):
    """
    Return the id of a kernel endpoint

    Example param: /journals/8767-9988-01-02-2

    Return: 8767-9988-01-02-2
    """
    _entity, _id = parser_endpoint(endpoint)

    return _id


def parser_endpoint(endpoint):
    """
    Parser the endpoint:

    Example param: /journals/0000-0000-00-00-2

    Return: (journals, 0000-0000-00-00-2)

    """
    patterns = [
        r"^\/(?P<entity>journals)\/(?P<id>[-\w]+)$",
        r"^\/(?P<entity>bundles)\/(?P<id>[-\w\.]+)$",
        r"^\/(?P<entity>documents)\/(?P<id>[-\w]+)$",
        r"^\/documents\/(?P<id>[-\w]+)\/(?P<entity>renditions)$",
    ]

    for pattern in patterns:
        matched = re.match(pattern, endpoint)

        if matched:
            groups = matched.groupdict()
            return (groups["entity"], groups["id"])


def filter_changes(tasks, entity, action):
    """
    Filter changes

    Return a list of items that matched by criteria ``entity`` and ``action``
    """

    for task in tasks:
        _entity = get_entity(task["id"])
        if _entity == entity and task.get("task") == action:
            yield task


http_kernel_check = HttpSensor(
    task_id="http_kernel_check",
    http_conn_id="kernel_conn",
    endpoint="/changes",
    request_params={},
    poke_interval=5,
    dag=dag,
)


read_changes_task = ShortCircuitOperator(
    task_id="read_changes_task",
    provide_context=True,
    python_callable=read_changes,
    dag=dag,
)


def JournalFactory(data):
    """Produz instância de `models.Journal` a partir dos dados retornados do
    endpoint `/journals/:journal_id` do Kernel.
    """
    metadata = data["metadata"]

    try:
        journal = models.Journal.objects.get(_id=data.get("id"))
    except models.Journal.DoesNotExist:
        journal = models.Journal()

    journal._id = journal.jid = data.get("id")
    journal.title = metadata.get("title", "")
    journal.title_iso = metadata.get("title_iso", "")
    journal.short_title = metadata.get("short_title", "")
    journal.acronym = metadata.get("acronym", "")
    journal.scielo_issn = metadata.get("scielo_issn", "")
    journal.print_issn = metadata.get("print_issn", "")
    journal.eletronic_issn = metadata.get("electronic_issn", "")

    # Subject Categories
    journal.subject_categories = metadata.get("subject_categories", [])

    # Métricas
    journal.metrics = models.JounalMetrics(**metadata.get("metrics", {}))

    # Issue count
    journal.issue_count = len(data.get("items", []))

    # Mission
    journal.mission = [
        models.Mission(**{"language": m["language"], "description": m["value"]})
        for m in metadata.get("mission", [])
    ]

    # Study Area
    journal.study_areas = metadata.get("subject_areas", [])

    # Sponsors
    sponsors = metadata.get("sponsors", [])
    journal.sponsors = [s["name"] for s in sponsors if sponsors]

    # TODO: Verificar se esse e-mail é o que deve ser colocado no editor.
    # Editor mail
    if metadata.get("contact", ""):
        contact = metadata.get("contact")
        journal.editor_email = EMAIL_SPLIT_REGEX.split(contact.get("email", ""))[
            0
        ].strip()
        journal.publisher_address = contact.get("address")

    if metadata.get("institution_responsible_for"):
        institutions = [
            item
            for item in metadata.get("institution_responsible_for", [{}])
            if item.get("name")
        ]
        if institutions:
            journal.publisher_name = ', '.join(
                item.get("name")
                for item in institutions
            )
            institution = institutions[0]
            journal.publisher_city = institution.get("city")
            journal.publisher_state = institution.get("state")
            journal.publisher_country = institution.get("country")

    journal.online_submission_url = metadata.get("online_submission_url", "")
    if journal.logo_url is None or len(journal.logo_url) == 0:
        journal.logo_url = metadata.get("logo_url", "")
    journal.current_status = metadata.get("status_history", [{}])[-1].get("status")

    journal.created = data.get("created", "")
    journal.updated = data.get("updated", "")

    return journal


def register_journals(ds, **kwargs):
    mongo_connect()
    tasks = kwargs["ti"].xcom_pull(key="tasks", task_ids="read_changes_task")

    journal_changes = filter_changes(tasks, "journals", "get")

    # Dictionary with id of journal and list of issues, something like: known_issues[journal_id] = [issue_id, issue_id, ....]
    known_issues = {}

    # Dictionary with id of journal and aop of the jounal, something like:
    # journals_aop = {'journal_id' = 'aop_id', 'journal_id' = 'aop_id', ....}
    journals_aop = {}

    for journal in journal_changes:
        resp_json = fetch_journal(get_id(journal.get("id")))

        t_journal = JournalFactory(resp_json)
        t_journal.save()

        known_issues[get_id(journal.get("id"))] = resp_json.get("items", [])

        if resp_json.get("aop"):
            journals_aop[resp_json.get("aop", "")] = get_id(journal.get("id"))

    kwargs["ti"].xcom_push(key="known_issues", value=known_issues)
    kwargs["ti"].xcom_push(key="journals_aop", value=journals_aop)

    return tasks


register_journals_task = PythonOperator(
    task_id="register_journals_task",
    provide_context=True,
    python_callable=register_journals,
    dag=dag,
)


def IssueFactory(data, journal_id, issue_order=None, _type="regular"):
    """
    Realiza o registro fascículo utilizando o opac schema.

    Esta função pode lançar a exceção `models.Journal.DoesNotExist`.

    Para satisfazer a obrigatoriedade do ano para os "Fascículos" ahead, estamos fixando o ano de fascículos do tipo ``ahead`` com o valor 9999
    """
    mongo_connect()

    metadata = data["metadata"]

    try:
        issue = models.Issue.objects.get(_id=data["id"])
    except models.Issue.DoesNotExist:
        issue = models.Issue()
    else:
        journal_id = journal_id or issue.journal._id
        _type = "ahead" if _type == "ahead" or data["id"].endswith("-aop") else _type

    issue._id = issue.iid = data["id"]
    issue.spe_text = metadata.get("spe_text", "")
    issue.start_month = metadata.get("publication_months", {"range": [0, 0]}).get("range", [0])[0]
    issue.end_month = metadata.get("publication_months", {"range": [0, 0]}).get("range", [0])[-1]

    if _type == "ahead":
        issue.year = issue.year or "9999"
        issue.number = issue.number or "ahead"
    else:
        issue.year = metadata.get("publication_year", issue.year)
        issue.number = metadata.get("number", issue.number)

    issue.volume = metadata.get("volume", "")

    if issue_order:
        issue.order = issue_order

    issue.pid = metadata.get("pid", "")
    issue.journal = models.Journal.objects.get(_id=journal_id)

    def _get_issue_label(metadata: dict) -> str:
        """Produz o label esperado pelo OPAC de acordo com as regras aplicadas
        pelo OPAC Proc e Xylose.

        Args:
            metadata (dict): conteúdo de um bundle

        Returns:
            str: label produzido a partir de um bundle
        """

        START_REGEX = re.compile("^0")
        END_REGEX = re.compile("0$")

        label_number = metadata.get("number", "")
        label_volume = metadata.get("volume", "")
        label_supplement = (
            " suppl %s" % metadata.get("supplement", "")
            if metadata.get("supplement", "")
            else ""
        )

        if label_number:
            label_number += label_supplement
            label_number = START_REGEX.sub("", label_number)
            label_number = END_REGEX.sub("", label_number)
            label_number = label_number.strip()

        return "".join(["v" + label_volume, "n" + label_number])

    issue.label = _get_issue_label(metadata)

    if metadata.get("supplement"):
        issue.suppl_text = metadata.get("supplement")
        issue.type = "supplement"
    elif issue.volume and not issue.number:
        issue.type = "volume_issue"
    elif issue.number and "spe" in issue.number:
        issue.type = "special"
    else:
        issue.type = _type

    issue.created = data.get("created", "")
    issue.updated = data.get("updated", "")

    return issue


def try_register_issues(
    issues, get_journal_id, get_issue_order, fetch_data, issue_factory, is_aop=False
):
    """Registra uma coleção de fascículos.

    Retorna a dupla: lista dos fascículos que não foram registrados por
    serem órfãos, e dicionário com os documentos conhecidos.

    :param issues: lista de identificadores dos fascículos a serem registrados.
    :param get_journal_id: função que recebe o identificador de um fascículo no
    Kernel e retorna o identificador do periódico que o contém.
    :param get_aop_id: função que recebe o identificador de um fascículo e
    retorna o identificador do bundle que representa um ahead of print.
    :param get_issue_order: função que recebe o identificador de um fascículo e
    retorna um número inteiro referente a posição do fascículo em relação
    aos demais.
    :param fetch_data: função que recebe o identificador de um fascículo e
    retorna seus dados, em estruturas do Python, conforme retornado pelo
    endpoint do Kernel.
    :param issue_factory: função que recebe os dados retornados da função
    `fetch_data` e retorna uma instância da classe `Issue`, do `opac_schema`.
    :param is_aop: booleano responsável por indicar se é cadastro de AOP ou fascículo regular, valor padrão False.
    """
    known_documents = {}
    orphans = []

    for issue_id in issues:
        journal_id = get_journal_id(issue_id)
        logging.info('Registering issue "%s" to journal "%s"', issue_id, journal_id)
        data = fetch_data(issue_id)
        try:
            if not is_aop:
                issue = issue_factory(data, journal_id, get_issue_order(issue_id))
            else:
                # Não é necessário o campo de ordenação(order) no ahead
                issue = issue_factory(data, journal_id, _type="ahead")
            issue.save()
        except models.Journal.DoesNotExist:
            orphans.append(issue_id)
        else:
            known_documents[issue_id] = data.get("items", [])

    return list(set(orphans)), known_documents


def register_issues(ds, **kwargs):
    """Registra ou atualiza todos os fascículos a partir do Kernel.

    Fascículos de periódicos não encontrados são marcados como órfãos e
    armazenados em uma variável persistente para futuras tentativas.
    """
    tasks = kwargs["ti"].xcom_pull(key="tasks", task_ids="read_changes_task")
    known_issues = kwargs["ti"].xcom_pull(
        key="known_issues", task_ids="register_journals_task"
    )
    journals_aop = kwargs["ti"].xcom_pull(
        key="journals_aop", task_ids="register_journals_task"
    )

    def _journal_id(issue_id):
        """Obtém o identificador do periódico onde `issue_id` está contido."""
        for journal_id, issues in known_issues.items():
            for issue in issues:
                if issue_id == issue["id"]:
                    return journal_id

    def _issue_order(issue_id):
        """A posição em relação aos demais fascículos do periódico.

        Pode levantar `ValueError` caso `issue_id` não conste na relação de
        fascículos do periódico `journal_id`.
        """
        issues = known_issues.get(_journal_id(issue_id), [])
        for issue in issues:
            if issue_id == issue["id"]:
                return issue["order"]

    def _journal_aop_id(aop_id):
        """Obtém o identificador do periódico a partir da lista de AOPs."""
        return journals_aop[aop_id]

    issues_to_get = itertools.chain(
        Variable.get("orphan_issues", default_var=[], deserialize_json=True),
        (get_id(task["id"]) for task in filter_changes(tasks, "bundles", "get")),
    )

    # Cadastra os AOPs
    # No caso dos aops não é obrigatório o atributo order
    orphans, known_documents = try_register_issues(
        journals_aop.keys(), _journal_aop_id, None, fetch_bundles, IssueFactory, True
    )

    # Cadastra os fascículos regulares
    orphans, known_documents = try_register_issues(
        issues_to_get, _journal_id, _issue_order, fetch_bundles, IssueFactory
    )

    kwargs["ti"].xcom_push(key="i_documents", value=known_documents)
    Variable.set("orphan_issues", orphans, serialize_json=True)

    return tasks


register_issues_task = PythonOperator(
    task_id="register_issues_task",
    provide_context=True,
    python_callable=register_issues,
    dag=dag,
)


def register_documents(**kwargs):
    """Registra documentos na base de dados do OPAC a partir de
    informações vindas da API do `Kernel`. Armazena como órfãos nas variáveis
    do Airflow os documentos que não puderam ser salvos."""

    mongo_connect()

    tasks = kwargs["ti"].xcom_pull(key="tasks", task_ids="read_changes_task")

    def _get_relation_data(document_id: str) -> Tuple[str, Dict]:
        """Recupera informações sobre o relacionamento entre o
        DocumentsBundle e o Document.

        Retorna uma tupla contendo o identificador da issue onde o
        documento está relacionado e o item do relacionamento.

        >> _get_relation_data("67TH7T7CyPPmgtVrGXhWXVs")
        ('0034-8910-2019-v53', {'id': '67TH7T7CyPPmgtVrGXhWXVs', 'order': '01'})

        :param document_id: Identificador único de um documento
        """

        for issue_id, items in known_documents.items():
            for item in items:
                if document_id == item["id"]:
                    return (issue_id, item)

        return (None, {})

    def _get_known_documents(**kwargs) -> Dict[str, List[str]]:
        """Recupera a lista de todos os documentos que estão relacionados com
        um `DocumentsBundle`.

        Levando em consideração que a DAG que detecta mudanças na API do Kernel
        roda de forma assíncrona em relação a DAG de espelhamento/sincronização.

        É possível que algumas situações especiais ocorram onde em uma rodada
        **anterior** o **evento de registro** de um `Document` foi capturado mas a
        atualização de seu `DocumentsBundle` não ocorreu (elas ocorrem em transações
        distintas e possuem timestamps também distintos). O documento será
        registrado como **órfão** e sua `task` não será processada na próxima
        execução.

        Na próxima execução a task `register_issue_task` entenderá que o
        `bundle` é órfão e não conhecerá os seus documentos (known_documents)
        e consequentemente o documento continuará órfão.

        Uma solução para este problema é atualizar a lista de documentos
        conhecidos a partir da lista de eventos de `get` de `bundles`.
        """

        known_documents = kwargs["ti"].xcom_pull(
            key="i_documents", task_ids="register_issues_task"
        )

        issues_recently_updated = [
            get_id(task["id"]) for task in filter_changes(tasks, "bundles", "get")
            if known_documents.get(get_id(task["id"])) is None
        ]

        for issue_id in issues_recently_updated:
            known_documents.setdefault(issue_id, [])
            known_documents[issue_id] = list(
                itertools.chain(
                    known_documents[issue_id], fetch_bundles(issue_id).get("items", [])
                )
            )
        return known_documents

    known_documents = _get_known_documents(**kwargs)

    # TODO: Em caso de um update no document é preciso atualizar o registro
    # Precisamos de uma nova task?

    documents_to_get = itertools.chain(
        Variable.get("orphan_documents", default_var=[], deserialize_json=True),
        (get_id(task["id"]) for task in filter_changes(tasks, "documents", "get")),
    )

    orphans = try_register_documents(
        documents_to_get, _get_relation_data, fetch_documents_front, ArticleFactory
    )

    Variable.set("orphan_documents", orphans, serialize_json=True)


register_documents_task = PythonOperator(
    task_id="register_documents_task",
    provide_context=True,
    python_callable=register_documents,
    dag=dag,
)


def register_documents_renditions(**kwargs):
    """Registra as manifestações de documentos processados na base de dados
    do OPAC"""

    mongo_connect()

    tasks = kwargs["ti"].xcom_pull(key="tasks", task_ids="read_changes_task")

    renditions_to_get = itertools.chain(
        Variable.get("orphan_renditions", default_var=[], deserialize_json=True),
        (get_id(task["id"]) for task in filter_changes(tasks, "renditions", "get")),
    )

    orphans = try_register_documents_renditions(
        renditions_to_get, fetch_documents_renditions, ArticleRenditionFactory
    )

    Variable.set("orphan_renditions", orphans, serialize_json=True)


register_documents_renditions_task = PythonOperator(
    task_id="register_documents_renditions_task",
    provide_context=True,
    python_callable=register_documents_renditions,
    dag=dag,
)


def delete_documents(ds, **kwargs):
    mongo_connect()
    tasks = kwargs["ti"].xcom_pull(key="tasks", task_ids="read_changes_task")

    document_changes = filter_changes(tasks, "documents", "delete")

    for document in document_changes:

        try:
            article = models.Article.objects.get(_id=get_id(document.get("id")))
            article.is_public = False
            article.save()
        except models.Article.DoesNotExist:
            logging.info(
                "Could not delete document '%s' "
                "it does not exist in Website database",
                get_id(document.get("id")),
            )

    return tasks


delete_documents_task = PythonOperator(
    task_id="delete_documents_task",
    provide_context=True,
    python_callable=delete_documents,
    dag=dag,
)


def delete_issues(ds, **kwargs):
    mongo_connect()
    tasks = kwargs["ti"].xcom_pull(key="tasks", task_ids="read_changes_task")

    issue_changes = filter_changes(tasks, "bundles", "delete")

    for issue in issue_changes:

        issue = models.Issue.objects.get(_id=get_id(issue.get("id")))
        issue.is_public = False
        issue.save()

    return tasks


delete_issues_task = PythonOperator(
    task_id="delete_issues_task",
    provide_context=True,
    python_callable=delete_issues,
    dag=dag,
)


def delete_journals(ds, **kwargs):
    tasks = kwargs["ti"].xcom_pull(key="tasks", task_ids="read_changes_task")

    journal_changes = filter_changes(tasks, "journals", "delete")

    for journal in journal_changes:

        journal = models.Journal.objects.get(_id=get_id(journal.get("id")))
        journal.is_public = False
        journal.save()

    return tasks


delete_journals_task = PythonOperator(
    task_id="delete_journals_task",
    provide_context=True,
    python_callable=delete_journals,
    dag=dag,
)


def register_last_issues(ds, **kwargs):
    mongo_connect()

    for journal in models.Journal.objects.all():
        try:
            logging.info("Id do journal: %s" % journal._id)
            last_j_issue = (
                models.Issue.objects.filter(journal=journal._id, is_public=True)
                .order_by("-year", "-order")
                .first()
                .select_related()
            )

            l_issue_sec = []
            if hasattr(last_j_issue, "sections"):
                l_issue_sec = last_j_issue.sections

            last_issue = {"sections": l_issue_sec}

            if hasattr(last_j_issue, "volume"):
                last_issue["volume"] = last_j_issue.volume

            if hasattr(last_j_issue, "iid"):
                last_issue["iid"] = last_j_issue.iid

            if hasattr(last_j_issue, "number"):
                last_issue["number"] = last_j_issue.number

            if hasattr(last_j_issue, "start_month"):
                last_issue["start_month"] = last_j_issue.start_month

            if hasattr(last_j_issue, "end_month"):
                last_issue["end_month"] = last_j_issue.end_month

            if hasattr(last_j_issue, "label"):
                last_issue["label"] = last_j_issue.label

            if hasattr(last_j_issue, "year"):
                last_issue["year"] = last_j_issue.year

            if hasattr(last_j_issue, "type"):
                last_issue["type"] = last_j_issue.type

            if hasattr(last_j_issue, "suppl_text"):
                last_issue["suppl_text"] = last_j_issue.suppl_text

            journal.last_issue = models.LastIssue(**last_issue)
            journal.save()
        except AttributeError:
            logging.info("No issues are registered to models.Journal: %s " % journal)


register_last_issues_task = PythonOperator(
    task_id="register_last_issues",
    provide_context=True,
    python_callable=register_last_issues,
    dag=dag,
)

http_kernel_check >> read_changes_task

register_journals_task << read_changes_task

register_issues_task << register_journals_task

register_documents_task << register_issues_task

register_documents_renditions_task << register_documents_task

delete_journals_task << register_documents_renditions_task

delete_issues_task << delete_journals_task

delete_documents_task << delete_issues_task

register_last_issues_task << delete_documents_task
