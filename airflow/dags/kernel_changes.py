import os
import json
import logging
from datetime import timedelta
import itertools

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


failure_recipients = os.environ.get("EMIAL_ON_FAILURE_RECIPIENTS", None)
EMIAL_ON_FAILURE_RECIPIENTS = (
    failure_recipients.split(",") if failure_recipients else []
)

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
    dag_id="kernel_changes",
    default_args=default_args,
    schedule_interval=timedelta(minutes=1),
)

api_hook = HttpHook(http_conn_id="kernel_conn", method="GET")


@retry(wait=tenacity.wait_exponential(), stop=tenacity.stop_after_attempt(10))
def mongo_connect():
    # TODO: Necessário adicionar um commando para adicionar previamente uma conexão, ver: https://github.com/puckel/docker-airflow/issues/75
    conn = BaseHook.get_connection("opac_conn")

    uri = "mongodb://{creds}{host}{port}/{database}".format(
        creds="{}:{}@".format(conn.login, conn.password) if conn.login else "",
        host=conn.host,
        port="" if conn.port is None else ":{}".format(conn.port),
        database=conn.schema,
    )

    connect(host=uri, **conn.extra_dejson)


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


@retry(
    wait=tenacity.wait_exponential(),
    stop=tenacity.stop_after_attempt(10),
    retry=tenacity.retry_if_exception_type(requests.exceptions.ConnectionError),
)
def fetch_data(endpoint):
    """
    Obtém o JSON do endpoint do Kernel
    """
    return api_hook.run(endpoint=endpoint).json()


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
    _, _entity, _id = endpoint.split("/")

    return (_entity, _id)


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
        journal.editor_email = contact.get("email", "").split(";")[0].strip()

    journal.online_submission_url = metadata.get("online_submission_url", "")
    journal.logo_url = metadata.get("logo_url", "")
    journal.current_status = metadata.get("status", {}).get("status")

    journal.created = metadata.get("created")
    journal.updated = metadata.get("updated")

    return journal


def register_journals(ds, **kwargs):
    mongo_connect()
    tasks = kwargs["ti"].xcom_pull(key="tasks", task_ids="read_changes_task")

    journal_changes = filter_changes(tasks, "journals", "get")

    # Dictionary with id of journal and list of issues, something like: known_issues[journal_id] = [issue_id, issue_id, ....]
    known_issues = {}

    for journal in journal_changes:
        resp_json = fetch_journal(get_id(journal.get("id")))

        t_journal = JournalFactory(resp_json)
        t_journal.save()

        known_issues[get_id(journal.get("id"))] = resp_json.get("items", [])

    kwargs["ti"].xcom_push(key="known_issues", value=known_issues)

    return tasks


register_journals_task = PythonOperator(
    task_id="register_journals_task",
    provide_context=True,
    python_callable=register_journals,
    dag=dag,
)


def register_issue(data, journal_id, issue_order):
    """
    Realiza o registro fascículo utilizando o opac schema.

    Esta função pode lançar a exceção `models.Journal.DoesNotExist`.
    """
    mongo_connect()

    journal = models.Journal.objects.get(_id=journal_id)

    metadata = data["metadata"]

    issue = models.Issue()
    issue._id = issue.iid = data.get("id")
    issue._id = issue.iid = data.get("id")
    issue.type = metadata.get("type", "regular")
    issue.spe_text = metadata.get("spe_text", "")
    issue.start_month = metadata.get("publication_month", 0)
    issue.end_month = metadata.get("publication_season", [0])[-1]
    issue.year = metadata.get("publication_year")
    issue.volume = metadata.get("volume", "")
    issue.number = metadata.get("number", "")

    issue.label = metadata.get(
        "label", "%s%s" % ("v" + issue.volume, "n" + issue.number)
    )
    issue.order = metadata.get("order", 0)
    issue.pid = metadata.get("pid", "")

    issue.journal = journal
    issue.order = issue_order
    issue.save()

    return issue


def register_issues(ds, **kwargs):
    """Registra ou atualiza todos os fascículos a partir do Kernel.

    Fascículos de periódicos não encontrados são marcados como órfãos e 
    armazenados em uma variável persistente para futuras tentativas.
    """
    tasks = kwargs["ti"].xcom_pull(key="tasks", task_ids="read_changes_task")
    known_issues = kwargs["ti"].xcom_pull(key="known_issues", task_ids="register_journals_task")

    def _journal_id(issue_id):
        """Obtém o identificador do periódico onde `issue_id` está contido."""
        j = [
            journal_id for journal_id, issues in known_issues.items() if issue_id in issues
        ]
        try:
            return j[0]
        except IndexError:
            return None

    def _load_orphans():
        return json.loads(Variable.get("orphan_issues", "[]"))

    def _issue_order(issue_id, journal_id):
        """A posição em relação aos demais fascículos do periódico.
        
        Pode levantar `ValueError` caso `issue_id` não conste na relação de 
        fascículos do periódico `journal_id`.
        """
        return known_issues.get(journal_id, []).index(issue_id)

    i_documents = {}
    orphan_issues = []

    for issue_id in itertools.chain(
        _load_orphans(),
        (get_id(task["id"]) for task in filter_changes(tasks, "bundles", "get")),
    ):
        if _journal_id(issue_id) is not None:
            data = fetch_bundles(issue_id)
            try:
                register_issue(
                    data,
                    _journal_id(issue_id),
                    _issue_order(issue_id, _journal_id(issue_id)),
                )
            except models.Journal.DoesNotExist:
                orphan_issues.append(issue_id)
            else:
                i_documents[issue_id] = data.get("items", [])
        else:
            orphan_issues.append(issue_id)

    kwargs["ti"].xcom_push(key="i_documents", value=i_documents)
    Variable.set("orphan_issues", json.dumps(orphan_issues))

    return tasks


register_issues_task = PythonOperator(
    task_id="register_issues_task",
    provide_context=True,
    python_callable=register_issues,
    dag=dag,
)


def register_document(data, issue_id, document_id, i_documents):
    """
    Esta função pode lançar a exceção `models.Issue.DoesNotExist`.
    """

    def nestget(data, *path, default=""):
        """
        Obtém valores de list ou dicionários.
        """
        for key_or_index in path:
            try:
                data = data[key_or_index]
            except (KeyError, IndexError):
                return default
        return data

    article = nestget(data, "article", 0)
    article_meta = nestget(data, "article_meta", 0)
    pub_date = nestget(data, "pub_date", 0)
    sub_articles = nestget(data, "sub_article")
    contribs = nestget(data, "contrib")

    document = models.Article()

    document.title = nestget(article_meta, "article_title", 0)
    document.section = nestget(article_meta, "pub_subject", 0)

    authors = []

    valid_contrib_types = [
        "author",
        "editor",
        "organizer",
        "translator",
        "autor",
        "compiler",
    ]

    for contrib in contribs:

        if nestget(contrib, "contrib_type", 0) in valid_contrib_types:
            authors.append(
                "%s, %s"
                % (
                    nestget(contrib, "contrib_surname", 0),
                    nestget(contrib, "contrib_given_names", 0),
                )
            )

    document.authors = authors

    document.abstract = nestget(article_meta, "abstract", 0)

    publisher_id = nestget(article_meta, "article_publisher_id", 0)

    document._id = publisher_id
    document.aid = publisher_id
    document.pid = nestget(article_meta, "article_publisher_id", 1)
    document.doi = nestget(article_meta, "article_doi", 0)

    original_lang = nestget(article, "lang", 0)

    # article.languages contém todas as traduções do artigo e o idioma original
    languages = [original_lang]
    trans_titles = []
    trans_sections = []
    trans_abstracts = []

    trans_sections.append(
        models.TranslatedSection(
            **{
                "name": nestget(article_meta, "pub_subject", 0),
                "language": original_lang,
            }
        )
    )

    trans_abstracts.append(
        models.Abstract(**{"text": document.abstract, "language": original_lang})
    )

    if data.get("trans_abstract"):

        for trans_abs in data.get("trans_abstract"):
            trans_abstracts.append(
                models.Abstract(
                    **{
                        "text": nestget(trans_abs, "text", 0),
                        "language": nestget(trans_abs, "lang", 0),
                    }
                )
            )

    keywords = []
    for sub in sub_articles:
        lang = nestget(sub, "article", 0, "lang", 0)

        languages.append(lang)

        trans_titles.append(
            models.TranslatedTitle(
                **{
                    "name": nestget(sub, "article_meta", 0, "article_title", 0),
                    "language": lang,
                }
            )
        )

        trans_sections.append(
            models.TranslatedSection(
                **{
                    "name": nestget(sub, "article_meta", 0, "pub_subject", 0),
                    "language": lang,
                }
            )
        )

        trans_abstracts.append(
            models.Abstract(
                **{
                    "text": nestget(sub, "article_meta", 0, "abstract_p", 0),
                    "language": lang,
                }
            )
        )

    if data.get("kwd_group"):

        for kwd_group in nestget(data, "kwd_group"):

            keywords.append(
                models.ArticleKeyword(
                    **{
                        "keywords": nestget(kwd_group, "kwd", default=[]),
                        "language": nestget(kwd_group, "lang", 0),
                    }
                )
            )

    document.languages = languages
    document.translated_titles = trans_titles
    document.sections = trans_sections
    document.abstracts = trans_abstracts
    document.keywords = keywords
    document.abstract_languages = [
        trans_abs["language"] for trans_abs in trans_abstracts
    ]

    document.original_language = original_lang

    document.publication_date = nestget(pub_date, "text", 0)

    document.type = nestget(article, "type", 0)
    document.elocation = nestget(article_meta, "pub_elocation", 0)
    document.fpage = nestget(article_meta, "pub_fpage", 0)
    document.fpage_sequence = nestget(article_meta, "pub_fpage_seq", 0)
    document.lpage = nestget(article_meta, "pub_lpage", 0)

    issue = models.Issue.objects.get(_id=issue_id)

    document.issue = issue
    document.journal = issue.journal

    document.order = i_documents.get(issue.id).index(document_id)
    document.xml = "%s/documents/%s" % (api_hook.base_url, document._id)

    document.save()

    return document


def register_orphan_documents(ds, **kwargs):
    """
    Registrar os documentos orfãos.
    """

    i_documents = kwargs["ti"].xcom_pull(
        key="i_documents", task_ids="register_issues_task"
    )

    orphan_documents = []

    for document_id in json.loads(Variable.get("orphan_documents", "[]")):

        issue_id = [i for i, d in i_documents.items() if document_id in d]

        if issue_id:
            resp_json = fetch_documents_front(document_id)
            try:
                register_document(resp_json, issue_id[0], document_id, i_documents)
            except models.Issue.DoesNotExist:
                orphan_documents.append(document_id)
        else:
            orphan_documents.append(document_id)

    Variable.set("orphan_documents", json.dumps(orphan_documents))


register_orphan_documents_task = PythonOperator(
    task_id="register_orphan_documents",
    provide_context=True,
    python_callable=register_orphan_documents,
    dag=dag,
)


def register_documents(ds, **kwargs):
    mongo_connect()

    tasks = kwargs["ti"].xcom_pull(key="tasks", task_ids="read_changes_task")

    i_documents = kwargs["ti"].xcom_pull(
        key="i_documents", task_ids="register_issues_task"
    )

    orphan_documents = []

    document_changes = filter_changes(tasks, "documents", "get")

    for document in document_changes:
        document_id = get_id(document.get("id"))
        issue_id = [i for i, d in i_documents.items() if document_id in d]

        if issue_id:
            resp_json = fetch_documents_front(document_id)
            try:
                register_document(resp_json, issue_id[0], document_id, i_documents)
            except models.Issue.DoesNotExist:
                orphan_documents.append(document_id)
        else:
            orphan_documents.append(document_id)

    Variable.set("orphan_documents", json.dumps(orphan_documents))
    return tasks


register_documents_task = PythonOperator(
    task_id="register_documents_task",
    provide_context=True,
    python_callable=register_documents,
    dag=dag,
)


def delete_documents(ds, **kwargs):
    tasks = kwargs["ti"].xcom_pull(key="tasks", task_ids="read_changes_task")

    document_changes = filter_changes(tasks, "documents", "delete")

    for document in document_changes:

        article = models.Article.objects.get(_id=get_id(document.get("id")))
        article.is_public = False
        article.save()

    return tasks


delete_documents_task = PythonOperator(
    task_id="delete_documents_task",
    provide_context=True,
    python_callable=delete_documents,
    dag=dag,
)


def delete_issues(ds, **kwargs):
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
                models.Issue.objects.filter(journal=journal._id)
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

register_last_issues_task << register_issues_task

register_orphan_documents_task << register_last_issues_task

register_documents_task << register_orphan_documents_task

delete_journals_task << register_documents_task

delete_issues_task << delete_journals_task

delete_documents_task << delete_issues_task
