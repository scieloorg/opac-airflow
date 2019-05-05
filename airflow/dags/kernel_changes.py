import os
import logging
from datetime import timedelta

import tenacity
from tenacity import retry

import airflow
from airflow import DAG
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


def changes(since=""):
    last_yielded = None

    api_hook = HttpHook(http_conn_id="kernel_conn", method="GET")

    while True:

        url = "changes?since=%s" % since

        resp_json = api_hook.run(endpoint=url).json()

        for result in resp_json["results"]:
            if result != last_yielded:
                last_yielded = result
                yield result
            else:
                continue

        if since == last_yielded["timestamp"]:
            return
        else:
            since = last_yielded["timestamp"]


@retry(
    wait=tenacity.wait_exponential(),
    stop=tenacity.stop_after_attempt(10),
    retry=tenacity.retry_if_exception_type(requests.exceptions.ConnectionError),
)
def read_changes(ds, **kwargs):
    reader = Reader()
    variable_timestamp = Variable.get("change_timestamp", "")
    tasks, timestamp = reader.read(changes(since=variable_timestamp))
    if timestamp == variable_timestamp:
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


def transform_journal(data):
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
    journal.current_status = metadata.get("status").get("status")

    journal.created = metadata.get("created")
    journal.created = metadata.get("updated")

    return journal


read_changes_task = ShortCircuitOperator(
    task_id="read_changes_task",
    provide_context=True,
    python_callable=read_changes,
    dag=dag,
)


@retry(
    wait=tenacity.wait_exponential(),
    stop=tenacity.stop_after_attempt(10),
    retry=tenacity.retry_if_exception_type(requests.exceptions.ConnectionError),
)
def register_journals(ds, **kwargs):
    mongo_connect()
    tasks = kwargs["ti"].xcom_pull(key="tasks", task_ids="read_changes_task")

    journal_changes = filter_changes(tasks, "journals", "get")

    api_hook = HttpHook(http_conn_id="kernel_conn", method="GET")

    # Dictionary with id of journal and list of issues, something like: j_issues[journal_id] = [issue_id, issue_id, ....]
    j_issues = {}

    for journal in journal_changes:
        resp_json = api_hook.run(endpoint=journal.get("id")).json()

        t_journal = transform_journal(resp_json)
        t_journal.save()

        j_issues[get_id(journal.get("id"))] = resp_json.get("items", [])

    kwargs["ti"].xcom_push(key="j_issues", value=j_issues)

    return tasks


register_journals_task = PythonOperator(
    task_id="register_journals_task",
    provide_context=True,
    python_callable=register_journals,
    dag=dag,
)


def transform_issue(data):
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

    return issue


@retry(
    wait=tenacity.wait_exponential(),
    stop=tenacity.stop_after_attempt(10),
    retry=tenacity.retry_if_exception_type(requests.exceptions.ConnectionError),
)
def register_issues(ds, **kwargs):
    mongo_connect()

    def get_journal(j_issues, issue_id):
        """
        Return issue`s journal
        """
        for j, i in j_issues.items():
            if issue_id in i:
                return models.Journal.objects.get(_id=j)

    tasks = kwargs["ti"].xcom_pull(key="tasks", task_ids="read_changes_task")

    j_issues = kwargs["ti"].xcom_pull(key="j_issues", task_ids="register_journals_task")

    # Dictionary with id of issue and a list of documents, something like: i_documents[issue_id] = [document_id, document_id, ....]
    i_documents = {}

    issue_changes = filter_changes(tasks, "bundles", "get")

    api_hook = HttpHook(http_conn_id="kernel_conn", method="GET")

    for issue in issue_changes:
        resp_json = api_hook.run(endpoint=issue.get("id")).json()

        issue_id = get_id(issue.get("id"))
        journal = get_journal(j_issues, issue_id)

        t_issue = transform_issue(resp_json)
        t_issue.journal = journal
        t_issue.order = j_issues.get(journal.id).index(issue_id)
        t_issue.save()

        i_documents[get_id(issue.get("id"))] = resp_json.get("items", [])

    kwargs["ti"].xcom_push(key="i_documents", value=i_documents)

    return tasks


register_issues_task = PythonOperator(
    task_id="register_issues_task",
    provide_context=True,
    python_callable=register_issues,
    dag=dag,
)


def transform_document(data):

    def nestget(data, *path, default=''):
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
                    **{"text": nestget(trans_abs, "text", 0), "language": nestget(trans_abs, "lang", 0)}
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
    document.abstract_languages = [trans_abs["language"] for trans_abs in trans_abstracts]

    document.original_language = original_lang

    document.publication_date = nestget(pub_date, "text", 0)

    document.type = nestget(article, "type", 0)
    document.elocation = nestget(article_meta, "pub_elocation", 0)
    document.fpage = nestget(article_meta, "pub_fpage", 0)
    document.fpage_sequence = nestget(article_meta, "pub_fpage_seq", 0)
    document.lpage = nestget(article_meta, "pub_lpage", 0)

    return document


@retry(
    wait=tenacity.wait_exponential(),
    stop=tenacity.stop_after_attempt(10),
    retry=tenacity.retry_if_exception_type(requests.exceptions.ConnectionError),
)
def register_documents(ds, **kwargs):
    mongo_connect()

    def get_issue(i_documents, document_id):
        """
        Return document`s issue
        """
        for i, d in i_documents.items():
            if document_id in d:
                return models.Issue.objects.get(_id=i)

    tasks = kwargs["ti"].xcom_pull(key="tasks", task_ids="read_changes_task")

    i_documents = kwargs["ti"].xcom_pull(
        key="i_documents", task_ids="register_issues_task"
    )

    document_changes = filter_changes(tasks, "documents", "get")

    api_hook = HttpHook(http_conn_id="kernel_conn", method="GET")

    for document in document_changes:
        resp_json = api_hook.run("%s/front" % document.get("id")).json()

        t_document = transform_document(resp_json)

        document_id = get_id(document.get("id"))
        issue = get_issue(i_documents, document_id)

        t_document.issue = issue
        t_document.journal = issue.journal

        t_document.order = i_documents.get(issue.id).index(document_id)
        t_document.xml = "%s%s" % (api_hook.base_url, document.get("id"))

        t_document.save()

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


read_changes_task >> register_journals_task
register_issues_task << register_journals_task
register_last_issues_task << register_issues_task
register_documents_task << register_last_issues_task
delete_journals_task << register_documents_task
delete_issues_task << delete_journals_task
delete_documents_task << delete_issues_task
