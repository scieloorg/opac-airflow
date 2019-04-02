import os
from datetime import timedelta
from urllib.parse import urljoin

import airflow
import tenacity
from tenacity import retry
from airflow import DAG
from airflow.hooks.http_hook import HttpHook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator

import requests

from mongoengine import connect

from opac_schema.v1.models import Journal, JounalMetrics


failure_recipients = os.environ.get('EMIAL_ON_FAILURE_RECIPIENTS', None)
EMIAL_ON_FAILURE_RECIPIENTS = failure_recipients.split(',') if failure_recipients else []

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
    dag_id="documentstore_changes",
    default_args=default_args,
    schedule_interval=timedelta(minutes=1),
)

KERNEL_URL = os.environ.get("KERNEL_URL", "http://0.0.0.0:6543")

OPAC_MONGODB_NAME = os.environ.get("OPAC_MONGODB_NAME", "opac")

OPAC_MONGODB_HOSTNAME = os.environ.get("OPAC_MONGODB_HOSTNAME", "localhost")

OPAC_MONGODB_USERNAME = os.environ.get("OPAC_MONGODB_USERNAME", "")

OPAC_MONGODB_PASS = os.environ.get("OPAC_MONGODB_PASS", "")

connect(
    db=OPAC_MONGODB_NAME,
    host=OPAC_MONGODB_HOSTNAME,
    username=OPAC_MONGODB_USERNAME,
    password=OPAC_MONGODB_PASS,
    authentication_source="admin",
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


@retry(wait=tenacity.wait_exponential(),
       stop=tenacity.stop_after_attempt(10),
       retry=tenacity.retry_if_exception_type(requests.exceptions.ConnectionError))
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


def read_changes(ds, **kwargs):
    reader = Reader()
    variable_timestamp = Variable.get("documentstore_timestamp", "")
    tasks, timestamp = reader.read(changes(since=variable_timestamp))
    if timestamp == variable_timestamp:
        return False
    kwargs["ti"].xcom_push(key="tasks", value=tasks)
    Variable.set("documentstore_timestamp", timestamp)
    return timestamp


def filter_changes(tasks, entity, action):
    """
    Filter changes

    Return a list of items that matched by criteria ``entity`` and ``action``
    """

    for task in tasks:
        _, _entity, __ = task["id"].split("/")
        if _entity == entity and task.get("task") == action:
            yield task


def transform_journal(data):
    metadata = data["metadata"]

    journal = Journal()
    journal._id = journal.jid = data.get("id")
    journal.title = metadata.get("title", "")
    journal.title_iso = metadata.get("title_iso", "")
    journal.short_title = metadata.get("short_title", "")
    journal.acronym = metadata.get("acronym", "")
    journal.scielo_issn = metadata.get("scielo_issn", "")
    journal.print_issn = metadata.get("print_issn", "")
    journal.eletronic_issn = metadata.get("electronic_issn", "")

    # Subject_categories
    journal.subject_categories = metadata.get("subject_categories", [])

    # Métricas
    journal.metrics = JounalMetrics(**metadata.get("metrics", {}))

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


@retry(wait=tenacity.wait_exponential(),
       stop=tenacity.stop_after_attempt(10),
       retry=tenacity.retry_if_exception_type(requests.exceptions.ConnectionError))
def register_journals(ds, **kwargs):
    tasks = kwargs["ti"].xcom_pull(key="tasks", task_ids="read_changes_task")

    journal_changes = filter_changes(tasks, "journals", "get")

    api_hook = HttpHook(http_conn_id="kernel_conn", method="GET")

    for journal in journal_changes:
        resp_json = api_hook.run(endpoint=journal.get("id")).json()

        response = resp_json
        journal = transform_journal(response)
        journal.save()

    return tasks


register_journals_task = PythonOperator(
    task_id="register_journals_task",
    provide_context=True,
    python_callable=register_journals,
    dag=dag,
)


def register_issues(ds, **kwargs):
    tasks = kwargs["ti"].xcom_pull(key="tasks", task_ids="read_changes_task")
    return tasks


register_issues_task = PythonOperator(
    task_id="register_issues_task",
    provide_context=True,
    python_callable=register_issues,
    dag=dag,
)


def register_documents(ds, **kwargs):
    tasks = kwargs["ti"].xcom_pull(key="tasks", task_ids="read_changes_task")
    return tasks


register_documents_task = PythonOperator(
    task_id="register_documents_task",
    provide_context=True,
    python_callable=register_documents,
    dag=dag,
)


def delete_documents(ds, **kwargs):
    tasks = kwargs["ti"].xcom_pull(key="tasks", task_ids="read_changes_task")
    return tasks


delete_documents_task = PythonOperator(
    task_id="delete_documents_task",
    provide_context=True,
    python_callable=delete_documents,
    dag=dag,
)


def delete_issues(ds, **kwargs):
    tasks = kwargs["ti"].xcom_pull(key="tasks", task_ids="read_changes_task")
    return tasks


delete_issues_task = PythonOperator(
    task_id="delete_issues_task",
    provide_context=True,
    python_callable=delete_issues,
    dag=dag,
)


def delete_journals(ds, **kwargs):
    tasks = kwargs["ti"].xcom_pull(key="tasks", task_ids="read_changes_task")
    return tasks


delete_journals_task = PythonOperator(
    task_id="delete_journals_task",
    provide_context=True,
    python_callable=delete_journals,
    dag=dag,
)

read_changes_task >> [register_journals_task, delete_documents_task]
register_issues_task << register_journals_task
register_documents_task << register_issues_task
delete_issues_task << delete_documents_task
delete_journals_task << delete_issues_task
