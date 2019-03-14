from datetime import timedelta

import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
import requests


default_args = {
    "owner": "airflow",
    "start_date": airflow.utils.dates.days_ago(2),
    "provide_context": True,
}

dag = DAG(
    "documentstore_changes",
    default_args=default_args,
    schedule_interval=timedelta(minutes=1),
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


CHANGES_URL = "http://0.0.0.0:6543/changes?since=%s"


def changes(since=""):
    last_yielded = None
    while True:
        with requests.get(CHANGES_URL % since) as f:
            response = f.json()
            for result in response["results"]:
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


read_changes_task = ShortCircuitOperator(
    task_id="read_changes_task",
    provide_context=True,
    python_callable=read_changes,
    dag=dag,
)


def register_journals(ds, **kwargs):
    tasks = kwargs["ti"].xcom_pull(key="tasks", task_ids="read_changes_task")
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

register_journals_task << read_changes_task
register_issues_task << register_journals_task
register_documents_task << register_issues_task
delete_documents_task << read_changes_task
delete_issues_task << delete_documents_task
delete_journals_task << delete_issues_task
