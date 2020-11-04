"""
DAG responsável por sincronizar informações dos documentos com o Kernel e Object Store
"""
import os
import logging
from datetime import datetime
from tempfile import mkdtemp

from airflow import DAG
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.models import Variable

from operations import sync_documents_to_kernel_operations
from common.hooks import get_documents_in_database, add_execution_in_database


Logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 7, 21),
}


def apply_document_change(dag_run, run_id, **kwargs):
    scielo_id = dag_run.conf.get("scielo_id")
    pre_syn_dag_run_id = dag_run.conf.get("pre_syn_dag_run_id")

    document_records = get_documents_in_database(
        "e_documents",
        {"pid": scielo_id, "pre_sync_dag_run": pre_syn_dag_run_id},
        order_by=[("package_path", 1)],
    )

    document_data, executions = sync_documents_to_kernel_operations.sync_document(
        document_records
    )

    for execution in executions:
        execution["dag_run"] = run_id
        execution["pre_sync_dag_run"] = dag_run.conf.get("pre_syn_dag_run_id")
        add_execution_in_database(table="xml_documents", data=execution)

    if document_data:
        kwargs["ti"].xcom_push(key="document_data", value=document_data)
        return True
    else:
        return False


def update_document_in_bundle(dag_run, run_id, **kwargs):
    document_data = kwargs["ti"].xcom_pull(
        key="document_data", task_ids="apply_document_change_task"
    )
    issn_index_json_path = kwargs["ti"].xcom_pull(
        task_ids="process_journals_task",
        dag_id="sync_isis_to_kernel",
        key="issn_index_json_path",
        include_prior_dates=True
    )

    result, executions = sync_documents_to_kernel_operations.update_document_in_bundle(
        document_data, issn_index_json_path
    )

    for execution in executions:
        execution["dag_run"] = run_id
        execution["pre_sync_dag_run"] = dag_run.conf.get("pre_syn_dag_run_id")
        add_execution_in_database(table="xml_documentsbundle", data=execution)

    if result:
        kwargs["ti"].xcom_push(key="result", value=result)


with DAG(
    dag_id="sync_docs_to_kernel", default_args=default_args, schedule_interval=None
) as dag:
    apply_document_change_task = ShortCircuitOperator(
        task_id="apply_document_change_task",
        provide_context=True,
        python_callable=apply_document_change,
        dag=dag,
    )

    update_document_in_bundle_task = PythonOperator(
        task_id="update_document_in_bundle_task",
        provide_context=True,
        python_callable=update_document_in_bundle,
        dag=dag,
    )

    apply_document_change_task >> update_document_in_bundle_task