"""
    DAG responsável por registrar/atualizar pacote SPS no Kernel.

    Passos:
        a. Ler Pacote SPS
            - Arquivos de computador no formato .zip, cada um representando um número
            (fascículo), com todos os arquivos XML, e respectivos arquivos PDF e
            outros ativos digitais.
            Para cada um dos XMLs
                1. Obter SciELO ID no XML
                2. Verificar XMLs para deletar
                   (/article-meta/article-id/@specific-use="delete")
                   I. DELETE documentos no Kernel
                3. Verificar XMLs para preservar
                    I. PUT pacotes SPS no Minio
                    II. PUT/PATCH XML no Kernel
                    III. PUT PDF no Kernel
        c. Não conseguiu ler pacotes
            1. Envio de Email sobre pacote inválido
            2. Pensar em outras forma de verificar
"""
import os
import logging
from datetime import datetime, timedelta
from tempfile import mkdtemp

from airflow import DAG
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.models import Variable

from operations import sync_documents_to_kernel_operations
from common.hooks import add_execution_in_database


Logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 7, 21),
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(dag_id="sync_documents_to_kernel", default_args=default_args, schedule_interval=None)


def list_documents(dag_run, **kwargs):
    _sps_package = dag_run.conf.get("sps_package")
    _xmls_filenames = sync_documents_to_kernel_operations.list_documents(_sps_package)
    if _xmls_filenames:
        kwargs["ti"].xcom_push(key="xmls_filenames", value=_xmls_filenames)
        return True
    else:
        return False


def delete_documents(dag_run, **kwargs):
    _sps_package = dag_run.conf.get("sps_package")
    _xmls_filenames = kwargs["ti"].xcom_pull(
        key="xmls_filenames", task_ids="list_docs_task_id"
    )
    if not _xmls_filenames:
        return False

    _xmls_to_preserve, executions = sync_documents_to_kernel_operations.delete_documents(
        _sps_package, _xmls_filenames
    )

    for execution in executions:
        execution["dag_run"] = kwargs.get("run_id")
        execution["pre_sync_dag_run"] = dag_run.conf.get("pre_syn_dag_run_id")
        execution["package_name"] = os.path.basename(_sps_package)
        add_execution_in_database(table="xml_documents", data=execution)

    if _xmls_to_preserve:
        kwargs["ti"].xcom_push(key="xmls_to_preserve", value=_xmls_to_preserve)
        return True
    else:
        return False


def optimize_package(dag_run, **kwargs):
    _sps_package = dag_run.conf.get("sps_package")
    _xmls_to_preserve = kwargs["ti"].xcom_pull(
        key="xmls_to_preserve", task_ids="delete_docs_task_id"
    )
    if not _xmls_to_preserve:
        return False

    new_sps_zip_dir = Variable.get("NEW_SPS_ZIP_DIR", mkdtemp())
    _optimized_package = sync_documents_to_kernel_operations.optimize_sps_pkg_zip_file(
        _sps_package, new_sps_zip_dir
    )
    if _optimized_package:
        kwargs["ti"].xcom_push(
            key="optimized_package", value=_optimized_package)
    return True


def register_update_documents(dag_run, **kwargs):

    _xmls_to_preserve = kwargs["ti"].xcom_pull(
        key="xmls_to_preserve", task_ids="delete_docs_task_id"
    )
    if not _xmls_to_preserve:
        return False

    _optimized_package = kwargs["ti"].xcom_pull(
        key="optimized_package", task_ids="optimize_package_task_id"
    )

    package = _optimized_package or dag_run.conf.get("sps_package")
    _documents, executions = sync_documents_to_kernel_operations.register_update_documents(
        package, _xmls_to_preserve
    )

    for execution in executions:
        execution["dag_run"] = kwargs.get("run_id")
        execution["pre_sync_dag_run"] = dag_run.conf.get("pre_syn_dag_run_id")
        execution["package_name"] = os.path.basename(_optimized_package)
        add_execution_in_database(table="xml_documents", data=execution)

    if _documents:
        kwargs["ti"].xcom_push(key="documents", value=_documents)
        return True
    else:
        return False


def link_documents_to_documentsbundle(dag_run, **kwargs):
    _sps_package = dag_run.conf.get("sps_package")
    documents = kwargs["ti"].xcom_pull(key="documents", task_ids="register_update_docs_id")
    issn_index_json_path = kwargs["ti"].xcom_pull(
        task_ids="process_journals_task",
        dag_id="sync_isis_to_kernel",
        key="issn_index_json_path",
        include_prior_dates=True
    )

    if not documents:
        return False

    linked_bundle, link_executions = sync_documents_to_kernel_operations.link_documents_to_documentsbundle(
        _sps_package, documents, issn_index_json_path
    )

    for execution in link_executions:
        execution["dag_run"] = kwargs.get("run_id")
        execution["pre_sync_dag_run"] = dag_run.conf.get("pre_syn_dag_run_id")
        execution["package_name"] = os.path.basename(_sps_package)
        add_execution_in_database(table="xml_documentsbundle", data=execution)

    if linked_bundle:
        kwargs["ti"].xcom_push(key="linked_bundle", value=linked_bundle)
        return True
    else:
        return False


list_documents_task = ShortCircuitOperator(
    task_id="list_docs_task_id",
    provide_context=True,
    python_callable=list_documents,
    dag=dag,
)

delete_documents_task = ShortCircuitOperator(
    task_id="delete_docs_task_id",
    provide_context=True,
    python_callable=delete_documents,
    dag=dag,
)

optimize_package_task = ShortCircuitOperator(
    task_id="optimize_package_task_id",
    provide_context=True,
    python_callable=optimize_package,
    dag=dag,
)

register_update_documents_task = ShortCircuitOperator(
    task_id="register_update_docs_id",
    provide_context=True,
    python_callable=register_update_documents,
    dag=dag,
)

link_documents_task = ShortCircuitOperator(
    task_id="link_documents_task_id",
    provide_context=True,
    python_callable=link_documents_to_documentsbundle,
    dag=dag,
)

trigger_sync_kernel_to_website_dag_task = TriggerDagRunOperator(
    task_id="trigger_sync_kernel_to_website_dag_task",
    trigger_dag_id="sync_kernel_to_website",
    dag=dag,
)

list_documents_task >> delete_documents_task
delete_documents_task >> optimize_package_task
optimize_package_task >> register_update_documents_task
register_update_documents_task >> link_documents_task
link_documents_task >> trigger_sync_kernel_to_website_dag_task
