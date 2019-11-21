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
import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from operations import sync_documents_to_kernel_operations


Logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 7, 21),
}

dag = DAG(dag_id="sync_documents_to_kernel", default_args=default_args, schedule_interval=None)


def list_documents(dag_run, **kwargs):
    _sps_package = dag_run.conf.get("sps_package")
    _xmls_filenames = sync_documents_to_kernel_operations.list_documents(_sps_package)
    if _xmls_filenames:
        kwargs["ti"].xcom_push(key="xmls_filenames", value=_xmls_filenames)


def delete_documents(dag_run, **kwargs):
    _sps_package = dag_run.conf.get("sps_package")
    _xmls_filenames = kwargs["ti"].xcom_pull(
        key="xmls_filenames", task_ids="list_docs_task_id"
    )
    if _xmls_filenames:
        _xmls_to_preserve = sync_documents_to_kernel_operations.delete_documents(
            _sps_package, _xmls_filenames
        )
        if _xmls_to_preserve:
            kwargs["ti"].xcom_push(key="xmls_to_preserve", value=_xmls_to_preserve)


def register_update_documents(dag_run, **kwargs):
    _sps_package = dag_run.conf.get("sps_package")
    _xmls_to_preserve = kwargs["ti"].xcom_pull(
        key="xmls_to_preserve", task_ids="delete_docs_task_id"
    )
    if _xmls_to_preserve:
        _documents = sync_documents_to_kernel_operations.register_update_documents(
            _sps_package, _xmls_to_preserve
        )
        if _documents:
            kwargs["ti"].xcom_push(key="documents", value=_documents)


def link_documents_to_documentsbundle(dag_run, **kwargs):
    _sps_package = dag_run.conf.get("sps_package")
    documents = kwargs["ti"].xcom_pull(key="documents", task_ids="register_update_docs_id")
    issn_index_json_path = kwargs["ti"].xcom_pull(
        task_ids="process_journals_task",
        dag_id="sync_isis_to_kernel",
        key="issn_index_json_path",
        include_prior_dates=True
    )

    if documents:
        linked_bundle = sync_documents_to_kernel_operations.link_documents_to_documentsbundle(
            _sps_package, documents, issn_index_json_path
        )

        if linked_bundle:
            kwargs["ti"].xcom_push(key="linked_bundle", value=linked_bundle)


list_documents_task = PythonOperator(
    task_id="list_docs_task_id",
    provide_context=True,
    python_callable=list_documents,
    dag=dag,
)

delete_documents_task = PythonOperator(
    task_id="delete_docs_task_id",
    provide_context=True,
    python_callable=delete_documents,
    dag=dag,
)

register_update_documents_task = PythonOperator(
    task_id="register_update_docs_id",
    provide_context=True,
    python_callable=register_update_documents,
    dag=dag,
)

link_documents_task = PythonOperator(
    task_id="link_documents_task_id",
    provide_context=True,
    python_callable=link_documents_to_documentsbundle,
    dag=dag,
)

list_documents_task >> delete_documents_task >> register_update_documents_task >> link_documents_task
