import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


Logger = logging.getLogger(__name__)
PARENT_DAG_NAME = 'sync_kernel_to_website'


def _group_documents_by_bundle(document_ids, _get_relation_data):
    """Agrupa `document_ids` em grupos
    """
    groups = {}
    for doc_id in document_ids:
        bundle_id, doc = _get_relation_data(doc_id)
        if bundle_id:
            groups[bundle_id] = groups.get(bundle_id) or []
            groups[bundle_id].append(doc_id)
    return groups


def create_subdag_to_register_documents_grouped_by_bundle(
        dag, register_docs_callable,
        document_ids, _get_relation_data,
        register_renditions_callable, renditions_documents_id,
        args,
        ):
    """
    Cria uma subdag para executar register_documents_grouped_by_bundle
    para que se houver falha, será necessário reexecutar apenas os grupos
    que falharam
    """
    CHILD_DAG_NAME = 'register_documents_groups_id'

    Logger.info("Create register_documents_grouped_by_bundle subdag")

    groups = _group_documents_by_bundle(document_ids, _get_relation_data)
    Logger.info("Total groups: %i", len(groups))

    dag_subdag = DAG(
        dag_id='{}.{}'.format(PARENT_DAG_NAME, CHILD_DAG_NAME),
        default_args=args,
        schedule_interval=None,
    )
    with dag_subdag:
        if not groups:
            Logger.info("Do nothing")
            task_id = f'{CHILD_DAG_NAME}_do_nothing'
            PythonOperator(
                task_id=task_id,
                python_callable=do_nothing,
                dag=dag_subdag,
            )

    return dag_subdag


def do_nothing(**kwargs):
    return True
