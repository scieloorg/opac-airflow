import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


Logger = logging.getLogger(__name__)
PARENT_DAG_NAME = 'sync_kernel_to_website'


def _group_documents_by_bundle(document_ids, _get_relation_data):
    """Agrupa `document_ids` em grupos
    """
    Logger.info("_group_documents_by_bundle")
    groups = {}
    for doc_id in document_ids:
        bundle_id, doc = _get_relation_data(doc_id)
        if bundle_id:
            groups[bundle_id] = groups.get(bundle_id) or []
            groups[bundle_id].append(doc_id)
    Logger.info("_group_documents_by_bundle: %i", len(groups))
    return groups


def create_subdag_to_register_documents_grouped_by_bundle(
        dag, register_docs_callable, register_renditions_callable,
        register_documents_subdag_params,
        args,
        ):
    """
    Cria uma subdag para executar register_documents_grouped_by_bundle
    para que se houver falha, será necessário reexecutar apenas os grupos
    que falharam
    """
    Logger.info("Create register_documents_grouped_by_bundle subdag")

    CHILD_DAG_NAME = 'register_documents_groups_id'

    Logger.info("Call register_documents_subdag_params")
    data = register_documents_subdag_params(dag, args)
    document_ids, renditions_documents_id, _get_relation_data = data

    renditions_documents_id = set(renditions_documents_id)

    Logger.info("Call _group_documents_by_bundle")
    groups = _group_documents_by_bundle(document_ids, _get_relation_data)
    Logger.info("Total groups: %i", len(groups))

    dag_subdag = DAG(
        dag_id='{}.{}'.format(PARENT_DAG_NAME, CHILD_DAG_NAME),
        default_args=args,
        schedule_interval=None,
    )
    with dag_subdag:
        for bundle_id, doc_ids in groups.items():
            task_id = f'{CHILD_DAG_NAME}_{bundle_id}_docs'

            Logger.info("%s", bundle_id)
            Logger.info("Total: %i", len(doc_ids))

            t1 = PythonOperator(
                task_id=task_id,
                python_callable=register_docs_callable,
                op_args=(doc_ids, _get_relation_data),
                dag=dag_subdag,
            )

            # register documents renditions
            _renditions_documents_id = set(doc_ids) & renditions_documents_id
            Logger.info(
                "Total renditions documents (%s): %i",
                bundle_id, len(_renditions_documents_id))
            if _renditions_documents_id:
                task_id = f'{CHILD_DAG_NAME}_{bundle_id}_renditions'

                t2 = PythonOperator(
                    task_id=task_id,
                    python_callable=register_renditions_callable,
                    op_kwargs={'renditions_to_get': _renditions_documents_id},
                    dag=dag_subdag,
                )
                t1 >> t2

        _renditions_documents_id = renditions_documents_id - set(document_ids)
        Logger.info(
                "Total renditions documents: %i",
                len(_renditions_documents_id))
        if _renditions_documents_id:
            # registra `renditions` de ID de documentos que não estão em
            # `documents_to_get`
            task_id = f'{CHILD_DAG_NAME}_renditions'

            t3 = PythonOperator(
                task_id=task_id,
                python_callable=register_renditions_callable,
                op_kwargs={'renditions_to_get': _renditions_documents_id},
                dag=dag_subdag,
            )

        elif not groups:
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
