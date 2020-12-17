import logging

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator


Logger = logging.getLogger(__name__)
PARENT_DAG_NAME = 'sync_kernel_to_website'


def _group_documents_by_bundle(document_ids, _get_relation_data):
    """Agrupa `document_ids` em grupos usando `bundle_id`
    Caso `bundle_id` is `None`, `doc_id` são órfãos
    """
    Logger.info("_group_documents_by_bundle")
    groups = {}
    for doc_id in document_ids:
        bundle_id, doc = _get_relation_data(doc_id)
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

    # obtém as listas de documents_id e renditions_id mais recentes
    # _get_relation_data é um callable para recuperar (issue_id, doc data)
    # dado um PID v3
    document_ids, renditions_documents_id, _get_relation_data = data

    # transforma a lista em um conjunto (set)
    renditions_documents_id = set(renditions_documents_id)

    Logger.info("Call _group_documents_by_bundle")
    groups = _group_documents_by_bundle(document_ids, _get_relation_data)
    Logger.info("Total groups: %i", len(groups))

    # `orphan_documents` são documentos registrados no Kernel, mas que ao tentar
    # ser registrado no website, levanta uma exceção, devido à ausência de
    # vínculo com seu respectivo `bundle`.
    # No entanto, agora como o registro de documentos no website é guiado
    # pelos `groups`, é sabido previamente quais são os documentos órfãos:
    # `groups[None]`
    try:
        orphan_documents = groups.pop(None)
    except KeyError:
        orphan_documents = []

    # `orphan_renditions` são as manifestação de documentos registradas
    # no Kernel, mas que ao tentar ser registrada no website,
    # levanta uma exceção, pois o documento a qual está vinculada
    # não está registrado no website.
    # A falha do registro pode ser minimizada previamente, excluindo
    # os valores já identificados como órfãos (`orphan_documents`).
    # Nota: Pode haver em `renditions_documents_id` valores que não sabemos
    # identificar previamente se são de documentos registrados ou não.

    # `orphan_renditions` é a interseção de `renditions_documents_id` com
    # `orphan_documents`
    orphan_renditions = renditions_documents_id & set(orphan_documents)

    # subtrai de `renditions_documents_id` os id das manifestações órfãs
    renditions_documents_id = renditions_documents_id - orphan_renditions

    dag_subdag = DAG(
        dag_id='{}.{}'.format(PARENT_DAG_NAME, CHILD_DAG_NAME),
        default_args=args,
        schedule_interval=None,
    )
    with dag_subdag:

        task_id = f'{CHILD_DAG_NAME}_finish'
        t_finish = PythonOperator(
            task_id=task_id,
            provide_context=True,
            python_callable=finish,
            op_args=(list(groups.keys()), orphan_documents, list(orphan_renditions)),
            dag=dag_subdag,
        )

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
                t1 >> t2 >> t_finish
            else:
                t1 >> t_finish

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
            t3 >> t_finish
    return dag_subdag


def finish(bundles, orphan_documents, orphan_renditions, **kwargs):
    Logger.info("Finish")

    t1_orphan_documents = orphan_documents or []
    for bundle_id in bundles:
        t1_orphan_documents += kwargs["ti"].xcom_pull(
            key="orphan_documents",
            task_ids=f'register_documents_groups_id_{bundle_id}_docs') or []

    t2_orphan_renditions = list(orphan_renditions) or []
    for bundle_id in bundles:
        t2_orphan_renditions += kwargs["ti"].xcom_pull(
            key="orphan_renditions",
            task_ids=f'register_documents_groups_id_{bundle_id}_renditions') or []

    t3_orphan_renditions = kwargs["ti"].xcom_pull(
        key="orphan_renditions",
        task_ids="register_documents_groups_id_renditions") or []

    if t1_orphan_documents:
        Variable.set(
            "orphan_documents", t1_orphan_documents, serialize_json=True)

    orphan_renditions = t2_orphan_renditions + t3_orphan_renditions
    if orphan_renditions:
        Variable.set(
            "orphan_renditions", orphan_renditions, serialize_json=True)

    Logger.info("Finish %i orphan_documents", len(t1_orphan_documents))
    Logger.info("Finish %i orphan_renditions", len(orphan_renditions))
    Logger.info("Finish - FIM")
    return True
