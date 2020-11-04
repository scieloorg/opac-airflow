"""DAG responsável por extrair dados dos pacote SPS para publicação."""

import logging
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.utils import timezone
from airflow.api.common.experimental.trigger_dag import trigger_dag

from operations import sync_documents_to_kernel_operations
from common.hooks import save_document_in_database, clear_existing_dag_run


Logger = logging.getLogger(__name__)


def optimize_packages(dag_run, **kwargs):
    """
    Otimiza os pacotes SPS do bundle para WEB. Registra a lista de pacotes 
    `sps_packages` atualizada no XCom, com os caminhos de pacotes que foram otimizados.

    :param dag_run: Dados do DAG Run. Serão usados bundle_label, sps_packages e 
        gerapadrao_id
    """
    bundle_label = dag_run.conf.get("bundle_label")
    sps_packages = dag_run.conf.get("sps_packages") or []
    gerapadrao_id = dag_run.conf.get("gerapadrao_id")
    new_sps_pkg_zip_dir = Path(Variable.get("PROC_SPS_PACKAGES_DIR")) / gerapadrao_id / "optimized"
    if not new_sps_pkg_zip_dir.is_dir():
        new_sps_pkg_zip_dir.mkdir()

    packages_after_optimization = []
    for sps_package in sps_packages:
        new_sps_pkg_zip_file = sync_documents_to_kernel_operations.optimize_sps_pkg_zip_file(
            sps_package, new_sps_pkg_zip_dir
        )
        if new_sps_pkg_zip_file:
            Logger.info(
                "[%s] Package %s optimized in %s",
                bundle_label,
                sps_package,
                new_sps_pkg_zip_file,
            )
            packages_after_optimization.append(new_sps_pkg_zip_file)
        else:
            Logger.warning("[%s] Package %s not optimized", bundle_label, sps_package)
            packages_after_optimization.append(sps_package)

    if packages_after_optimization:
        kwargs["ti"].xcom_push(key="optimized_packages", value=packages_after_optimization)
        return True
    else:
        return False


def extract_packages_data(dag_run, run_id, execution_date, **kwargs):
    """
    Executa a extração de metadados dos documentos XMLs contidos nos pacotes SPS.
    Os dados serão usados para a syncronização com o Kernel e o Object Store.

    :param run_id: Run ID da DAG Run atual
    :param dag_run: Dados do DAG Run. Serão usados bundle_label e pre_syn_dag_run_id
    """
    sps_packages = kwargs["ti"].xcom_pull(
        key="optimized_packages", task_ids="optimize_packages_task"
    )
    data_to_complete = {
        "bundle_label": dag_run.conf.get("bundle_label"),
        "gerapadrao_id": dag_run.conf.get("gerapadrao_id"),
        "pre_sync_dag_run": run_id,
        "execution_date": execution_date,
    }

    scielo_ids = []
    for sps_package in sps_packages:
        # Extrai os dados de cada pacote
        documents_info, fields_filter = \
            sync_documents_to_kernel_operations.extract_package_data(sps_package)

        for xml_filename, document_record in documents_info.items():
            Logger.info(
                'Saving "%s" extracted document info from "%s" package',
                xml_filename, sps_package
            )

            # Completa os dados de cada documento com dados da Dag Run
            filter = {
                field: value
                for field, value in document_record.items()
                if field in fields_filter
            }
            filter.update(data_to_complete)
            document_record.update(data_to_complete)
            save_document_in_database("e_documents", filter, document_record)

            # Adiciona o pid na lista para carga de documentos
            if document_record.get("pid"):
                scielo_ids.append(document_record["pid"])

    if scielo_ids:
        kwargs["ti"].xcom_push(key="scielo_ids", value=set(scielo_ids))
        return True
    else:
        return False


def start_sync_documents(dag_run, run_id, **kwargs):
    """Executa trigger de dags de carregamento dos dados por documento."""

    bundle_label = dag_run.conf.get("bundle_label")
    scielo_ids = kwargs["ti"].xcom_pull(
        key="scielo_ids", task_ids="extract_packages_data_task"
    )
    for i, scielo_id in enumerate(scielo_ids, 1):
        Logger.info(
            "[%s] Triggering an external dag for XML file [%d/%d]: %s",
            bundle_label, i, len(scielo_ids), scielo_id,
        )

        dag_id = "sync_docs_to_kernel"
        trigger_run_id = f'trig__{dag_run.conf.get("gerapadrao_id")}_{scielo_id}'
        if not clear_existing_dag_run(dag_id, trigger_run_id):
            now = timezone.utcnow()
            trigger_dag(
                dag_id=dag_id,
                run_id=trigger_run_id,
                execution_date=now,
                replace_microseconds=False,
                conf={
                    "scielo_id": scielo_id,
                    "pre_syn_dag_run_id": run_id,
                },
            )


DAG_NAME = "extract_packages_info"
args = {
    'owner': 'airflow',
    "depends_on_past": False,
    "start_date": datetime(2019, 7, 21),
}


with DAG(
        dag_id=DAG_NAME, default_args=args, schedule_interval=None
    ) as dag:

    optimize_packages_task = ShortCircuitOperator(
        task_id="optimize_packages_task",
        provide_context=True,
        python_callable=optimize_packages,
        dag=dag,
    )

    extract_packages_data_task = ShortCircuitOperator(
        task_id="extract_packages_data_task",
        provide_context=True,
        python_callable=extract_packages_data,
        dag=dag,
    )

    start_sync_documents_task = ShortCircuitOperator(
        task_id="start_sync_documents_task",
        provide_context=True,
        python_callable=start_sync_documents,
        dag=dag,
    )

    optimize_packages_task >> extract_packages_data_task >> start_sync_documents_task