# conding: utf-8
"""
    DAG responsável por preparar os pacotes SPS vindos do fluxo de ingestão para atualização do Kernel.

    Passos:
        a. Ler Scilista e determinar ação de cada fascículo
        b. Se for comando para deleção de fascículo, ignora comando para fascículo
        c. Senão
            1. Move pacotes SPS referentes ao fascículo da Scilista, ordenados pelo nome
               com data e hora
            2. Dispara execução de DAG de sincronização para cada bundle
"""
import logging
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.utils import timezone
from airflow.api.common.experimental.trigger_dag import trigger_dag

from operations import pre_sync_documents_to_kernel_operations
from common.hooks import clear_existing_dag_run


Logger = logging.getLogger(__name__)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 7, 22),
}


def get_sps_packages(dag_run, **kwargs):
    """Obtém os pacotes SPS para o diretório de trabalho com base na scilista."""

    _gerapadrao_id = dag_run.conf.get("GERAPADRAO_ID")
    if not _gerapadrao_id:
        Logger.error("GERAPADRAO_ID is None and must be informed.")
        return False

    _xc_sps_packages_dir = Path(Variable.get("XC_SPS_PACKAGES_DIR"))
    _proc_sps_packages_dir = Path(Variable.get("PROC_SPS_PACKAGES_DIR")) / _gerapadrao_id
    if not _proc_sps_packages_dir.is_dir():
        _proc_sps_packages_dir.mkdir()

    _scilista_file_path = pre_sync_documents_to_kernel_operations.get_scilista_file_path(
        _xc_sps_packages_dir,
        _proc_sps_packages_dir,
        _gerapadrao_id,
    )
    _sps_packages = pre_sync_documents_to_kernel_operations.get_sps_packages_on_scilista(
        _scilista_file_path,
        _xc_sps_packages_dir,
        _proc_sps_packages_dir,
    )

    if _sps_packages:
        kwargs["ti"].xcom_push(key="sps_packages", value=_sps_packages)
        return True
    else:
        return False


def start_sync_packages(dag_run, **kwargs):
    """Executa trigger de dags de extração de informações dos pacotes SPS."""

    _sps_packages = kwargs["ti"].xcom_pull(
        key="sps_packages", task_ids="get_sps_packages_task"
    ) or {}
    for bundle_label, bundle_packages in _sps_packages.items():
        Logger.info(
            'Triggering an external dag from "%s", gerapadrao_id "%s" with "%s" packages'
            ': %s', kwargs.get("run_id"), dag_run.conf.get("GERAPADRAO_ID"), bundle_label, bundle_packages
        )
        dag_id = "extract_packages_info"
        run_id = f'trig__{dag_run.conf.get("GERAPADRAO_ID")}_{bundle_label}'
        if not clear_existing_dag_run(dag_id, run_id):
            now = timezone.utcnow()
            trigger_dag(
                dag_id=dag_id,
                run_id=run_id,
                execution_date=now,
                replace_microseconds=False,
                conf={
                    "bundle_label": bundle_label,
                    "sps_packages": bundle_packages,
                    "pre_syn_dag_run_id": kwargs.get("run_id"),
                    "gerapadrao_id": dag_run.conf.get("GERAPADRAO_ID")
                },
            )


with DAG(
    dag_id="collect_scilista_packages", default_args=default_args, schedule_interval=None
) as dag:

    get_sps_packages_task = ShortCircuitOperator(
        task_id="get_sps_packages_task",
        provide_context=True,
        python_callable=get_sps_packages,
        dag=dag,
    )

    start_sync_packages_task = PythonOperator(
        task_id="start_sync_packages_task",
        provide_context=True,
        python_callable=start_sync_packages,
        dag=dag,
    )
    
    get_sps_packages_task >> start_sync_packages_task