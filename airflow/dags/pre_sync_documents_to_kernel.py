# conding: utf-8
"""
    DAG responsável por preparar os pacotes SPS vindos do fluxo de ingestão para atualização do Kernel.

    Passos:
        a. Ler Scilista e determinar ação de cada fascículo
        b. Se for comando para deleção de fascículo, ignora comando para fascículo
        c. Senão
            1. Move pacotes SPS referentes ao fascículo da Scilista, ordenados pelo nome
               com data e hora
            2. Dispara execução de DAG de sincronização para cada pacote
"""
import os
import logging
import shutil
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.utils import timezone
from airflow.api.common.experimental.trigger_dag import trigger_dag

from operations import pre_sync_documents_to_kernel_operations


Logger = logging.getLogger(__name__)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 7, 22),
}

dag = DAG(
    dag_id="pre_sync_documents_to_kernel", default_args=default_args, schedule_interval=None
)


def get_scilista_file_path(
    xc_sps_packages_dir: Path, proc_sps_packages_dir: Path, gerapadrao_id: str
) -> str:
    """Garante que a scilista usada será a do diretório de PROC. Quando for a primeira
    execução da DAG, a lista será copiada para o diretório de PROC. Caso contrário, a 
    mesma será mantida.
    """
    proc_dir_scilista_list = list(proc_sps_packages_dir.glob(f"scilista-*.lst"))
    if proc_dir_scilista_list:
        _proc_scilista_file_path = proc_dir_scilista_list[0]
        Logger.info('Proc scilista "%s" already exists', _proc_scilista_file_path)
    else:
        _scilista_filename = f"scilista-{gerapadrao_id}.lst"
        _origin_scilista_file_path = xc_sps_packages_dir / _scilista_filename
        if not _origin_scilista_file_path.is_file():
            raise FileNotFoundError(_origin_scilista_file_path)

        _proc_scilista_file_path = proc_sps_packages_dir / _scilista_filename
        Logger.info(
            'Copying original scilista "%s" to proc "%s"',
            _origin_scilista_file_path,
            _proc_scilista_file_path,
        )
        shutil.copy(_origin_scilista_file_path, _proc_scilista_file_path)
    return str(_proc_scilista_file_path)


def get_sps_packages(conf, **kwargs):
    """Executa ``pre_sync_documents_to_kernel_operations.get_sps_packages`` com a 
    scilista referente à DagRun. Todos os pacotes obtidos serão armazenados em 
    diretório identificado pelo DAG_RUN_ID junto com a scilista. Caso esta DAG seja 
    reexecutada, os mesmos pacotes serão sincronizados anteriormente serão novamente 
    sincronizados.
    Armazena os pacotes em XCom para a próxima tarefa.
    """
    _xc_sps_packages_dir = Path(Variable.get("XC_SPS_PACKAGES_DIR"))
    _proc_sps_packages_dir = Path(Variable.get("PROC_SPS_PACKAGES_DIR")) / kwargs["run_id"]
    if not _proc_sps_packages_dir.is_dir():
        _proc_sps_packages_dir.mkdir()

    _scilista_file_path = get_scilista_file_path(
        _xc_sps_packages_dir,
        _proc_sps_packages_dir,
        Variable.get("GERAPADRAO_ID_FOR_SCILISTA"),
    )
    _sps_packages = pre_sync_documents_to_kernel_operations.get_sps_packages(
        _scilista_file_path,
        _xc_sps_packages_dir,
        _proc_sps_packages_dir,
    )

    if _sps_packages:
        kwargs["ti"].xcom_push(key="sps_packages", value=_sps_packages)
        return True
    else:
        return False


def start_sync_packages(conf, **kwargs):
    """Executa trigger de dags de sincronização de documentos com o Kernel, cada uma com
    um pacote SPS da lista armazenada pela tarefa anterior.
    """
    _sps_packages = kwargs["ti"].xcom_pull(key="sps_packages") or []
    for bundle_label, bundle_packages in _sps_packages.items():
        Logger.info("Triggering an external dag with package %s" % bundle_label)
        now = timezone.utcnow()
        trigger_dag(
            dag_id="sync_documents_to_kernel",
            run_id="manual__%s_%s" % (os.path.basename(bundle_label), now.isoformat()),
            execution_date=now,
            replace_microseconds=False,
            conf={
                "bundle_label": bundle_label,
                "sps_package": bundle_packages,
                "pre_syn_dag_run_id": kwargs.get("run_id"),
            },
        )
    Logger.debug("create_all_subdags OUT")


get_sps_packages_task = ShortCircuitOperator(
    task_id="get_sps_packages_task_id",
    provide_context=True,
    python_callable=get_sps_packages,
    dag=dag,
)

start_sync_packages_task = PythonOperator(
    task_id="start_sync_packages_task_id",
    provide_context=True,
    python_callable=start_sync_packages,
    dag=dag,
)

get_sps_packages_task >> start_sync_packages_task
