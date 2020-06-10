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
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
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


def get_sps_packages(conf, **kwargs):
    Logger.debug("create_all_subdags IN")
    sps_packages = pre_sync_documents_to_kernel_operations.get_sps_packages(
        Variable.get("SCILISTA_FILE_PATH"),
        Variable.get("XC_SPS_PACKAGES_DIR"),
        Variable.get("PROC_SPS_PACKAGES_DIR"),
    )
    for sps_package in sps_packages:
        Logger.info("Triggering an external dag with package %s" % sps_package)
        now = timezone.utcnow()
        trigger_dag(
            dag_id="sync_documents_to_kernel",
            run_id="manual__%s_%s" % (os.path.basename(sps_package), now.isoformat()),
            execution_date=now,
            replace_microseconds=False,
            conf={
                "sps_package": sps_package,
                "pre_syn_dag_run_id": kwargs.get("run_id"),
            },
        )
    Logger.debug("create_all_subdags OUT")


get_sps_packages_task = PythonOperator(
    task_id="get_sps_packages_task_id",
    provide_context=True,
    python_callable=get_sps_packages,
    dag=dag,
)

get_sps_packages_task
