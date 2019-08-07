# conding: utf-8
"""
    DAG responsável por preparar os pacotes SPS vindos do fluxo de ingestão para
    atualização do Kernel.

    Passos:
        a. Obtém Pacotes SPS através da Scilista
        b. Ler Pacotes SPS de acordo com a Scilista
            - Diretório configurável, alimentado pelo XC
        c. Não conseguiu ler pacotes
            1. Envio de Email sobre pacote inválido
            2. Pensar em outras forma de verificar
        d. Deleta fascículos de acordo com a Scilista
            1. Deletar o bundle no Kernel
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
    "start_date": datetime.utcnow(),
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
            conf={"sps_package": sps_package},
        )
    Logger.debug("create_all_subdags OUT")


get_sps_packages_task = PythonOperator(
    task_id="get_sps_packages_task_id",
    provide_context=True,
    python_callable=get_sps_packages,
    dag=dag,
)

get_sps_packages_task
