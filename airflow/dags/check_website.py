import os
import shutil
import re
import json
import logging
from datetime import timedelta
from pathlib import Path
import itertools
from typing import Dict, List, Tuple

import tenacity
from tenacity import retry

import airflow
from airflow import DAG
from airflow.sensors.http_sensor import HttpSensor
from airflow.hooks.http_hook import HttpHook
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator

import requests

from operations.docs_utils import group_pids
from operations import check_website_operations


Logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "start_date": airflow.utils.dates.days_ago(2),
    "provide_context": True,
    "depends_on_past": False,
}

dag = DAG(
    dag_id="check_website",
    default_args=default_args,
    schedule_interval=None,
)


def get_file_path_in_proc_dir(
    xc_sps_packages_dir: Path, proc_sps_packages_dir: Path, filename: str
) -> str:
    """Garante que arquivo estará no diretório de PROC.
    Quando for a primeira execução da DAG, o arquivo será copiado para
    o diretório de PROC.
    """
    file_path = proc_sps_packages_dir / filename
    if not file_path.is_file():
        _originfile_path = xc_sps_packages_dir / filename
        Logger.info(
            'Copying original "%s" to proc "%s"',
            _originfile_path,
            file_path,
        )
        shutil.copy(_originfile_path, file_path)
    if not file_path.is_file():
        raise FileNotFoundError(file_path)

    return str(file_path)


def check_website_uri_list(conf, **kwargs):
    """Executa ``check_website.check_website_uri_list`` com a 
    uri_list referente à DagRun. 
    """
    gerapadrao_id_items = Variable.get(
        "GERAPADRAO_ID_FOR_URI_LIST", default_var=[], deserialize_json=True)

    _website_url_list = Variable.get("WEBSITE_URL_LIST", default_var=[], deserialize_json=True)
    if not _website_url_list:
        raise ValueError(
            "Unable to check the Web site resources are available "
            "because no Website URL (`Variable[\"WEBSITE_URL_LIST\"]`) was informed")

    _xc_sps_packages_dir = Path(Variable.get("XC_SPS_PACKAGES_DIR"))
    _proc_sps_packages_dir = Path(Variable.get("PROC_SPS_PACKAGES_DIR")) / kwargs["run_id"]
    if not _proc_sps_packages_dir.is_dir():
        _proc_sps_packages_dir.mkdir()

    for gerapadrao_id in gerapadrao_id_items:
        # obtém o caminho do arquivo que contém a lista de URI
        _uri_list_file_path = get_uri_list_file_path(
            _xc_sps_packages_dir,
            _proc_sps_packages_dir,
            gerapadrao_id,
        )
        # obtém o conteúdo do arquivo que contém a lista de URI
        # Exemplo do conteúdo de `_uri_list_file_path`:
        # /scielo.php?script=sci_serial&pid=0001-3765
        # /scielo.php?script=sci_issues&pid=0001-3765
        # /scielo.php?script=sci_issuetoc&pid=0001-376520200005
        # /scielo.php?script=sci_arttext&pid=S0001-37652020000501101
        uri_list_items = check_website_operations.read_file(_uri_list_file_path)

        # concatena cada item de `_website_url_list` com
        # cada item de `uri_list_items`
        website_uri_list = check_website_operations.concat_website_url_and_uri_list_items(
            _website_url_list, uri_list_items)

        # verifica a lista de URI
        check_website_operations.check_website_uri_list(website_uri_list)

    # atribui um str vazia para sinalizar que o valor foi usado
    Variable.set("GERAPADRAO_ID_FOR_URI_LIST", [], serialize_json=True)


def get_uri_list_file_paths(conf, **kwargs):
    """
    Identifica os caminhos dos arquivos, gerados pelo script `GeraUriList.bat`
    e que contém lista de URI no padrão:
        /scielo.php?script=sci_serial&pid=0001-3765
        /scielo.php?script=sci_issues&pid=0001-3765
        /scielo.php?script=sci_issuetoc&pid=0001-376520200005
        /scielo.php?script=sci_arttext&pid=S0001-37652020000501101
    """
    gerapadrao_id_items = Variable.get(
        "GERAPADRAO_ID_FOR_URI_LIST", default_var=[], deserialize_json=True)
    Logger.info(
        "Obtém os caminhos dos arquivos que contém URI (`uri_list_*.lst`): %s",
        gerapadrao_id_items)

    _xc_sps_packages_dir = Path(Variable.get("XC_SPS_PACKAGES_DIR"))
    _proc_sps_packages_dir = Path(Variable.get("PROC_SPS_PACKAGES_DIR")) / kwargs["run_id"]
    if not _proc_sps_packages_dir.is_dir():
        _proc_sps_packages_dir.mkdir()

    file_paths = [
        str(_proc_sps_packages_dir / f)
        for f in _proc_sps_packages_dir.glob('uri_list_*.lst')]

    for gerapadrao_id in gerapadrao_id_items:
        # obtém o caminho do arquivo que contém a lista de URI
        _uri_list_file_path = get_file_path_in_proc_dir(
            _xc_sps_packages_dir,
            _proc_sps_packages_dir,
            f"uri_list_{gerapadrao_id}.lst"
        )

        if _uri_list_file_path not in file_paths:
            file_paths.append(_uri_list_file_path)

    kwargs["ti"].xcom_push("uri_list_file_paths", file_paths)
    Logger.info("Os arquivos `uri_list_*.lst` são %s", file_paths)


def get_uri_items_from_uri_list_files(**context):
    """
    Retorna uma lista de URI dado uma lista de arquivos `uri_list`
    """
    uri_list_file_paths = context["ti"].xcom_pull(
        task_ids="get_uri_list_file_paths_task", key="uri_list_file_paths"
    )

    items = []
    for file_path in uri_list_file_paths:
        # obtém o conteúdo do arquivo que contém a lista de URI
        # Exemplo do conteúdo de `_uri_list_file_path`:
        # /scielo.php?script=sci_serial&pid=0001-3765
        # /scielo.php?script=sci_issues&pid=0001-3765
        # /scielo.php?script=sci_issuetoc&pid=0001-376520200005
        # /scielo.php?script=sci_arttext&pid=S0001-37652020000501101
        uri_items = check_website_operations.read_file(file_path)
        items.extend(uri_items)

    context["ti"].xcom_push("uri_items", list(set(items)))


def get_pid_list_csv_file_paths(conf, **kwargs):
    """
    Identifica os caminhos dos arquivos CSV
    que contém dados de documentos, sendo a primeira coluna, contém PID v2
    """
    pid_v2_list_file_names = Variable.get(
        "PID_LIST_CSV_FILE_NAMES", default_var=[], deserialize_json=True)
    Logger.info(
        "Obtém os caminhos dos arquivos que contém PIDs: %s",
        pid_v2_list_file_names)

    _xc_sps_packages_dir = Path(Variable.get("XC_SPS_PACKAGES_DIR"))
    _proc_sps_packages_dir = Path(Variable.get("PROC_SPS_PACKAGES_DIR")) / kwargs["run_id"]
    if not _proc_sps_packages_dir.is_dir():
        _proc_sps_packages_dir.mkdir()

    file_paths = [
        str(_proc_sps_packages_dir / f)
        for f in _proc_sps_packages_dir.glob('*.csv')]

    for filename in pid_v2_list_file_names:
        # obtém o caminho do arquivo que contém a lista de PIDs
        _pid_list_csv_file_path = get_file_path_in_proc_dir(
            _xc_sps_packages_dir,
            _proc_sps_packages_dir,
            filename,
        )
        if _pid_list_csv_file_path not in file_paths:
            file_paths.append(_pid_list_csv_file_path)

    kwargs["ti"].xcom_push("pid_list_csv_file_paths", file_paths)
    Logger.info("Os arquivos são: %s", file_paths)


def get_uri_items_from_pid_list_csv_files(**context):
    """
    Retorna uma lista de PIDs dado uma lista de arquivos `pid_list`
    """
    pid_list_csv_file_paths = context["ti"].xcom_pull(
        task_ids="get_pid_list_csv_file_paths_task",
        key="pid_list_csv_file_paths"
    )

    pids = []
    for file_path in pid_list_csv_file_paths:
        pids.extend(check_website_operations.get_pid_list_from_csv(file_path))

    items = check_website_operations.get_uri_list_from_pid_dict(
        group_pids(pids))
    context["ti"].xcom_push("uri_items", items)


check_website_uri_list_task = PythonOperator(
    task_id="check_website_uri_list_id",
    provide_context=True,
    python_callable=check_website_uri_list,
    dag=dag,
)


get_uri_list_file_paths_task = PythonOperator(
    task_id="get_uri_list_file_paths_id",
    provide_context=True,
    python_callable=get_uri_list_file_paths,
    dag=dag,
)


get_uri_items_from_uri_list_files_task = PythonOperator(
    task_id="get_uri_items_from_uri_list_files_id",
    provide_context=True,
    python_callable=get_uri_items_from_uri_list_files,
    dag=dag,
)


get_pid_list_csv_file_paths_task = PythonOperator(
    task_id="get_pid_list_csv_file_paths_id",
    provide_context=True,
    python_callable=get_pid_list_csv_file_paths,
    dag=dag,
)


get_uri_items_from_pid_list_csv_files_task = PythonOperator(
    task_id="get_uri_items_from_pid_list_csv_files_id",
    provide_context=True,
    python_callable=get_uri_items_from_pid_list_csv_files,
    dag=dag,
)

check_website_uri_list_task
