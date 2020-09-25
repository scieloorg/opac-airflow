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
        "Get file paths which name pattern is `uri_list_*.lst`: %s",
        gerapadrao_id_items)

    _xc_sps_packages_dir = Path(Variable.get("XC_SPS_PACKAGES_DIR"))
    _proc_sps_packages_dir = Path(Variable.get("PROC_SPS_PACKAGES_DIR")) / kwargs["run_id"]
    if not _proc_sps_packages_dir.is_dir():
        _proc_sps_packages_dir.mkdir()

    old_file_paths = [
        str(_proc_sps_packages_dir / f)
        for f in _proc_sps_packages_dir.glob('uri_list_*.lst')]

    file_paths = []
    for gerapadrao_id in gerapadrao_id_items:
        # obtém o caminho do arquivo que contém a lista de URI
        _uri_list_file_path = get_file_path_in_proc_dir(
            _xc_sps_packages_dir,
            _proc_sps_packages_dir,
            f"uri_list_{gerapadrao_id}.lst"
        )

        if _uri_list_file_path not in file_paths:
            file_paths.append(_uri_list_file_path)

    Logger.info("Found: %s", file_paths)

    # atribui um str vazia para sinalizar que o valor foi usado
    Variable.set("GERAPADRAO_ID_FOR_URI_LIST", [], serialize_json=True)

    if old_file_paths or file_paths:
        kwargs["ti"].xcom_push(
            "old_uri_list_file_paths", sorted(old_file_paths))
        kwargs["ti"].xcom_push(
            "new_uri_list_file_paths", sorted(file_paths))
        kwargs["ti"].xcom_push(
            "uri_list_file_paths", sorted(old_file_paths + file_paths))
        return True
    else:
        return False


def get_uri_items_from_uri_list_files(**context):
    """
    Obtém uma lista de URI contidos nos arquivos `uri_list_*.lst`
    """
    Logger.info("Get URI items from `url_list_*.lst`")
    uri_list_file_paths = context["ti"].xcom_pull(
        task_ids="get_uri_list_file_paths_id", key="uri_list_file_paths"
    ) or []

    all_items = set()
    for file_path in uri_list_file_paths:
        # obtém o conteúdo do arquivo que contém a lista de URI
        # Exemplo do conteúdo de `_uri_list_file_path`:
        # /scielo.php?script=sci_serial&pid=0001-3765
        # /scielo.php?script=sci_issues&pid=0001-3765
        # /scielo.php?script=sci_issuetoc&pid=0001-376520200005
        # /scielo.php?script=sci_arttext&pid=S0001-37652020000501101
        partial = [row
                   for row in check_website_operations.read_file(file_path)
                   if len(row.strip())]

        Logger.info("File %s: %i items", file_path, len(partial))
        all_items = all_items | set(partial)

        total = len(all_items)
        Logger.info("Partial total: %i items", total)

    total = len(all_items)
    Logger.info("Total: %i URIs", total)
    if total:
        context["ti"].xcom_push("uri_items", sorted(all_items))

    return total > 0


def get_pid_list_csv_file_paths(**kwargs):
    """
    Identifica os caminhos dos arquivos CSV
    que contém dados de documentos, sendo a primeira coluna, contém PID v2,
    podendo haver mais colunas, e a segunda coluna, é do previous PID v2
    """
    pid_v2_list_file_names = Variable.get(
        "PID_LIST_CSV_FILE_NAMES", default_var=[], deserialize_json=True)
    Logger.info(
        "Get file paths which name pattern is `*.csv`: %s",
        pid_v2_list_file_names)

    _xc_sps_packages_dir = Path(Variable.get("XC_SPS_PACKAGES_DIR"))
    _proc_sps_packages_dir = Path(Variable.get("PROC_SPS_PACKAGES_DIR")) / kwargs["run_id"]
    if not _proc_sps_packages_dir.is_dir():
        _proc_sps_packages_dir.mkdir()

    old_file_paths = [
        str(_proc_sps_packages_dir / f)
        for f in _proc_sps_packages_dir.glob('*.csv')]
    file_paths = []
    for filename in pid_v2_list_file_names:
        # obtém o caminho do arquivo que contém a lista de PIDs
        _pid_list_csv_file_path = get_file_path_in_proc_dir(
            _xc_sps_packages_dir,
            _proc_sps_packages_dir,
            filename,
        )
        if _pid_list_csv_file_path not in file_paths:
            file_paths.append(_pid_list_csv_file_path)

    Logger.info("Found: %s", file_paths)
    if old_file_paths or file_paths:
        kwargs["ti"].xcom_push(
            "old_file_paths", sorted(old_file_paths))
        kwargs["ti"].xcom_push(
            "new_file_paths", sorted(file_paths))
        kwargs["ti"].xcom_push(
            "file_paths", sorted(old_file_paths + file_paths))
        return True
    else:
        return False


def get_uri_items_from_pid_list_csv_files(**context):
    """
    Obtém uma lista de PIDs contidos nos arquivos `*.csv`
    E a partir desta lista, obtém os respectivos URI, no mesmo formato
    encontrado nos arquivos `uri_list_*.lst`
    """
    Logger.info("Get URI items from `*.csv`")
    pid_list_csv_file_paths = context["ti"].xcom_pull(
        task_ids="get_pid_list_csv_file_paths_id",
        key="file_paths"
    ) or []

    pids = set()
    for file_path in pid_list_csv_file_paths:
        _items = {item
                  for item in check_website_operations.get_pid_list_from_csv(
                    file_path)
                  if item and len(item) == 23}
        Logger.info("File %s: %i pids", file_path, len(_items))
        pids = pids | _items
        total = len(pids)
        Logger.info("Partial total: %i pids", total)

    total = len(pids)
    Logger.info("Total: %i pids", total)

    if total:
        items = check_website_operations.get_uri_list_from_pid_dict(
            group_pids(pids))
        context["ti"].xcom_push("pid_items", sorted(pids))
        context["ti"].xcom_push("uri_items", items)
        Logger.info("Total: %i URIs", len(items))
    return total > 0


def group_uri_items_from_uri_lists_by_script_name(**context):
    """
    Agrupa URI items provenientes dos arquivos `uri_list_*.lst`
    pelo `script_name`
    """
    Logger.info("Group URI items, from `uri_list_*.lst`, by script name")
    uri_items = context["ti"].xcom_pull(
                task_ids="get_uri_items_from_uri_list_files_id",
                key="uri_items"
            )
    total = len(uri_items or [])
    Logger.info("Total %i URIs", total)
    if total == 0:
        return True

    items = check_website_operations.group_items_by_script_name(uri_items)
    for script_name, _items in items.items():
        Logger.info(
            "Total %i URIs for `%s`", len(_items), script_name)
        context["ti"].xcom_push(script_name, sorted(_items))
    return len(items) > 0


def merge_uri_items_from_different_sources(**context):
    """
    Une todos URI items provenientes de `uri_list_*.lst` and `*.csv`,
    removendo repetições
    """
    Logger.info("Merge URI items from `uri_list_*.lst` and `*.csv`")
    uri_items_from_lst = context["ti"].xcom_pull(
        task_ids="get_uri_items_from_uri_list_files_id",
        key="uri_items"
    ) or []
    uri_items_from_csv = context["ti"].xcom_pull(
        task_ids="get_uri_items_from_pid_list_csv_files_id",
        key="uri_items"
    ) or []

    uri_items = set(uri_items_from_lst + uri_items_from_csv)
    total = len(uri_items)
    Logger.info("Total %i URIs", total)
    if total:
        context["ti"].xcom_push("uri_items", sorted(uri_items))
    return total > 0


def get_uri_items_grouped_by_script_name(**context):
    """
    Agrupa URI items pelo nome do script
    """
    uri_items = context["ti"].xcom_pull(
        task_ids="merge_uri_items_from_different_sources_id",
        key="uri_items"
    )
    Logger.info("Total %i URIs", len(uri_items))
    items = check_website_operations.group_items_by_script_name(uri_items)
    for script_name, _items in items.items():
        Logger.info(
            "Total %i URIs for `%s`", len(_items), script_name)
        context["ti"].xcom_push(script_name, sorted(_items))
    return bool(items)


def get_website_url_list():
    Logger.info("Get website URL list")
    _website_url_list = Variable.get(
        "WEBSITE_URL_LIST", default_var=[], deserialize_json=True)
    Logger.info(_website_url_list)
    if not _website_url_list:
        raise ValueError("`Variable[\"WEBSITE_URL_LIST\"]` is required")
    return _website_url_list


def get_task_execution_flag(flag_name, task_name):
    """
    FLAGS:
        CHECK_SCI_SERIAL_PAGES,
        CHECK_SCI_ISSUES_PAGES,
        CHECK_SCI_ISSUETOC_PAGES,
        CHECK_SCI_ARTTEXT_PAGES,
        CHECK_SCI_PDF_PAGES,
        CHECK_RENDITIONS,
        CHECK_DIGITAL_ASSETS,
        CHECK_WEB_HTML_PAGES,
        CHECK_WEB_PDF_PAGES,
    """
    flag = Variable.get(flag_name, default_var=True, deserialize_json=True)
    Logger.info("%s: %s", flag_name, "on" if flag else "off")
    if flag is False:
        Logger.info(
            "Skip '%s', because '%s' is off",
            task_name, flag_name)
    return flag


def check_any_uri_items(uri_list_items, label, dag_info):
    flag_name = "CHECK_{}_PAGES".format(label.upper())
    flag = get_task_execution_flag(
        flag_name, "checking '%s' pages".format(label))
    if flag is False:
        return 0

    _website_url_list = get_website_url_list()

    total = len(uri_list_items or [])
    Logger.info("Total URI items: %i", total)
    if total == 0:
        return 0

    # concatena cada item de `_website_url_list` com
    # cada item de `uri_list_items`
    website_uri_list = check_website_operations.concat_website_url_and_uri_list_items(
        _website_url_list, uri_list_items)

    # verifica a lista de URI
    success, failures = check_website_operations.check_website_uri_list(
        website_uri_list, label)

    Logger.info(
        "Checked total %i: %i failures and %i success",
        len(website_uri_list),
        len(failures),
        len(success),
    )

    # registra na base de dados as falhas
    Logger.info(
        "Register the %i records of %s which is UNAVAILABLE",
        len(failures),
        label,
    )
    check_website_operations.register_sci_pages_availability_report(
        failures, dag_info)

    # registra na base de dados os sucessos
    Logger.info(
        "Register the %i records of %s which is available",
        len(success),
        label,
    )
    check_website_operations.register_sci_pages_availability_report(
        success, dag_info)

    return len(website_uri_list)


def check_sci_serial_uri_items(**context):
    """
    Executa ``check_website.check_sci_serial_uri_items`` para o padrão de URI
    /scielo.php?script=sci_serial&pid=0001-3765
    """
    Logger.info("Check `sci_serial` URI list")

    uri_list_items = context["ti"].xcom_pull(
        task_ids="get_uri_items_grouped_by_script_name_id",
        key="sci_serial")

    total = check_any_uri_items(uri_list_items, "sci_serial", context)

    Logger.info("Checked %i `sci_serial` URI items", total)


def check_sci_issues_uri_items(**context):
    """
    Executa ``check_website.check_sci_issues_uri_items`` para o padrão de URI
    /scielo.php?script=sci_issues&pid=0001-3765
    """
    Logger.info("Check `sci_issues` URI list")

    uri_list_items = context["ti"].xcom_pull(
        task_ids="get_uri_items_grouped_by_script_name_id",
        key="sci_issues")

    total = check_any_uri_items(uri_list_items, "sci_issues", context)

    Logger.info("Checked %i `sci_issues` URI items", total)


def check_sci_issuetoc_uri_items(**context):
    """
    Executa ``check_website.check_sci_issuetoc_uri_items`` para o padrão de URI
    /scielo.php?script=sci_issuetoc&pid=0001-376520200005
    """
    Logger.info("Check `sci_issuetoc` URI list")

    uri_list_items = context["ti"].xcom_pull(
        task_ids="get_uri_items_grouped_by_script_name_id",
        key="sci_issuetoc")

    total = check_any_uri_items(uri_list_items, "sci_issuetoc", context)

    Logger.info("Checked %i `sci_issuetoc` URI items", total)


def check_sci_pdf_uri_items(**context):
    """
    Executa ``check_website.check_sci_pdf_uri_items`` para o padrão de URI
    /scielo.php?script=sci_pdf&pid=0001-376520200005
    """
    Logger.info("Check `sci_pdf` URI list")

    uri_list_items = context["ti"].xcom_pull(
        task_ids="get_uri_items_grouped_by_script_name_id",
        key="sci_pdf")

    total = check_any_uri_items(uri_list_items, "sci_pdf", context)

    Logger.info("Checked %i `sci_pdf` URI items", total)


def check_sci_arttext_uri_items(**context):
    """
    Executa ``check_website.check_sci_arttext_uri_items`` para o padrão de URI
    /scielo.php?script=sci_arttext&pid=0001-376520200005
    """
    Logger.info("Check `sci_arttext` URI list")

    uri_list_items = context["ti"].xcom_pull(
        task_ids="get_uri_items_grouped_by_script_name_id",
        key="sci_arttext")

    total = check_any_uri_items(uri_list_items, "sci_arttext", context)

    Logger.info("Checked %i `sci_arttext` URI items", total)


def check_documents_deeply(**context):
    """
    Executa ``check_website.check_documents_deeply`` para a lista de PID v3
    """
    Logger.info("Check documents deeply")

    flags = {
        name: get_task_execution_flag(
            name, name.replace("CHECK", "checking").replace("_", " ").lower())
        for name in ("CHECK_RENDITIONS",
                     "CHECK_DIGITAL_ASSETS",
                     "CHECK_WEB_HTML_PAGES",
                     "CHECK_WEB_PDF_PAGES",)
    }
    if not any(flags.values()):
        Logger.warning(
            "'check_documents_deeply_id' was NOT executed because "
            "all FLAGS are set to 'false'")
        return False

    website_url = context["ti"].xcom_pull(
        task_ids="get_pid_v3_list_id",
        key="website_url")
    if website_url is None:
        raise ValueError(
            "Unable to execute this task because `website_url` is not set")

    object_store_url = Variable.get("OBJECT_STORE_URL", default_var="")

    pid_v3_list = context["ti"].xcom_pull(
        task_ids="get_pid_v3_list_id",
        key="pid_v3_list")

    total = len(pid_v3_list or [])
    Logger.info("PID v3: %i items", total)
    if total == 0:
        Logger.warning("There is no PID v3 to check")
        return False

    extra_data = context.copy()
    extra_data.update(flags)

    pid_v2_processed = check_website_operations.check_website_uri_list_deeply(
        pid_v3_list, website_url, object_store_url, extra_data)
    total_processed_pid_v2 = len(pid_v2_processed or [])

    if total_processed_pid_v2 > 0:
        context["ti"].xcom_push(
            "processed_pid_v2_items", sorted(pid_v2_processed))

    Logger.info("Checked %i documents", total)
    Logger.info("Checked %i PID v2 items", total_processed_pid_v2)
    return total_processed_pid_v2 > 0


def get_pid_v3_list(**context):
    """
    Executa ``check_website.get_pid_v3_list``
    para o obter os pid v3 a partir do padrão de URI
    /scielo.php?script=sci_arttext&pid=S0001-37652020000501101
    """
    Logger.info("Get PID v3 from old pattern document URI")

    _website_url_list = get_website_url_list()
    website_url = check_website_operations.get_main_website_url(
        _website_url_list)
    if website_url is None:
        raise ValueError(
            "Unable to identify which one is the (new) SciELO website "
            "in this list: {}".format(_website_url_list)
        )

    uri_items = context["ti"].xcom_pull(
        task_ids="get_uri_items_grouped_by_script_name_id",
        key="sci_arttext")
    if uri_items is None or len(uri_items) == 0:
        raise ValueError("Missing URI items to get PID v3")

    pid_v3_list = check_website_operations.get_pid_v3_list(
        uri_items, website_url)

    if pid_v3_list:
        context["ti"].xcom_push("pid_v3_list", pid_v3_list)
        context["ti"].xcom_push("website_url", website_url)

    total = len(pid_v3_list or [])
    Logger.info("PID v3: %i items", total)
    return total > 0


def check_input_vs_processed_pids(**context):
    """
    Une todos PID provenientes de `uri_list_*.lst` and `*.csv`,
    removendo repetições
    E verifica se todos eles foram processados, comparando com o resultado
    da tarefa `check_documents_deeply` 
    """
    Logger.info(
        "Check if all the PID items from `uri_list_*.lst` and `*.csv` "
        "were processed at the end"
    )
    pid_v2_items_from_lst = context["ti"].xcom_pull(
        task_ids="group_uri_items_from_uri_lists_by_script_name_id",
        key="sci_arttext"
    ) or []
    pid_v2_items_from_csv = context["ti"].xcom_pull(
        task_ids="get_uri_items_from_pid_list_csv_files_id",
        key="pid_items"
    ) or []

    Logger.info("Total %i PIDs v2 from uri_list", len(pid_v2_items_from_lst))
    Logger.info("Total %i PIDs v2 from csv", len(pid_v2_items_from_csv))

    pid_items = set(pid_v2_items_from_lst + pid_v2_items_from_csv)
    processed = set(context["ti"].xcom_pull(
        task_ids="check_documents_deeply_id",
        key="processed_pid_v2_items") or [])

    Logger.info("Total %i input PIDs", len(pid_items))
    Logger.info("Total %i processed PIDs", len(processed))

    present_in_pid_items_but_not_in_processed = sorted(pid_items - processed)
    present_in_processed_but_not_in_pid_items = sorted(processed - pid_items)
    present_in_both = pid_items & processed

    if present_in_processed_but_not_in_pid_items:
        Logger.warning(
            "There are %i processed PIDs which were not in input lists:\n%s"
            "(Probably because they are previous PID)",
            len(present_in_processed_but_not_in_pid_items),
            "\n".join(present_in_processed_but_not_in_pid_items))
    if present_in_pid_items_but_not_in_processed:
        Logger.error(
            "There are %i PIDs which are in input lists, "
            "but were not processed:\n%s",
            len(present_in_pid_items_but_not_in_processed),
            "\n".join(present_in_pid_items_but_not_in_processed))

    # intersecção de pid_items e processed é igual a pid_items
    # então, todos os PIDs de entrada foram processados
    if present_in_both == pid_items:
        Logger.info("All the PIDs were processed")

    context["ti"].xcom_push(
        "present_in_processed_but_not_in_pid_items",
        present_in_processed_but_not_in_pid_items)
    context["ti"].xcom_push(
        "present_in_pid_items_but_not_in_processed",
        present_in_pid_items_but_not_in_processed)
    return present_in_both == pid_items


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

group_uri_items_from_uri_lists_by_script_name_task = PythonOperator(
    task_id="group_uri_items_from_uri_lists_by_script_name_id",
    provide_context=True,
    python_callable=group_uri_items_from_uri_lists_by_script_name,
    dag=dag,
)

merge_uri_items_from_different_sources_task = ShortCircuitOperator(
    task_id="merge_uri_items_from_different_sources_id",
    provide_context=True,
    python_callable=merge_uri_items_from_different_sources,
    dag=dag,
)

get_uri_items_grouped_by_script_name_task = ShortCircuitOperator(
    task_id="get_uri_items_grouped_by_script_name_id",
    provide_context=True,
    python_callable=get_uri_items_grouped_by_script_name,
    dag=dag,
)

check_sci_serial_uri_items_task = PythonOperator(
    task_id="check_sci_serial_uri_items_id",
    provide_context=True,
    python_callable=check_sci_serial_uri_items,
    dag=dag,
)

check_sci_issues_uri_items_task = PythonOperator(
    task_id="check_sci_issues_uri_items_id",
    provide_context=True,
    python_callable=check_sci_issues_uri_items,
    dag=dag,
)

check_sci_issuetoc_uri_items_task = PythonOperator(
    task_id="check_sci_issuetoc_uri_items_id",
    provide_context=True,
    python_callable=check_sci_issuetoc_uri_items,
    dag=dag,
)

check_sci_pdf_uri_items_task = PythonOperator(
    task_id="check_sci_pdf_uri_items_id",
    provide_context=True,
    python_callable=check_sci_pdf_uri_items,
    dag=dag,
)

check_sci_arttext_uri_items_task = PythonOperator(
    task_id="check_sci_arttext_uri_items_id",
    provide_context=True,
    python_callable=check_sci_arttext_uri_items,
    dag=dag,
)

get_pid_v3_list_task = ShortCircuitOperator(
    task_id="get_pid_v3_list_id",
    provide_context=True,
    python_callable=get_pid_v3_list,
    dag=dag,
)

check_documents_deeply_task = PythonOperator(
    task_id="check_documents_deeply_id",
    provide_context=True,
    python_callable=check_documents_deeply,
    dag=dag,
)

check_input_vs_processed_pids_task = PythonOperator(
    task_id="check_input_vs_processed_pids_id",
    provide_context=True,
    python_callable=check_input_vs_processed_pids,
    dag=dag,
)
"""
Dependência das tarefas
    o
    |
    |   +----------------------------+
    +-->|get_uri_list_file_paths_task|
        +----------------------------+
            |
            |   +--------------------------------------+
            +-->|get_uri_items_from_uri_list_files_task|---------------------+
                +--------------------------------------+                     |
                    |                                                        |
                    |   +--------------------------------------------------+ |
                    +-->|group_uri_items_from_uri_lists_by_script_name_task| |
                        +--------------------------------------------------+ |
                                                                             |
o                                                                            |
|                                                                            |
|   +--------------------------------+                                       |
+-->|get_pid_list_csv_file_paths_task|                                       |
    +--------------------------------+                                       |
        |                                                                    |
        |   +------------------------------------------+                     |
        +-->|get_uri_items_from_pid_list_csv_files_task|                     |
            +------------------------------------------+                     |
                |                                                            |
                |   +-------------------------------------------+            |
                +-->|merge_uri_items_from_different_sources_task|<-----------+
                    +-------------------------------------------+
                        |
                        |   +-----------------------------------------+
                        +-->|get_uri_items_grouped_by_script_name_task|
                            +-----------------------------------------+
                                |
                                |   +--------------------------------+
                                +-->|check_sci_arttext_uri_items_task|
                                |   +--------------------------------+
                                |
                                |   +-------------------------------+
                                +-->|check_sci_issues_uri_items_task|
                                |   +-------------------------------+
                                |
                                |   +---------------------------------+
                                +-->|check_sci_issuetoc_uri_items_task|
                                |   +---------------------------------+
                                |
                                |   +----------------------------+
                                +-->|check_sci_pdf_uri_items_task|
                                |   +----------------------------+
                                |
                                |   +-------------------------------+
                                +-->|check_sci_serial_uri_items_task|
                                |   +-------------------------------+
                                |
                                |   +--------------------+
                                +-->|get_pid_v3_list_task|
                                    +--------------------+
                                        |
                                        |   +---------------------------+
                                        +-->|check_documents_deeply_task|
                                            +---------------------------+
                                                |
                                                |   +----------------------------------+
                                                +-->|check_input_vs_processed_pids_task|
                                                    +----------------------------------+

"""
# obtém a lista de arquivos uri_list
# ler todos os arquivos url_list e retorna uma lista de URI
get_uri_list_file_paths_task >> get_uri_items_from_uri_list_files_task
# agrupa URI items provenientes de `uri_list_*.lst` pelo nome do script
get_uri_items_from_uri_list_files_task >> group_uri_items_from_uri_lists_by_script_name_task

# obtém a lista de arquivos que contém pid v2
# ler todos os arquivos que contém pid v2 e retorna uma lista de URI
get_pid_list_csv_file_paths_task >> get_uri_items_from_pid_list_csv_files_task

# junta os ítens de URI provenientes de ambos tipos de arquivos
get_uri_items_from_pid_list_csv_files_task >> merge_uri_items_from_different_sources_task << get_uri_items_from_uri_list_files_task

# agrupa URI items, provenientes de ambos tipos de arquivos, pelo nome do script
merge_uri_items_from_different_sources_task >> get_uri_items_grouped_by_script_name_task

# valida os URI de sci_serial (página do periódico)
get_uri_items_grouped_by_script_name_task >> check_sci_serial_uri_items_task

# valida os URI de sci_issues (grades de fascículos)
get_uri_items_grouped_by_script_name_task >> check_sci_issues_uri_items_task

# valida os URI de sci_issuetoc (sumarios)
get_uri_items_grouped_by_script_name_task >> check_sci_issuetoc_uri_items_task

# valida os URI de sci_pdf (PDF dos documentos)
get_uri_items_grouped_by_script_name_task >> check_sci_pdf_uri_items_task

# valida os URI de sci_arttext (HTML dos documentos)
get_uri_items_grouped_by_script_name_task >> check_sci_arttext_uri_items_task

# obtém a lista de pid v3
get_uri_items_grouped_by_script_name_task >> get_pid_v3_list_task

# valida os documentos no nível mais profundo
get_pid_v3_list_task >> check_documents_deeply_task

# verifica os PID v2 de entrada vs os PID v2 processados
check_documents_deeply_task >> check_input_vs_processed_pids_task
