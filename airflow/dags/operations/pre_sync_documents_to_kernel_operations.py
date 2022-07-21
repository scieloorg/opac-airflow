import os
import logging
import shutil
from pathlib import Path


def get_id_provider_functions():
    try:
        from scielo_core.id_provider.lib import request_document_id, connect
    except ImportError:
        request_document_id = None
        connect = None
    return {"request_document_id": request_document_id, "connect": connect}


Logger = logging.getLogger(__name__)

PREFIX_PATTERN = "[0-9]" * 4
PREFIX_PATTERN += ("-" + ("[0-9]" * 2)) * 5
PREFIX_PATTERN += "-" + ("[0-9]" * 6)


def get_sps_packages(scilista_file_path, xc_dir_name, proc_dir_name, id_provider_db_uri=None):
    """
    Obtém Pacotes SPS através da Scilista, movendo os pacotes para o diretório de 
    processamento do Airflow e gera lista dos paths dos pacotes SPS no diretório de 
    processamento.

    list scilista: lista com as linhas do arquivo scilista
        rsp v10n4
        rsp 2018nahead
        csp v4n2-3
    list sps_packages: lista com os paths dos pacotes SPS no diretório de
    processamento
    """
    ID_PROVIDER_FUNCTIONS = get_id_provider_functions()
    CONNECT = ID_PROVIDER_FUNCTIONS.get("connect")

    Logger.debug("get_sps_packages IN")
    if id_provider_db_uri and CONNECT:
        try:
            CONNECT(id_provider_db_uri)
        except Exception as e:
            Logger.exception(
                "Unable to connect %s %s %s" %
                (type(e), e, id_provider_db_uri)
            )

    xc_dir_path = Path(xc_dir_name)
    proc_dir_path = Path(proc_dir_name)
    sps_packages_list = []

    package_paths_list = proc_dir_path / "sps_packages.lst"
    if package_paths_list.is_file():
        Logger.info(
            'Pre-sync already done. Returning packages from "%s" file',
            package_paths_list,
        )
        return package_paths_list.read_text().split("\n")

    with open(scilista_file_path) as scilista:
        for scilista_item in scilista.readlines():
            acron_issue = scilista_item.strip().split()
            if len(acron_issue) != 2:
                continue
            filename_pattern = "{}_{}.zip".format(
                PREFIX_PATTERN, "_".join(acron_issue))
            Logger.info("Reading ZIP files pattern: %s", filename_pattern)

            # verifica na origem
            for item in xc_dir_path.glob(filename_pattern):
                dest_file_path = str(proc_dir_path / item.name)
                if os.path.isfile(dest_file_path):
                    Logger.info("Skip %s", str(item))
                    continue

                insert_package_in_proc_dir(str(item), str(proc_dir_path), id_provider_db_uri)

            # verifica no destino
            acron_issue_list = []
            for item in sorted(proc_dir_path.glob(filename_pattern)):
                Logger.info("Found %s", str(proc_dir_path / item.name))
                acron_issue_list.append(str(proc_dir_path / item.name))

            if acron_issue_list:
                sps_packages_list.extend(acron_issue_list)
            else:
                Logger.exception(
                    "Missing files which pattern is '%s' in %s and in %s",
                    filename_pattern, str(xc_dir_path), str(proc_dir_path))

    if sps_packages_list:
        Logger.info('Saving SPS packages list in "%s" file', package_paths_list)
        package_paths_list.write_text("\n".join(sps_packages_list))

    Logger.debug("get_sps_packages OUT")
    return sps_packages_list


def insert_package_in_proc_dir(source_file_path, proc_path, id_provider_db_uri):
    ID_PROVIDER_FUNCTIONS = get_id_provider_functions()
    REQUEST_DOCUMENT_ID = ID_PROVIDER_FUNCTIONS.get("request_document_id")
    if REQUEST_DOCUMENT_ID and id_provider_db_uri:
        try:
            changed_pkg_file_path = os.path.join(proc_path, os.path.basename(source_file_path))
            REQUEST_DOCUMENT_ID(
                source_file_path,
                changed_pkg_file_path,
                "airflow",
                id_provider_db_uri,
            )
        except Exception as e:
            Logger.exception(
                "Unable to request document id: %s %s %s" %
                (type(e), e, source_file_path)
            )
            move_package_to_proc_dir(source_file_path, proc_path)
    else:
        move_package_to_proc_dir(source_file_path, proc_path)


def move_package_to_proc_dir(source_file_path, proc_path):
    Logger.info("Moving %s to %s", source_file_path, proc_path)
    shutil.move(source_file_path, proc_path)
