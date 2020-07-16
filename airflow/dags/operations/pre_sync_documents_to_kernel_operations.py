import os
import logging
import shutil
from pathlib import Path

Logger = logging.getLogger(__name__)


def get_sps_packages(scilista_file_path, xc_dir_name, proc_dir_name):
    """
    Obtém Pacotes SPS através da Scilista, movendo os pacotes para o diretório de 
    processamento do Airflow e gera lista dos paths dos pacotes SPS no diretório de 
    processamento.

    list scilista: lista com as linhas do arquivo scilista.lst
        rsp v10n4
        rsp 2018nahead
        csp v4n2-3
    list sps_packages: lista com os paths dos pacotes SPS no diretório de
    processamento
    """
    Logger.debug("get_sps_packages IN")

    xc_dir_path = Path(xc_dir_name)
    proc_dir_path = Path(proc_dir_name)
    sps_packages_list = []

    with open(scilista_file_path) as scilista:
        for scilista_item in scilista.readlines():
            acron_issue = scilista_item.strip().split()
            if len(acron_issue) != 2:
                continue
            filename_pattern = "*{}.zip".format("_".join(acron_issue))
            Logger.info("Reading ZIP files pattern: %s", filename_pattern)

            # verifica na origem
            for item in xc_dir_path.glob(filename_pattern):
                dest_file_path = str(proc_dir_path / item.name)
                if os.path.isfile(dest_file_path):
                    Logger.info("Skip %s", str(item))
                    continue
                Logger.info("Moving %s to %s", str(item), str(proc_dir_path))
                shutil.move(str(item), str(proc_dir_path))

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

    Logger.debug("get_sps_packages OUT")
    return sps_packages_list
