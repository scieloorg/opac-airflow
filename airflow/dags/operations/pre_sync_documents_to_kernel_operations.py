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
        for row in scilista.readlines():
            # Verifica se comando DEL está indicado no fascículo
            acron_issue = row.strip().split()
            if len(acron_issue) != 2:
                continue
            filename_pattern = "*{}.zip".format("_".join(acron_issue))
            Logger.info("Reading ZIP files pattern: %s", filename_pattern)
            files = xc_dir_path.glob(filename_pattern)
            if not files:
                raise FileNotFoundError(
                    "Not found files which pattern is '{}'".format(
                        filename_pattern))
            for source in sorted(files):
                Logger.info("Copying %s to %s", str(source), str(proc_dir_path))
                shutil.copy(str(source), str(proc_dir_path))
                sps_packages_list.append(str(proc_dir_path / source.name))

    Logger.debug("get_sps_packages OUT")
    return sps_packages_list
