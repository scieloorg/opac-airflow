import os
import logging
import shutil
import json
from pathlib import Path

Logger = logging.getLogger(__name__)

PREFIX_PATTERN = "[0-9]" * 4
PREFIX_PATTERN += ("-" + ("[0-9]" * 2)) * 5
PREFIX_PATTERN += "-" + ("[0-9]" * 6)


def get_sps_packages(scilista_file_path, xc_dir_name, proc_dir_name):
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
    Logger.debug("get_sps_packages IN")

    xc_dir_path = Path(xc_dir_name)
    proc_dir_path = Path(proc_dir_name)
    sps_packages_list = {}

    package_paths_list = proc_dir_path / "sps_packages.lst"
    if package_paths_list.is_file():
        Logger.info(
            'Pre-sync already done. Returning packages from "%s" file',
            package_paths_list,
        )
        with package_paths_list.open() as sps_list_file:
            return json.load(sps_list_file)

    with open(scilista_file_path) as scilista:
        for scilista_item in scilista.readlines():
            acron_issue = scilista_item.strip().split()
            if len(acron_issue) != 2:
                continue
            bundle_label = "_".join(acron_issue)
            filename_pattern = "{}_{}.zip".format(
                PREFIX_PATTERN, bundle_label)
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
                sps_packages_list[bundle_label] = acron_issue_list
            else:
                Logger.exception(
                    "Missing files which pattern is '%s' in %s and in %s",
                    filename_pattern, str(xc_dir_path), str(proc_dir_path))

    if sps_packages_list:
        Logger.info('Saving SPS packages list in "%s" file', package_paths_list)
        with package_paths_list.open('w') as sps_list_file:
            json.dump(sps_packages_list, sps_list_file)

    Logger.debug("get_sps_packages OUT")
    return sps_packages_list
