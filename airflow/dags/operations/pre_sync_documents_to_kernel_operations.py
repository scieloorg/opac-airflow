import os
import logging
import shutil
from pathlib import Path
from zipfile import ZipFile

import requests
from lxml import etree

import common.hooks as hooks

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
        for acron_issue in scilista.readlines():
            zip_filename = "{}.zip".format("_".join(acron_issue.split()))
            source = xc_dir_path / zip_filename
            destination = proc_dir_path / zip_filename
            Logger.info("Reading ZIP file: %s" % str(source))
            if os.path.exists(str(source)):
                Logger.info("Moving %s to %s" % (str(source), str(destination)))
                shutil.move(str(source), str(destination))
                sps_packages_list.append(str(destination))

    Logger.debug("get_sps_packages OUT")
    return sps_packages_list
