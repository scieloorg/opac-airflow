import os
import logging
import shutil
from pathlib import Path
from zipfile import ZipFile

import requests
from lxml import etree

import common.hooks as hooks

Logger = logging.getLogger(__name__)


def list_documents(sps_package):
    """
    Lista todos os XMLs dos SPS Packages da lista obtida do diretório do XC.

    list sps_packages: lista com os paths dos pacotes SPS no diretório de processamento
    dict sps_packages_xmls: dict com os paths dos pacotes SPS e os respectivos nomes dos
        arquivos XML.
    """
    Logger.debug("list_documents IN")
    Logger.info("Reading sps_package: %s" % sps_package)
    with ZipFile(sps_package) as zf:
        xmls_filenames = [
            xml_filename
            for xml_filename in zf.namelist()
            if os.path.splitext(xml_filename)[-1] == ".xml"
        ]
        Logger.debug("list_documents OUT")
        return xmls_filenames


def documents_to_delete(sps_package, sps_xml_files):
    docs_to_delete = []
    xmls_to_delete = []
    with ZipFile(sps_package) as zf:
        for i, sps_xml_file in enumerate(sps_xml_files, 1):
            Logger.info(
                'Reading XML file "%s" from ZIP file "%s" [%s/%s]'
                % (sps_xml_file, sps_package, i, len(sps_xml_files))
            )
            xml_content = zf.read(sps_xml_file)
            if len(xml_content) > 0:
                xml_file = etree.XML(xml_content)
                scielo_id = xml_file.find(".//article-id[@specific-use='scielo']")
                if scielo_id is None:
                    Logger.info(
                        'Cannot read SciELO ID from "%s": missing element in XML'
                        % sps_xml_file
                    )
                else:
                    delete_tag = scielo_id.getparent().find(
                        "./article-id[@specific-use='delete']"
                    )
                    if delete_tag is not None:
                        docs_to_delete.append(scielo_id.text)
                        xmls_to_delete.append(sps_xml_file)
    return xmls_to_delete, docs_to_delete


def delete_documents(sps_package, xmls_filenames):
    """
    Deleta documentos informados do Kernel

    dict sps_packages_xmls: dict com os paths dos pacotes SPS e os respectivos nomes dos
        arquivos XML.
    """
    Logger.debug("delete_documents IN")
    Logger.info("Reading sps_package: %s" % sps_package)
    xmls_to_delete, docs_to_delete = documents_to_delete(sps_package, xmls_filenames)
    for doc_to_delete in docs_to_delete:
        try:
            response = hooks.kernel_connect("/documents/" + doc_to_delete, "DELETE")
        except requests.exceptions.HTTPError as exc:
            Logger.info(
                'Cannot delete "%s" from kernel status: %s' % (doc_to_delete, str(exc))
            )
        else:
            Logger.info(
                "Document %s deleted from kernel status: %d"
                % (doc_to_delete, response.status_code)
            )
    Logger.debug("delete_documents OUT")
    return list(set(xmls_filenames) - set(xmls_to_delete))


def register_update_documents(sps_package, xmls_to_preserve):
    """
    Registra/atualiza documentos informados e seus respectivos ativos digitais e
    renditions no Minio e no Kernel.

    list docs_to_preserve: lista de XMLs para manter no Kernel (Registrar ou atualizar)
    """
    Logger.debug("register_update_documents IN")
    Logger.info("sps_package: %s xmls_to_preserve: %s", sps_package, xmls_to_preserve)
    Logger.debug("register_update_documents OUT")
