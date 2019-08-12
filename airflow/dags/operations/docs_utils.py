import logging
import os

import requests
from lxml import etree

import common.hooks as hooks
from operations.exceptions import (
    PutXMLInObjectStoreException,
    RegisterUpdateDocIntoKernelException,
)
from common.sps_package import SPS_Package

Logger = logging.getLogger(__name__)


def read_file_from_zip(zipfile, filename):
    try:
        return zipfile.read(filename)
    except KeyError as exc:
        raise PutXMLInObjectStoreException(
            'Could not read file "{}" from zipfile "{}": {}'.format(
                filename, zipfile, str(exc)
            )
        ) from None


def register_update_doc_into_kernel(xml_data):

    payload = {"data": xml_data["xml_url"], "assets": xml_data["assets"]}
    try:
        hooks.kernel_connect(
            "/documents/{}".format(xml_data["scielo_id"]), "PUT", payload
        )
    except requests.exceptions.HTTPError as exc:
        raise RegisterUpdateDocIntoKernelException(
            'Could not PUT document "{}" in Kernel : {}'.format(
                xml_data["xml_package_name"], str(exc)
            )
        ) from None
    else:
        for pdf_payload in (xml_data or {}).get("pdfs", []):
            Logger.info('Putting Rendition "%s" to Kernel', pdf_payload["filename"])
            try:
                hooks.kernel_connect(
                    "/documents/{}/renditions".format(xml_data["scielo_id"]),
                    "PATCH",
                    pdf_payload,
                )
            except requests.exceptions.HTTPError as exc:
                raise RegisterUpdateDocIntoKernelException(
                    'Could not PATCH rendition "{}" in Kernel : {}'.format(
                        pdf_payload["filename"], str(exc)
                    )
                ) from None


def get_xml_data(xml_content, xml_package_name):
    """
    - Obter scielo ID
    - Obter infos de periódico e fascículo
    - Obter nomes dos arquivos ativos digitais
    - Obter nomes dos arquivos PDF
    - Obter idiomas (original e traduções)
    """
    parser = etree.XMLParser(remove_blank_text=True, no_network=True)
    try:
        metadata = SPS_Package(etree.XML(xml_content, parser), xml_package_name)
    except TypeError as exc:
        raise PutXMLInObjectStoreException(
            'Could not get xml data from "{}" : {}'.format(xml_package_name, str(exc))
        ) from None
    else:
        pdfs = [
            {
                "lang": metadata.original_language,
                "filename": "{}.pdf".format(xml_package_name),
                "mimetype": "application/pdf",
            }
        ]
        for lang in metadata.translation_languages:
            pdfs.append(
                {
                    "lang": lang,
                    "filename": "{}-{}.pdf".format(xml_package_name, lang),
                    "mimetype": "application/pdf",
                }
            )

        _xml_data = {
            "scielo_id": metadata.scielo_id,
            "issn": metadata.issn,
            "volume": metadata.volume,
            "number": metadata.number,
            "xml_package_name": xml_package_name,
            "assets": [
                {"asset_id": asset_name} for asset_name in metadata.assets_names
            ],
            "pdfs": pdfs,
        }
        if metadata.supplement:
            _xml_data["supplement"] = metadata.supplement
        return _xml_data


def put_object_in_object_store(file, journal, scielo_id, filename):
    """
    - Persistir no Minio
    - Adicionar em dict a URL do Minio
    """
    filepath = "{}/{}/{}".format(journal, scielo_id, filename)
    try:
        return hooks.object_store_connect(file, filepath, "documentstore")
    except Exception as exc:
        raise PutXMLInObjectStoreException(
            'Could not put object "{}" in object store : {}'.format(filepath, str(exc))
        ) from None


def put_assets_and_pdfs_in_object_store(zipfile, xml_data):
    """
    - Ler XML
        - Obter dados do XML
        - Persistir cada ativo digital
        - Persistir cada PDF
        - Persistir XML no Minio
    - Retornar os dados do documento para persistir no Kernel
    - Raise PutXMLInObjectStoreException
    """
    _assets_and_renditions = {}
    for asset in (xml_data or {}).get("assets", []):
        Logger.info('Putting Asset file "%s" to Object Store', asset["asset_id"])
        asset["asset_url"] = put_object_in_object_store(
            read_file_from_zip(zipfile, asset["asset_id"]),
            xml_data["issn"],
            xml_data["scielo_id"],
            asset["asset_id"],
        )
    for pdf in (xml_data or {}).get("pdfs", []):
        Logger.info('Putting PDF file "%s" to Object Store', pdf["filename"])
        pdf_file = read_file_from_zip(zipfile, pdf["filename"])
        pdf["data_url"] = put_object_in_object_store(
            pdf_file,
            xml_data["issn"],
            xml_data["scielo_id"],
            pdf["filename"],
        )
        pdf["size_bytes"] = len(pdf_file)
    return xml_data


def put_xml_into_object_store(zipfile, xml_filename):
    try:
        xml_file = zipfile.read(xml_filename)
    except KeyError as exc:
        raise PutXMLInObjectStoreException(
            'Could not read file "{}" from zipfile "{}": {}'.format(
                xml_filename, zipfile, exc
            )
        ) from None
    xml_data = get_xml_data(xml_file, os.path.splitext(xml_filename)[-2])
    Logger.info('Putting XML file "%s" to Object Store', xml_filename)
    xml_data["xml_url"] = put_object_in_object_store(
        xml_file, xml_data["issn"], xml_data["scielo_id"], xml_filename
    )
    return xml_data
