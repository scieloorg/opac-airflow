import logging
import os

import requests
from lxml import etree

import common.hooks as hooks
from operations.exceptions import (
    DeleteDocFromKernelException,
    DocumentToDeleteException,
    PutXMLInObjectStoreException,
    ObjectStoreError,
    RegisterUpdateDocIntoKernelException,
)
from common.sps_package import SPS_Package

Logger = logging.getLogger(__name__)


def delete_doc_from_kernel(doc_to_delete):
    try:
        response = hooks.kernel_connect(
            "/documents/" + doc_to_delete, "DELETE"
        )
    except requests.exceptions.HTTPError as exc:
        raise DeleteDocFromKernelException(str(exc)) from None


def document_to_delete(zipfile, sps_xml_file):
    parser = etree.XMLParser(remove_blank_text=True, no_network=True)
    try:
        metadata = SPS_Package(
            etree.XML(zipfile.read(sps_xml_file), parser),
            sps_xml_file
        )
    except (etree.XMLSyntaxError, TypeError, KeyError) as exc:
        raise DocumentToDeleteException(str(exc)) from None
    else:
        if metadata.is_document_deletion:
            if metadata.scielo_id is None:
                raise DocumentToDeleteException('Missing element in XML')
            return metadata.scielo_id


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
    except (etree.XMLSyntaxError, TypeError) as exc:
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
            "year": metadata.year,
            "order": metadata.order,
            "xml_package_name": xml_package_name,
            "assets": [
                {"asset_id": asset_name} for asset_name in metadata.assets_names
            ],
            "pdfs": pdfs,
        }
        for attr in ["volume", "number", "supplement"]:
            if getattr(metadata, attr) is not None:
                _xml_data[attr] = getattr(metadata, attr)
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
        raise ObjectStoreError(
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
    _assets = []
    for asset in (xml_data or {}).get("assets", []):
        Logger.info('Putting Asset file "%s" to Object Store', asset["asset_id"])
        try:
            asset_file = zipfile.read(asset["asset_id"])
        except KeyError as exc:
            Logger.info(
                'Could not read asset "%s" from zipfile "%s": %s',
                asset["asset_id"],
                zipfile,
                str(exc)
            )
        else:
            _assets.append(
                {
                    "asset_id": asset["asset_id"],
                    "asset_url": put_object_in_object_store(
                        asset_file,
                        xml_data["issn"],
                        xml_data["scielo_id"],
                        asset["asset_id"],
                    )
                }
            )
    _pdfs = []
    for pdf in (xml_data or {}).get("pdfs", []):
        Logger.info('Putting PDF file "%s" to Object Store', pdf["filename"])
        try:
            pdf_file = zipfile.read(pdf["filename"])
        except KeyError as exc:
            Logger.info(
                'Could not read PDF "%s" from zipfile "%s": %s',
                pdf["filename"],
                zipfile,
                str(exc)
            )
        else:
            _pdfs.append(
                {
                    "size_bytes": len(pdf_file),
                    "filename": pdf["filename"],
                    "lang": pdf["lang"],
                    "mimetype": pdf["mimetype"],
                    "data_url": put_object_in_object_store(
                        pdf_file,
                        xml_data["issn"],
                        xml_data["scielo_id"],
                        pdf["filename"],
                    )
                }
            )

    return {
        "assets": _assets,
        "pdfs": _pdfs,
    }


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
