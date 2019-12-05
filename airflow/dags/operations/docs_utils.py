import os
import logging
import hashlib
import http.client

import requests
from lxml import etree

import common.hooks as hooks
from operations.exceptions import (
    DeleteDocFromKernelException,
    DocumentToDeleteException,
    PutXMLInObjectStoreException,
    ObjectStoreError,
    RegisterUpdateDocIntoKernelException,
    LinkDocumentToDocumentsBundleException,
    Pidv3Exception,
)
from common.sps_package import SPS_Package

Logger = logging.getLogger(__name__)


def delete_doc_from_kernel(doc_to_delete):
    try:
        response = hooks.kernel_connect("/documents/" + doc_to_delete, "DELETE")
    except requests.exceptions.HTTPError as exc:
        raise DeleteDocFromKernelException(str(exc)) from None


def document_to_delete(zipfile, sps_xml_file):
    parser = etree.XMLParser(remove_blank_text=True, no_network=True)
    try:
        metadata = SPS_Package(
            etree.XML(zipfile.read(sps_xml_file), parser), sps_xml_file
        )
    except (etree.XMLSyntaxError, TypeError, KeyError) as exc:
        raise DocumentToDeleteException(str(exc)) from None
    else:
        if metadata.is_document_deletion:
            if metadata.scielo_pid_v3 is None:
                raise DocumentToDeleteException("Missing element in XML")
            return metadata.scielo_pid_v3


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
            "scielo_id": metadata.scielo_pid_v3,
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


def files_sha1(file):
    _sum = hashlib.sha1()
    _sum.update(file)
    return _sum.hexdigest()


def put_object_in_object_store(file, journal, scielo_id, filename):
    """
    - Persistir no Minio
    - Adicionar em dict a URL do Minio
    """

    n_filename = files_sha1(file)
    _, file_extension = os.path.splitext(filename)

    filepath = "{}/{}/{}".format(
        journal, scielo_id, "{}{}".format(n_filename, file_extension)
    )
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
                str(exc),
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
                    ),
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
                str(exc),
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
                    ),
                }
            )

    return {"assets": _assets, "pdfs": _pdfs}


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

    if not xml_data.get("scielo_id"):
        raise Pidv3Exception('Could not get scielo id v3') from None

    Logger.info('Putting XML file "%s" to Object Store', xml_filename)
    xml_data["xml_url"] = put_object_in_object_store(
        xml_file, xml_data["issn"], xml_data["scielo_id"], xml_filename
    )
    return xml_data


def update_documents_in_bundle(bundle_id, payload):
    """
        Relaciona documento com seu fascículo(DocumentsBundle).

        Utiliza a endpoint do Kernel /bundles/{{ DUNDLE_ID }}
    """

    Logger.info('Updating Bundle "%s" with Documents: %s', bundle_id, payload)
    try:
        response = hooks.kernel_connect(
            "/bundles/%s/documents" % bundle_id, "PUT", payload)
        return response
    except requests.exceptions.HTTPError as exc:
        raise LinkDocumentToDocumentsBundleException(str(exc)) from None


def get_bundle_id(issn_id, year, volume=None, number=None, supplement=None):
    """
        Gera Id utilizado na ferramenta de migração para cadastro do documentsbundle.
    """

    if all(list(map(lambda x: x is None, [volume, number, supplement]))):
        return issn_id + "-aop"

    labels = ["issn_id", "year", "volume", "number", "supplement"]
    values = [issn_id, year, volume, number, supplement]

    data = dict([(label, value) for label, value in zip(labels, values)])

    labels = ["issn_id", "year"]
    _id = []
    for label in labels:
        value = data.get(label)
        if value:
            _id.append(value)

    labels = [("volume", "v"), ("number", "n"), ("supplement", "s")]
    for label, prefix in labels:
        value = data.get(label)
        if value:
            if value.isdigit():
                value = str(int(value))
            _id.append(prefix + value)

    return "-".join(_id)


def create_aop_bundle(bundle_id):
    try:
        hooks.kernel_connect("/bundles/" + bundle_id, "PUT")
    except requests.exceptions.HTTPError as exc:
        raise LinkDocumentToDocumentsBundleException(str(exc))
    else:
        journal_aop_path = "/journals/{}/aop".format(bundle_id[:9])
        hooks.kernel_connect(journal_aop_path, "PATCH", {"aop": bundle_id})


def get_or_create_bundle(bundle_id, is_aop):
    try:
        return hooks.kernel_connect("/bundles/" + bundle_id, "GET")
    except requests.exceptions.HTTPError as exc:
        if is_aop and exc.response.status_code == http.client.NOT_FOUND:
            create_aop_bundle(bundle_id)
            try:
                return hooks.kernel_connect("/bundles/" + bundle_id, "GET")
            except requests.exceptions.HTTPError as exc:
                raise LinkDocumentToDocumentsBundleException(str(exc), response=exc.response)
        else:
            raise LinkDocumentToDocumentsBundleException(str(exc), response=exc.response)


def update_aop_bundle_items(issn_id, documents_list):
    try:
        journal_resp = hooks.kernel_connect(f"/journals/{issn_id}", "GET")
    except requests.exceptions.HTTPError as exc:
        raise LinkDocumentToDocumentsBundleException(str(exc))
    else:
        aop_bundle_id = journal_resp.json().get("aop")
        if aop_bundle_id is not None:
            try:
                aop_bundle_resp = hooks.kernel_connect(
                    f"/bundles/{aop_bundle_id}", "GET"
                )
            except requests.exceptions.HTTPError as exc:
                raise LinkDocumentToDocumentsBundleException(str(exc))
            else:
                aop_bundle_items = aop_bundle_resp.json()["items"]
                documents_ids = [document["id"] for document in documents_list]
                updated_aop_items = []
                for aop_item in aop_bundle_items:
                    if aop_item["id"] not in documents_ids:
                        updated_aop_items.append(aop_item)
                    else:
                        Logger.info(
                            'Movindo ex-Ahead of Print "%s" to bundle',
                            aop_item["id"],
                        )
                update_documents_in_bundle(
                    aop_bundle_id,
                    updated_aop_items
                )
