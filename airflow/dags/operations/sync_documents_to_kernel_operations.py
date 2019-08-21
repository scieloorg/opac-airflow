import os
import logging
from zipfile import ZipFile

from operations.exceptions import (
    DeleteDocFromKernelException,
    DocumentToDeleteException,
    PutXMLInObjectStoreException,
    RegisterUpdateDocIntoKernelException,
)

from operations.docs_utils import (
    delete_doc_from_kernel,
    document_to_delete,
    register_update_doc_into_kernel,
    put_assets_and_pdfs_in_object_store,
    put_xml_into_object_store,
    issue_id,
    register_document_to_documentsbundle,
)

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


def delete_documents(sps_package, xmls_filenames):
    """
    Deleta documentos informados do Kernel

    dict sps_packages_xmls: dict com os paths dos pacotes SPS e os respectivos nomes dos
        arquivos XML.
    """
    Logger.debug("delete_documents IN")
    Logger.info("Reading sps_package: %s" % sps_package)
    xmls_to_delete = []
    with ZipFile(sps_package) as zipfile:
        for i, sps_xml_file in enumerate(xmls_filenames, 1):
            Logger.info(
                'Reading XML file "%s" from ZIP file "%s" [%s/%s]',
                sps_xml_file,
                sps_package,
                i,
                len(xmls_filenames),
            )
            try:
                doc_to_delete = document_to_delete(zipfile, sps_xml_file)
            except DocumentToDeleteException as exc:
                Logger.info(
                    'Could not delete document "%s": %s', sps_xml_file, str(exc)
                )
            else:
                if doc_to_delete:
                    xmls_to_delete.append(sps_xml_file)
                    try:
                        delete_doc_from_kernel(doc_to_delete)
                    except DeleteDocFromKernelException as exc:
                        Logger.info(
                            'Could not delete "%s" (scielo_id: "%s") from kernel: %s',
                            sps_xml_file,
                            doc_to_delete,
                            str(exc)
                        )
                    else:
                        Logger.info(
                            'Document "%s" (scielo_id: "%s") deleted from kernel',
                            sps_xml_file,
                            doc_to_delete
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
    with ZipFile(sps_package) as zipfile:
        for i, xml_filename in enumerate(xmls_to_preserve):
            Logger.info(
                'Reading XML file "%s" from ZIP file "%s" [%s/%s]',
                xml_filename,
                sps_package,
                i,
                len(xmls_to_preserve),
            )
            try:
                xml_data = put_xml_into_object_store(zipfile, xml_filename)
            except PutXMLInObjectStoreException as exc:
                Logger.info(
                    'Could not put document "%s" in object store: %s',
                    xml_filename,
                    str(exc),
                )
            else:
                assets_and_pdfs_data = put_assets_and_pdfs_in_object_store(zipfile, xml_data)
                xml_data.update(assets_and_pdfs_data)
                try:
                    register_update_doc_into_kernel(xml_data)

                except RegisterUpdateDocIntoKernelException as exc:
                    Logger.info(
                        'Could not register or update document "%s" in Kernel: %s',
                        xml_filename,
                        str(exc),
                    )

    Logger.debug("register_update_documents OUT")


def relate_documents_to_documentsbundle(documents):
    """
        Relaciona documentos com seu fascículos(DocumentsBundle).

        :param kwargs['documents']: Uma lista de dicionários contento os atributos necessários para a descoberta do fascículo.

            Exemplo contendo a lista de atributos(mínimo):
            [
                {
                 "scielo_id": "S0034-8910.2014048004923",
                 "issn": "0034-8910",
                 "year": "2014",
                 "volume": "48",
                 "number": "2",
                 "order": "347",
                 },
                {
                 "scielo_id": "S0034-8910.2014048004924",
                 "issn": "0034-8910",
                 "year": "2014",
                 "volume": "48",
                 "number": "2",
                 "order": "348",
                 },
                {
                 "scielo_id": "S0034-8910.20140078954641",
                 "issn": "1518-8787",
                 "year": "2014",
                 "volume": "02",
                 "number": "2",
                 "order": "978",
                 },
                {
                 "scielo_id": "S0034-8910.20140078954641",
                 "issn": "1518-8787",
                 "year": "2014",
                 "volume": "02",
                 "number": "2",
                 "order": "978",
                 "supplement": "1",
                 }
            ]

        Return a list of document related or not, something like:
            [
             {'S0034-8910.2014048004923': 'related'},
             {'S0034-8910.20140078954641': 'not_related', 'error': ''}
            ]
    """

    Logger.info("Entrou no relate_documents_to_documentsbundle")

    bundle_id = ''
    bundle_id_doc = {}
    ret = []

    if documents:
        for doc in documents:

            bundle_id = issue_id(issn_id=doc.get("issn"),
                                 year=doc.get("year"),
                                 volume=doc.get("volume", None),
                                 number=doc.get("number", None),
                                 supplement=doc.get("supplement", None))

            bundle_id_doc.setdefault(bundle_id, [])

            payload_doc = {}
            payload_doc['id'] = doc.get("scielo_id")
            payload_doc['order'] = doc.get("order")

            bundle_id_doc[bundle_id].append(payload_doc)

        for bundle_id, payload in bundle_id_doc.items():
            response = register_document_to_documentsbundle(bundle_id, payload)

            ret.append({bundle_id:
                        'related' if response.status_code == 200 else 'not_related'
                        })

        return ret

    Logger.info("Saiu do relate_documents_to_documentsbundle")
