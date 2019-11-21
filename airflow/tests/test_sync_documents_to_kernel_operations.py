import os
import copy
import tempfile
import builtins
import json
from unittest import TestCase, main
from unittest.mock import patch, Mock, MagicMock

import requests
from airflow import DAG

from operations.sync_documents_to_kernel_operations import (
    list_documents,
    delete_documents,
    register_update_documents,
    link_documents_to_documentsbundle,
)
from operations.exceptions import (
    DeleteDocFromKernelException,
    DocumentToDeleteException,
    PutXMLInObjectStoreException,
    RegisterUpdateDocIntoKernelException,
    LinkDocumentToDocumentsBundleException
)


class TestListDocuments(TestCase):
    def setUp(self):
        self.sps_package = "dir/destination/abc_v50.zip"

    @patch("operations.sync_documents_to_kernel_operations.ZipFile")
    def test_list_document_opens_all_zips(self, MockZipFile):
        list_documents(self.sps_package)
        MockZipFile.assert_called_once_with(self.sps_package)

    @patch("operations.sync_documents_to_kernel_operations.ZipFile")
    def test_list_document_raises_error_if_zipfile_not_found(self, MockZipFile):
        MockZipFile.side_effect = FileNotFoundError
        self.assertRaises(FileNotFoundError, list_documents, self.sps_package)

    @patch("operations.sync_documents_to_kernel_operations.ZipFile")
    def test_list_document_reads_all_xmls_from_zip(self, MockZipFile):
        sps_package_file_lists = [
            "0123-4567-abc-50-1-8.xml",
            "v53n1a01.pdf",
            "0123-4567-abc-50-1-8-gpn1a01t1.htm",
            "0123-4567-abc-50-1-8-gpn1a01g1.htm",
            "0123-4567-abc-50-9-18.xml",
            "v53n1a02.pdf",
        ]
        MockZipFile.return_value.__enter__.return_value.namelist.return_value = (
            sps_package_file_lists
        )
        result = list_documents(self.sps_package)
        self.assertEqual(
            result, ["0123-4567-abc-50-1-8.xml", "0123-4567-abc-50-9-18.xml"]
        )

    @patch("operations.sync_documents_to_kernel_operations.ZipFile")
    def test_list_document_empty_list_if_no_xml_in_zip(self, MockZipFile):
        sps_package_file_lists = [
            "v53n1a01.pdf",
            "0123-4567-abc-50-1-8-gpn1a01t1.htm",
            "0123-4567-abc-50-1-8-gpn1a01g1.htm",
            "v53n1a02.pdf",
        ]
        MockZipFile.return_value.__enter__.return_value.namelist.return_value = (
            sps_package_file_lists
        )
        result = list_documents(self.sps_package)
        self.assertEqual(result, [])


class TestDeleteDocuments(TestCase):
    def setUp(self):
        self.kwargs = {
            "sps_package": "dir/destination/rba_v53n1.zip",
            "xmls_filenames": [
                "1806-907X-rba-53-01-1-8.xml",
                "1806-907X-rba-53-01-9-18.xml",
                "1806-907X-rba-53-01-19-25.xml",
            ],
        }
        self.docs_to_delete = [
            "FX6F3cbyYmmwvtGmMB7WCgr",
            "GZ5K2cbyYmmwvtGmMB71243",
            "KU890cbyYmmwvtGmMB7JUk4",
        ]

    @patch("operations.sync_documents_to_kernel_operations.delete_doc_from_kernel")
    @patch("operations.sync_documents_to_kernel_operations.document_to_delete")
    @patch("operations.sync_documents_to_kernel_operations.ZipFile")
    def test_delete_documents_opens_zip(
        self, MockZipFile, mk_document_to_delete, mk_delete_doc_from_kernel
    ):
        delete_documents(**self.kwargs)
        MockZipFile.assert_called_once_with(self.kwargs["sps_package"])

    @patch("operations.sync_documents_to_kernel_operations.delete_doc_from_kernel")
    @patch("operations.sync_documents_to_kernel_operations.document_to_delete")
    @patch("operations.sync_documents_to_kernel_operations.ZipFile")
    def test_delete_documents_calls_document_to_delete_for_each_xml(
        self, MockZipFile, mk_document_to_delete, mk_delete_doc_from_kernel
    ):
        delete_documents(**self.kwargs)
        for sps_xml_file in self.kwargs["xmls_filenames"]:
            with self.subTest(sps_xml_file=sps_xml_file):
                mk_document_to_delete.assert_any_call(
                    MockZipFile.return_value.__enter__.return_value,
                    sps_xml_file
                )

    @patch("operations.sync_documents_to_kernel_operations.delete_doc_from_kernel")
    @patch("operations.sync_documents_to_kernel_operations.Logger")
    @patch("operations.sync_documents_to_kernel_operations.document_to_delete")
    @patch("operations.sync_documents_to_kernel_operations.ZipFile")
    def test_delete_documents_logs_error_if_document_to_delete_error(
        self, MockZipFile, mk_document_to_delete, MockLogger, mk_delete_doc_from_kernel
    ):
        mk_document_to_delete.side_effect = DocumentToDeleteException("XML Error")
        delete_documents(**self.kwargs)
        mk_delete_doc_from_kernel.assert_not_called()
        for xml_filename in self.kwargs["xmls_filenames"]:
            with self.subTest(xml_filename=xml_filename):
                MockLogger.info.assert_any_call(
                    'Could not delete document "%s": %s', xml_filename, "XML Error"
                )

    @patch("operations.sync_documents_to_kernel_operations.delete_doc_from_kernel")
    @patch("operations.sync_documents_to_kernel_operations.Logger")
    @patch("operations.sync_documents_to_kernel_operations.document_to_delete")
    @patch("operations.sync_documents_to_kernel_operations.ZipFile")
    def test_delete_documents_calls_delete_doc_from_kernel(
        self, MockZipFile, mk_document_to_delete, MockLogger, mk_delete_doc_from_kernel
    ):
        mk_document_to_delete.side_effect = self.docs_to_delete
        delete_documents(**self.kwargs)
        for doc_to_delete in self.docs_to_delete:
            with self.subTest(doc_to_delete=doc_to_delete):
                mk_delete_doc_from_kernel.assert_any_call(doc_to_delete)

    @patch("operations.sync_documents_to_kernel_operations.delete_doc_from_kernel")
    @patch("operations.sync_documents_to_kernel_operations.Logger")
    @patch("operations.sync_documents_to_kernel_operations.document_to_delete")
    @patch("operations.sync_documents_to_kernel_operations.ZipFile")
    def test_delete_documents_logs_error_if_kernel_connect_error(
        self, MockZipFile, mk_document_to_delete, MockLogger, mk_delete_doc_from_kernel
    ):
        mk_document_to_delete.side_effect = self.docs_to_delete
        mk_delete_doc_from_kernel.side_effect = DeleteDocFromKernelException(
            "404 Client Error: Not Found"
        )
        delete_documents(**self.kwargs)
        for sps_xml_file, doc_to_delete in zip(
                self.kwargs["xmls_filenames"], self.docs_to_delete
            ):
            with self.subTest(sps_xml_file=sps_xml_file, doc_to_delete=doc_to_delete):
                MockLogger.info.assert_any_call(
                    'Could not delete "%s" (scielo_id: "%s") from kernel: %s',
                    sps_xml_file,
                    doc_to_delete,
                    "404 Client Error: Not Found"
                )

    @patch("operations.sync_documents_to_kernel_operations.delete_doc_from_kernel")
    @patch("operations.sync_documents_to_kernel_operations.Logger")
    @patch("operations.sync_documents_to_kernel_operations.document_to_delete")
    @patch("operations.sync_documents_to_kernel_operations.ZipFile")
    def test_delete_documents_logs_document_deletion_success(
        self, MockZipFile, mk_document_to_delete, MockLogger, mk_delete_doc_from_kernel
    ):
        mk_document_to_delete.side_effect = self.docs_to_delete
        delete_documents(**self.kwargs)
        for sps_xml_file, doc_to_delete in zip(
                self.kwargs["xmls_filenames"], self.docs_to_delete
            ):
            with self.subTest(sps_xml_file=sps_xml_file, doc_to_delete=doc_to_delete):
                MockLogger.info.assert_any_call(
                    'Document "%s" (scielo_id: "%s") deleted from kernel',
                    sps_xml_file,
                    doc_to_delete
                )

    @patch("operations.sync_documents_to_kernel_operations.delete_doc_from_kernel")
    @patch("operations.sync_documents_to_kernel_operations.Logger")
    @patch("operations.sync_documents_to_kernel_operations.document_to_delete")
    @patch("operations.sync_documents_to_kernel_operations.ZipFile")
    def test_delete_documents_returns_xmls_to_preserve(
        self, MockZipFile, mk_document_to_delete, MockLogger, mk_delete_doc_from_kernel
    ):
        docs_to_delete = [
            "FX6F3cbyYmmwvtGmMB7WCgr", "GZ5K2cbyYmmwvtGmMB71243", None
        ]
        mk_document_to_delete.side_effect = docs_to_delete
        result = delete_documents(**self.kwargs)
        self.assertEqual(
            result,
            list(
                set(self.kwargs["xmls_filenames"]) - set(self.kwargs["xmls_filenames"][:-1])
            )
        )


class TestRegisterUpdateDocuments(TestCase):
    """
    - Minio
        - Abrir o ZIP
        - Ler cada XML
            - Obter scielo ID
            - Obter infos de periódico e fascículo
            - Obter nomes dos arquivos ativos digitais
            - Obter nomes dos arquivos PDF
            - Obter idiomas (original e traduções)
            - Ler cada ativo digital
                - Persistir no Minio
                - Adicionar em dict a URL do Minio
            - Ler cada PDF
                - Persistir no Minio
                - Adicionar em dict a URL do Minio
            - Persistir XML no Minio
            - Adicionar em dict a URL do Minio
    """

    def setUp(self):
        self.kwargs = {
            "sps_package": "dir/destination/rba_v53n1.zip",
            "xmls_to_preserve": [
                "1806-907X-rba-53-01-1-8.xml",
                "1806-907X-rba-53-01-9-18.xml",
                "1806-907X-rba-53-01-19-25.xml",
            ],
        }
        self.xmls_data = [
            {
                "journal": "1806-907X",
                "scielo_id": "FX6F3cbyYmmwvtGmMB7WCgr",
                "xml_url": "http://minio/documentstore/1806-907X-rba-53-01-1-8.xml",
                "assets": [
                    {
                        "asset_id": "1806-907X-rba-53-01-1-8-g01.jpg",
                    },
                    {
                        "asset_id": "1806-907X-rba-53-01-1-8-g02.jpg",
                    },
                ],
                "pdfs": [
                    {
                        "lang": "en",
                        "filename": "1806-907X-rba-53-01-1-8.pdf",
                        "mimetype": "application/pdf",
                    },
                    {
                        "lang": "pt",
                        "filename": "1806-907X-rba-53-01-1-8-pt.pdf",
                        "mimetype": "application/pdf",
                    },
                    {
                        "lang": "de",
                        "filename": "1806-907X-rba-53-01-1-8-de.pdf",
                        "mimetype": "application/pdf",
                    },
                ],
            },
            {
                "journal": "1806-907X",
                "scielo_id": "GZ5K2cbyYmmwvtGmMB71243",
                "xml_url": "http://minio/documentstore/1806-907X-rba-53-01-9-18.xml",
                "assets": [
                    {
                        "asset_id": "1806-907X-rba-53-01-9-18-g01.jpg",
                    },
                    {
                        "asset_id": "1806-907X-rba-53-01-9-18-g02.jpg",
                    },
                ],
                "pdfs": [
                    {
                        "lang": "en",
                        "filename": "1806-907X-rba-53-01-9-18.pdf",
                        "mimetype": "application/pdf",
                    }
                ],
            },
            {
                "journal": "1806-907X",
                "scielo_id": "KU890cbyYmmwvtGmMB7JUk4",
                "xml_url": "http://minio/documentstore/1806-907X-rba-53-01-19-25.xml",
                "assets": [
                    {
                        "asset_id": "1806-907X-rba-53-01-19-25-tb01.tiff",
                    }
                ],
                "pdfs": [
                    {
                        "lang": "es",
                        "filename": "1806-907X-rba-53-01-19-25.pdf",
                        "mimetype": "application/pdf",
                    },
                    {
                        "lang": "en",
                        "filename": "1806-907X-rba-53-01-19-25-en.pdf",
                        "mimetype": "application/pdf",
                    },
                ],
            },
        ]
        self.zip_file_returns = [
            b"1806-907X-rba-53-01-1-8.xml",
            b"1806-907X-rba-53-01-1-8-g01.jpg",
            b"1806-907X-rba-53-01-1-8-g02.jpg",
            b"1806-907X-rba-53-01-1-8.pdf",
            b"1806-907X-rba-53-01-1-8-en.pdf",
            b"1806-907X-rba-53-01-1-8-de.pdf",
            b"1806-907X-rba-53-01-9-18.xml",
            b"1806-907X-rba-53-01-9-18-g1.gif",
            b"1806-907X-rba-53-01-9-18-g2.gif",
            b"1806-907X-rba-53-01-9-18-g3.gif",
            b"1806-907X-rba-53-01-9-18.pdf",
            b"1806-907X-rba-53-01-19-25.xml",
            b"1806-907X-rba-53-01-19-25-tb01.tiff",
            b"1806-907X-rba-53-01-19-25.pdf",
            b"1806-907X-rba-53-01-19-25-en.pdf",
        ]

    @patch(
        "operations.sync_documents_to_kernel_operations.register_update_doc_into_kernel"
    )
    @patch(
        "operations.sync_documents_to_kernel_operations.put_assets_and_pdfs_in_object_store"
    )
    @patch("operations.sync_documents_to_kernel_operations.put_xml_into_object_store")
    @patch("operations.sync_documents_to_kernel_operations.Logger")
    @patch("operations.sync_documents_to_kernel_operations.ZipFile")
    def test_register_update_documents_opens_zip(
        self,
        MockZipFile,
        MockLogger,
        mk_put_xml_into_object_store,
        mk_put_assets_and_pdfs_in_object_store,
        mk_register_update_doc_into_kernel,
    ):
        register_update_documents(**self.kwargs)
        MockZipFile.assert_called_once_with(self.kwargs["sps_package"])

    @patch("operations.sync_documents_to_kernel_operations.ZipFile")
    def test_register_update_documents_open_zip_raises_error(self, MockZipFile):
        MockZipFile.side_effect = Exception("Zipfile error")
        self.assertRaises(Exception, register_update_documents, **self.kwargs)

    @patch(
        "operations.sync_documents_to_kernel_operations.register_update_doc_into_kernel"
    )
    @patch(
        "operations.sync_documents_to_kernel_operations.put_assets_and_pdfs_in_object_store"
    )
    @patch("operations.sync_documents_to_kernel_operations.put_xml_into_object_store")
    @patch("operations.sync_documents_to_kernel_operations.Logger")
    @patch("operations.sync_documents_to_kernel_operations.ZipFile")
    def test_register_update_documents_calls_put_xml_into_object_store(
        self,
        MockZipFile,
        MockLogger,
        mk_put_xml_into_object_store,
        mk_put_assets_and_pdfs_in_object_store,
        mk_register_update_doc_into_kernel,
    ):
        register_update_documents(**self.kwargs)
        for xml_filename in self.kwargs["xmls_to_preserve"]:
            with self.subTest(xml_filename=xml_filename):
                mk_put_xml_into_object_store.assert_any_call(
                    MockZipFile.return_value.__enter__.return_value, xml_filename
                )

    @patch(
        "operations.sync_documents_to_kernel_operations.register_update_doc_into_kernel"
    )
    @patch(
        "operations.sync_documents_to_kernel_operations.put_assets_and_pdfs_in_object_store"
    )
    @patch("operations.sync_documents_to_kernel_operations.put_xml_into_object_store")
    @patch("operations.sync_documents_to_kernel_operations.Logger")
    @patch("operations.sync_documents_to_kernel_operations.ZipFile")
    def test_register_update_documents_logs_error_if_put_xml_into_object_store_error(
        self,
        MockZipFile,
        MockLogger,
        mk_put_xml_into_object_store,
        mk_put_assets_and_pdfs_in_object_store,
        mk_register_update_doc_into_kernel,
    ):
        MockZipFile.return_value.__enter__.return_value.read.return_value = b""
        mk_put_xml_into_object_store.side_effect = [
            {},
            PutXMLInObjectStoreException("Put Doc in Object Store Error"),
            {},
        ]
        register_update_documents(**self.kwargs)
        MockLogger.info.assert_any_call(
            'Could not put document "%s" in object store: %s',
            self.kwargs["xmls_to_preserve"][1],
            "Put Doc in Object Store Error",
        )

    @patch(
        "operations.sync_documents_to_kernel_operations.register_update_doc_into_kernel"
    )
    @patch(
        "operations.sync_documents_to_kernel_operations.put_assets_and_pdfs_in_object_store"
    )
    @patch("operations.sync_documents_to_kernel_operations.put_xml_into_object_store")
    @patch("operations.sync_documents_to_kernel_operations.ZipFile")
    def test_register_update_documents_puts_each_doc_in_object_store(
        self,
        MockZipFile,
        mk_put_xml_into_object_store,
        mk_put_assets_and_pdfs_in_object_store,
        mk_register_update_doc_into_kernel,
    ):
        mk_put_xml_into_object_store.side_effect = self.xmls_data
        register_update_documents(**self.kwargs)
        for xml_data in self.xmls_data:
            with self.subTest(xml_data=xml_data):
                mk_put_assets_and_pdfs_in_object_store.assert_any_call(
                    MockZipFile.return_value.__enter__.return_value, xml_data
                )

    @patch(
        "operations.sync_documents_to_kernel_operations.register_update_doc_into_kernel"
    )
    @patch(
        "operations.sync_documents_to_kernel_operations.put_assets_and_pdfs_in_object_store"
    )
    @patch("operations.sync_documents_to_kernel_operations.put_xml_into_object_store")
    @patch("operations.sync_documents_to_kernel_operations.ZipFile")
    def test_register_update_documents_call_register_update_doc_into_kernel(
        self,
        MockZipFile,
        mk_put_xml_into_object_store,
        mk_put_assets_and_pdfs_in_object_store,
        mk_register_update_doc_into_kernel,
    ):
        expected = copy.deepcopy(self.xmls_data)
        mk_put_xml_into_object_store.side_effect = self.xmls_data
        mk_assets_and_pdfs = {
            "assets": [{"asset_id": "test"}],
            "pdfs": [{"filename": "test"}],
        }
        mk_put_assets_and_pdfs_in_object_store.return_value = mk_assets_and_pdfs
        register_update_documents(**self.kwargs)
        for xml_data in expected:
            xml_data.update(mk_assets_and_pdfs)
            with self.subTest(xml_data=xml_data):
                mk_register_update_doc_into_kernel.assert_any_call(xml_data)

    @patch(
        "operations.sync_documents_to_kernel_operations.register_update_doc_into_kernel"
    )
    @patch(
        "operations.sync_documents_to_kernel_operations.put_assets_and_pdfs_in_object_store"
    )
    @patch("operations.sync_documents_to_kernel_operations.put_xml_into_object_store")
    @patch("operations.sync_documents_to_kernel_operations.Logger")
    @patch("operations.sync_documents_to_kernel_operations.ZipFile")
    def test_register_update_documents_call_register_update_doc_into_kernel_raise_error(
        self,
        MockZipFile,
        MockLogger,
        mk_put_xml_into_object_store,
        mk_put_assets_and_pdfs_in_object_store,
        mk_register_update_doc_into_kernel,
    ):
        mk_put_assets_and_pdfs_in_object_store.side_effect = self.xmls_data
        mk_register_update_doc_into_kernel.side_effect = [
            None,
            RegisterUpdateDocIntoKernelException("Register Doc in Kernel Error"),
            None,
        ]

        register_update_documents(**self.kwargs)
        MockLogger.info.assert_any_call(
            'Could not register or update document "%s" in Kernel: %s',
            self.kwargs["xmls_to_preserve"][1],
            "Register Doc in Kernel Error",
        )

    @patch(
        "operations.sync_documents_to_kernel_operations.register_update_doc_into_kernel"
    )
    @patch(
        "operations.sync_documents_to_kernel_operations.put_assets_and_pdfs_in_object_store"
    )
    @patch("operations.sync_documents_to_kernel_operations.put_xml_into_object_store")
    @patch("operations.sync_documents_to_kernel_operations.ZipFile")
    def test_register_update_documents_returns_syncronized_documents_metadata_list(
        self,
        MockZipFile,
        mk_put_xml_into_object_store,
        mk_put_assets_and_pdfs_in_object_store,
        mk_register_update_doc_into_kernel,
    ):
        expected = [self.xmls_data[0], self.xmls_data[2]]
        mk_put_xml_into_object_store.side_effect = self.xmls_data
        mk_register_update_doc_into_kernel.side_effect = [
            None,
            RegisterUpdateDocIntoKernelException("Register Doc in Kernel Error"),
            None,
        ]

        result = register_update_documents(**self.kwargs)
        self.assertEqual(result, expected)


class TestLinkDocumentToDocumentsbundle(TestCase):
    def setUp(self):
        self.documents = [
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
        self.issn_index_json = json.dumps({
            "0034-8910": "0034-8910",
            "1518-8787": "1518-8787",
        })

    def test_if_link_documents_to_documentsbundle_return_none_when_param_document_empty(self):

        self.assertIsNone(link_documents_to_documentsbundle(
            "path_to_sps_package/package.zip", [], None),
            None
        )

    @patch.object(builtins, "open")
    @patch("operations.sync_documents_to_kernel_operations.Logger")
    @patch("operations.sync_documents_to_kernel_operations.get_or_create_bundle")
    @patch("operations.sync_documents_to_kernel_operations.register_document_to_documentsbundle")
    @patch("operations.sync_documents_to_kernel_operations.get_bundle_id")
    def test_link_documents_to_documentsbundle_logs_journal_issn_id_error(
        self, mk_get_bundle_id,  mk_regdocument, mk_get_or_create_bundle, MockLogger, mk_open
    ):
        mk_open.return_value.__enter__.return_value.read.return_value = '{}'
        link_documents_to_documentsbundle(
            "path_to_sps_package/package.zip", self.documents[:1], "/json/index.json"
        )
        MockLogger.info.assert_any_call(
            'Could not get journal ISSN ID: ISSN id "%s" not found', "0034-8910"
        )

    @patch.object(builtins, "open")
    @patch("operations.sync_documents_to_kernel_operations.get_or_create_bundle")
    @patch("operations.sync_documents_to_kernel_operations.register_document_to_documentsbundle")
    @patch("operations.sync_documents_to_kernel_operations.get_bundle_id")
    def test_link_documents_to_documentsbundle_calls_get_bundle_id_with_issn_id(
        self, mk_get_bundle_id,  mk_regdocument, mk_get_or_create_bundle, mk_open
    ):
        mk_open.return_value.__enter__.return_value.read.return_value = '{"0034-8910": "0101-0101"}'
        link_documents_to_documentsbundle(
            "path_to_sps_package/package.zip", self.documents[:1], "/json/index.json"
        )
        mk_get_bundle_id.assert_called_once_with(
            issn_id="0101-0101",
            year=self.documents[0]["year"],
            volume=self.documents[0].get("volume", None),
            number=self.documents[0].get("number", None),
            supplement=self.documents[0].get("supplement", None)
        )

    @patch("operations.sync_documents_to_kernel_operations.get_or_create_bundle")
    @patch("operations.sync_documents_to_kernel_operations.register_document_to_documentsbundle")
    @patch("operations.sync_documents_to_kernel_operations.get_bundle_id")
    def test_if_link_documents_to_documentsbundle_register_on_document_store(
        self, mk_get_bundle_id,  mk_regdocument, mk_get_or_create_bundle
    ):
        mock_response = Mock(status_code=204)

        mk_regdocument.return_value = mock_response

        mk_get_bundle_id.side_effect = [
                                   '0034-8910-2014-v48-n2',
                                   '0034-8910-2014-v48-n2',
                                   '1518-8787-2014-v2-n2',
                                   '1518-8787-2014-v2-n2-s1'
                                   ]

        with tempfile.TemporaryDirectory() as tmpdirname:
            issn_index_json_path = os.path.join(tmpdirname, "issn_index.json")
            with open(issn_index_json_path, "w") as index_file:
                index_file.write(self.issn_index_json)

            self.assertEqual(
                link_documents_to_documentsbundle(
                    "path_to_sps_package/package.zip",
                    self.documents,
                    issn_index_json_path
                ),
                [
                    {'id': '0034-8910-2014-v48-n2', 'status': 204},
                    {'id': '1518-8787-2014-v2-n2', 'status': 204},
                    {'id': '1518-8787-2014-v2-n2-s1', 'status': 204}
                ])

    @patch("operations.sync_documents_to_kernel_operations.get_or_create_bundle")
    @patch("operations.sync_documents_to_kernel_operations.register_document_to_documentsbundle")
    @patch("operations.sync_documents_to_kernel_operations.get_bundle_id")
    def test_if_some_documents_are_not_register_on_document_store(
        self, mk_get_bundle_id,  mk_regdocument, mk_get_or_create_bundle
    ):
        mk_regdocument.side_effect = [
                                      Mock(status_code=204),
                                      Mock(status_code=422),
                                      Mock(status_code=404)
                                      ]

        mk_get_bundle_id.side_effect = [
                                   '0034-8910-2014-v48-n2',
                                   '0034-8910-2014-v48-n2',
                                   '1518-8787-2014-v2-n2',
                                   '1518-8787-2014-v2-n2-s1'
                                   ]

        with tempfile.TemporaryDirectory() as tmpdirname:
            issn_index_json_path = os.path.join(tmpdirname, "issn_index.json")
            with open(issn_index_json_path, "w") as index_file:
                index_file.write(self.issn_index_json)

            self.assertEqual(
                link_documents_to_documentsbundle(
                    "path_to_sps_package/package.zip",
                    self.documents,
                    issn_index_json_path
                ),
                [
                    {'id': '0034-8910-2014-v48-n2', 'status': 204},
                    {'id': '1518-8787-2014-v2-n2', 'status': 422},
                    {'id': '1518-8787-2014-v2-n2-s1', 'status': 404}
                ])


    @patch(
        "operations.sync_documents_to_kernel_operations.register_document_to_documentsbundle"
    )
    @patch("operations.sync_documents_to_kernel_operations.get_or_create_bundle")
    @patch("operations.sync_documents_to_kernel_operations.get_bundle_id")
    @patch.object(builtins, "open")
    def test_link_documents_to_documentsbundle_should_not_emit_an_update_call_if_the_payload_wont_change(
        self, mk_open, mk_get_bundle_id, mk_get_or_create_bundle, mk_register_document_to_bundle
    ):
        new_document_to_link = self.documents[0]
        current_bundle_item_list = [
            {
                "id": new_document_to_link["scielo_id"],
                "order": new_document_to_link["order"],
            }
        ]

        # journal_issn_map
        mk_open.return_value.__enter__.return_value.read.return_value = (
            '{"0034-8910": "0101-0101"}'
        )

        # Bundle_id traduzido a partir do journal_issn_map
        mk_get_bundle_id.side_effect = ["0034-8910-2014-v48-n2"]
        mk_get_or_create_bundle.return_value.json.return_value.__getitem__.return_value = (
            current_bundle_item_list
        )

        link_documents_to_documentsbundle(
            "path_to_sps_package/package.zip",
            [new_document_to_link],
            "/some/random/json/path.json"
        )

        # Não emita uma atualização do bundle se a nova lista for idêntica a atual
        mk_register_document_to_bundle.assert_not_called()

    @patch(
        "operations.sync_documents_to_kernel_operations.register_document_to_documentsbundle"
    )
    @patch("operations.sync_documents_to_kernel_operations.get_or_create_bundle")
    @patch("operations.sync_documents_to_kernel_operations.get_bundle_id")
    @patch.object(builtins, "open")
    def test_link_documents_to_documentsbundle_should_not_reset_item_list_when_new_documents_arrives(
        self, mk_open, mk_get_bundle_id, mk_get_or_create_bundle, mk_register_document_to_bundle
    ):
        new_documents_to_link = self.documents[0:2]
        current_bundle_item_list = [
            {
                "id": new_documents_to_link[0]["scielo_id"],
                "order": new_documents_to_link[0]["order"],
            }
        ]

        # journal_issn_map
        mk_open.return_value.__enter__.return_value.read.return_value = (
            '{"0034-8910": "0101-0101"}'
        )

        # Bundle_id traduzido a partir do journal_issn_map
        mk_get_bundle_id.return_value = "0034-8910-2014-v48-n2"
        mk_get_or_create_bundle.return_value.json.return_value.__getitem__.return_value = (
            current_bundle_item_list
        )

        link_documents_to_documentsbundle(
            "path_to_sps_package/package.zip",
            new_documents_to_link,
            "/some/random/json/path.json"
        )

        # Lista produzida a partir dos documentos existes e dos novos
        # documentos
        new_payload_list = current_bundle_item_list + [
            {
                "id": new_documents_to_link[1]["scielo_id"],
                "order": new_documents_to_link[1]["order"],
            }
        ]

        mk_register_document_to_bundle.assert_called_with(
            "0034-8910-2014-v48-n2", new_payload_list
        )


@patch("operations.sync_documents_to_kernel_operations.Logger")
@patch(
    "operations.sync_documents_to_kernel_operations.register_document_to_documentsbundle"
)
@patch("operations.sync_documents_to_kernel_operations.get_bundle_id")
@patch("operations.sync_documents_to_kernel_operations.get_or_create_bundle")
class TestLinkDocumentToDocumentsbundleAOPs(TestCase):
    def setUp(self):
        self.documents = [
            {
                "scielo_id": "S0034-8910.2014048004923",
                "issn": "0034-8910",
                "order": "347",
             },
            {
                "scielo_id": "S0034-8910.2014048004924",
                "issn": "0034-8910",
                "order": "348",
             },
            {
                "scielo_id": "S0034-8910.20140078954641",
                "issn": "1518-8787",
                "order": "978",
             },
            {
                "scielo_id": "S0034-8910.20140078954641",
                "issn": "1518-8787",
                "order": "979",
             }
        ]
        self.issn_index_json = json.dumps({
            "0034-8910": "0034-8910",
            "1518-8787": "1518-8787",
        })

    @patch.object(builtins, "open")
    def test_calls_get_or_create_bundle(
        self,
        mk_open,
        mk_get_or_create_bundle,
        mk_get_bundle_id,
        mk_regdocument,
        MockLogger
    ):
        mk_open.return_value.__enter__.return_value.read.return_value = (
            '{"0034-8910": "0101-0101"}'
        )
        mk_get_bundle_id.return_value = "0101-0101-aop"
        mk_get_or_create_bundle.return_value = MagicMock()
        link_documents_to_documentsbundle(
            "path_to_sps_package/2019nahead.zip", self.documents[:1], "/json/index.json"
        )
        mk_get_or_create_bundle.assert_called_with("0101-0101-aop", is_aop=True)

    @patch.object(builtins, "open")
    def test_get_or_create_bundle_raises_exception(
        self,
        mk_open,
        mk_get_or_create_bundle,
        mk_get_bundle_id,
        mk_regdocument,
        MockLogger
    ):
        def raise_exception(*args, **kwargs):
            exc = LinkDocumentToDocumentsBundleException("Bundle not found")
            exc.response = Mock(status_code=404)
            raise exc

        mk_open.return_value.__enter__.return_value.read.return_value = (
            '{"0034-8910": "0101-0101"}'
        )
        mk_get_bundle_id.return_value = "0101-0101-aop"
        mk_get_or_create_bundle.side_effect = raise_exception
        result = link_documents_to_documentsbundle(
            "path_to_sps_package/2019nahead.zip", self.documents[:1], "/json/index.json"
        )
        self.assertEqual(result, [{'id': '0101-0101-aop', 'status': 404}])
        MockLogger.info.assert_called_with(
            "Could not get bundle %: Bundle not found", "0101-0101-aop"
        )


if __name__ == "__main__":
    main()
