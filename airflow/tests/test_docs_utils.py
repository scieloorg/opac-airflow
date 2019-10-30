import copy
import random
from unittest import TestCase, main
from unittest.mock import patch, Mock, MagicMock

import requests
from airflow import DAG
from lxml import etree

from dags.operations.docs_utils import (
    delete_doc_from_kernel,
    document_to_delete,
    get_xml_data,
    files_sha1,
    register_update_doc_into_kernel,
    put_object_in_object_store,
    put_assets_and_pdfs_in_object_store,
    put_xml_into_object_store,
    register_document_to_documentsbundle,
)
from dags.operations.exceptions import (
    DeleteDocFromKernelException,
    DocumentToDeleteException,
    PutXMLInObjectStoreException,
    ObjectStoreError,
    RegisterUpdateDocIntoKernelException,
    LinkDocumentToDocumentsBundleException,
)

from tests.fixtures import XML_FILE_CONTENT


class TestDeleteDocFromKernel(TestCase):
    @patch("dags.operations.docs_utils.hooks")
    def test_delete_doc_from_kernel_calls_kernel_connect(self, mk_hooks):
        delete_doc_from_kernel("FX6F3cbyYmmwvtGmMB7WCgr")
        mk_hooks.kernel_connect.assert_called_once_with(
            "/documents/FX6F3cbyYmmwvtGmMB7WCgr", "DELETE"
        )

    @patch("dags.operations.docs_utils.hooks")
    def test_delete_documents_raises_error_if_kernel_connect_error(self, mk_hooks):
        mk_hooks.kernel_connect.side_effect = requests.exceptions.HTTPError("Not Found")
        with self.assertRaises(DeleteDocFromKernelException) as exc_info:
            delete_doc_from_kernel("FX6F3cbyYmmwvtGmMB7WCgr")
        self.assertEqual(str(exc_info.exception), "Not Found")


class TestDocumentsToDelete(TestCase):
    @patch("dags.operations.docs_utils.SPS_Package")
    @patch("dags.operations.docs_utils.etree")
    def test_document_to_delete_reads_xml_from_zip(self, mk_etree, MockSPS_Package):
        MockSPS_Package.return_value.is_document_deletion = False
        MockZipFile = MagicMock()
        document_to_delete(MockZipFile, "1806-907X-rba-53-01-1-8.xml")
        MockZipFile.read.assert_any_call("1806-907X-rba-53-01-1-8.xml")

    @patch("dags.operations.docs_utils.SPS_Package")
    @patch("dags.operations.docs_utils.etree.XML")
    def test_document_to_delete_raises_error_if_read_from_zip_error(
        self, MockXML, MockSPS_Package
    ):
        MockZipFile = MagicMock()
        MockZipFile.read.side_effect = KeyError("File not found in the archive")
        with self.assertRaises(DocumentToDeleteException) as exc_info:
            document_to_delete(MockZipFile, "1806-907X-rba-53-01-1-8.xml")
        self.assertEqual(str(exc_info.exception), "'File not found in the archive'")

    @patch("dags.operations.docs_utils.SPS_Package")
    @patch("dags.operations.docs_utils.etree")
    def test_document_to_delete_creates_etree_parser(self, mk_etree, MockSPS_Package):
        MockSPS_Package.return_value.is_document_deletion = False
        MockZipFile = MagicMock()
        MockZipFile.read.return_value = XML_FILE_CONTENT
        document_to_delete(MockZipFile, "1806-907X-rba-53-01-1-8.xml")
        mk_etree.XMLParser.assert_called_once_with(
            remove_blank_text=True, no_network=True
        )

    @patch("dags.operations.docs_utils.SPS_Package")
    @patch("dags.operations.docs_utils.etree")
    def test_document_to_delete_creates_etree_xml(self, mk_etree, MockSPS_Package):
        MockParser = Mock()
        mk_etree.XMLParser.return_value = MockParser
        MockSPS_Package.return_value.is_document_deletion = False
        MockZipFile = MagicMock()
        MockZipFile.read.return_value = XML_FILE_CONTENT
        document_to_delete(MockZipFile, "1806-907X-rba-53-01-1-8.xml")
        mk_etree.XML.assert_called_once_with(XML_FILE_CONTENT, MockParser)

    @patch("dags.operations.docs_utils.SPS_Package")
    @patch("dags.operations.docs_utils.etree")
    def test_document_to_delete_creates_SPS_Package_instance(
        self, mk_etree, MockSPS_Package
    ):
        MockXML = Mock()
        mk_etree.XML.return_value = MockXML
        MockSPS_Package.return_value.is_document_deletion = False
        MockZipFile = MagicMock()
        document_to_delete(MockZipFile, "1806-907X-rba-53-01-1-8.xml")
        MockSPS_Package.assert_called_once_with(MockXML, "1806-907X-rba-53-01-1-8.xml")

    @patch("dags.operations.docs_utils.SPS_Package")
    @patch("dags.operations.docs_utils.etree.XML")
    @patch("dags.operations.docs_utils.Logger")
    def test_documents_to_delete_raises_error_if_SPS_Package_error(
        self, MockLogger, MockXML, MockSPS_Package
    ):
        MockSPS_Package.side_effect = TypeError("XML error")
        MockZipFile = MagicMock()
        with self.assertRaises(DocumentToDeleteException) as exc_info:
            document_to_delete(MockZipFile, "1806-907X-rba-53-01-1-8.xml")
        self.assertEqual(str(exc_info.exception), "XML error")

    def test_documents_to_delete_returns_none_if_xml_is_not_to_delete(self):
        MockZipFile = MagicMock()
        MockZipFile.read.return_value = XML_FILE_CONTENT
        result = document_to_delete(MockZipFile, "1806-907X-rba-53-01-1-8.xml")
        self.assertIsNone(result)

    def test_documents_to_delete_raises_error_if_no_scielo_id_in_xml(self):
        article_id = etree.Element("article-id")
        article_id.set("specific-use", "delete")
        xml_file = etree.XML(XML_FILE_CONTENT)
        am_tag = xml_file.find(".//article-meta")
        am_tag.append(article_id)
        scielo_id_tag = xml_file.find(".//article-id[@specific-use='scielo']")
        am_tag.remove(scielo_id_tag)
        deleted_xml_file = etree.tostring(xml_file)
        MockZipFile = MagicMock()
        MockZipFile.read.return_value = deleted_xml_file
        with self.assertRaises(DocumentToDeleteException) as exc_info:
            document_to_delete(MockZipFile, "1806-907X-rba-53-01-1-8.xml")
        self.assertEqual(str(exc_info.exception), "Missing element in XML")

    def test_documents_to_delete_returns_documents_id_to_delete_and_xmls_to_delete(
        self
    ):
        article_id = etree.Element("article-id")
        article_id.set("specific-use", "delete")
        xml_file = etree.XML(XML_FILE_CONTENT)
        am_tag = xml_file.find(".//article-meta")
        am_tag.append(article_id)
        deleted_xml_file = etree.tostring(xml_file)
        MockZipFile = MagicMock()
        MockZipFile.read.return_value = deleted_xml_file
        result = document_to_delete(MockZipFile, "1806-907X-rba-53-01-1-8.xml")
        self.assertEqual(
            result, "FX6F3cbyYmmwvtGmMB7WCgr"
        )  # SciELO ID de XML_FILE_CONTENT


class TestGetXMLData(TestCase):
    @patch("dags.operations.docs_utils.SPS_Package")
    @patch("dags.operations.docs_utils.etree")
    def test_get_xml_data_creates_etree_parser(self, mk_etree, MockSPS_Package):
        get_xml_data(XML_FILE_CONTENT, None)
        mk_etree.XMLParser.assert_called_once_with(
            remove_blank_text=True, no_network=True
        )

    @patch("dags.operations.docs_utils.SPS_Package")
    @patch("dags.operations.docs_utils.etree")
    def test_get_xml_data_creates_etree_xml(self, mk_etree, MockSPS_Package):
        xml_content = XML_FILE_CONTENT
        MockParser = Mock()
        mk_etree.XMLParser.return_value = MockParser
        get_xml_data(xml_content, None)
        mk_etree.XML.assert_called_once_with(xml_content, MockParser)

    @patch("dags.operations.docs_utils.SPS_Package")
    @patch("dags.operations.docs_utils.etree")
    def test_get_xml_data_creates_SPS_Package_instance(self, mk_etree, MockSPS_Package):
        xml_content = XML_FILE_CONTENT
        MockXML = Mock()
        mk_etree.XML.return_value = MockXML
        get_xml_data(xml_content, None)
        MockSPS_Package.assert_called_once_with(MockXML, None)

    def test_get_xml_data_does_not_return_volume_when_it_is_not_in_xml_content(self):
        xml_file = etree.XML(XML_FILE_CONTENT)
        volume_tag = xml_file.find(".//article-meta/volume")
        volume_tag.getparent().remove(volume_tag)
        result = get_xml_data(etree.tostring(xml_file), "1806-907X-rba-53-01-1-8")
        self.assertNotIn("volume", result.keys())

    def test_get_xml_data_does_not_return_number_when_it_is_not_in_xml_content(self):
        xml_file = etree.XML(XML_FILE_CONTENT)
        number_tag = xml_file.find(".//article-meta/issue")
        number_tag.text = "suppl 2"
        result = get_xml_data(etree.tostring(xml_file), "1806-907X-rba-53-01-1-8")
        self.assertNotIn("number", result.keys())

        number_tag.getparent().remove(number_tag)
        result = get_xml_data(etree.tostring(xml_file), "1806-907X-rba-53-01-1-8")
        self.assertNotIn("number", result.keys())

    def test_get_xml_data_returns_supplement_when_it_is_in_xml_content(self):
        xml_content = XML_FILE_CONTENT
        result = get_xml_data(xml_content, "1806-907X-rba-53-01-1-8")
        self.assertIsNone(result.get("supplement"))

        xml_file = etree.XML(XML_FILE_CONTENT)
        issue_tag = xml_file.find(".//article-meta/issue")
        issue_tag.text = "suppl 2"
        result = get_xml_data(etree.tostring(xml_file), "1806-907X-rba-53-01-1-8")
        self.assertEqual(result.get("supplement"), "02")

    def test_get_xml_data_returns_order_with_fpage_when_there_is_no_order_in_xml_content(self):
        xml_content = XML_FILE_CONTENT
        result = get_xml_data(xml_content, "1806-907X-rba-53-01-1-8")
        self.assertEqual(result.get("order"), "00001")

    def test_get_xml_data_returns_order_with_order_when_there_is_order_in_xml_content(self):
        article_id = etree.Element("article-id")
        article_id.set("pub-id-type", "other")
        article_id.text = "00200"
        xml_file = etree.XML(XML_FILE_CONTENT)
        am_tag = xml_file.find(".//article-meta")
        am_tag.append(article_id)
        result = get_xml_data(etree.tostring(xml_file), "1806-907X-rba-53-01-1-8")
        self.assertEqual(result.get("order"), "00200")

    def test_get_xml_data_returns_xml_metadata(self):
        xml_content = XML_FILE_CONTENT
        result = get_xml_data(xml_content, "1806-907X-rba-53-01-1-8")
        self.assertEqual(result["xml_package_name"], "1806-907X-rba-53-01-1-8")
        self.assertEqual(result["scielo_id"], "FX6F3cbyYmmwvtGmMB7WCgr")
        self.assertEqual(result["issn"], "1806-907X")
        self.assertEqual(result["year"], "2018")
        self.assertEqual(result["volume"], "53")
        self.assertEqual(result["number"], "01")
        self.assertEqual(
            result["assets"],
            [
                {"asset_id": "1806-907X-rba-53-01-1-8-g01.jpg"},
                {"asset_id": "1806-907X-rba-53-01-1-8-g02.jpg"},
            ],
        )
        self.assertEqual(
            result["pdfs"],
            [
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
            ],
        )

    @patch("dags.operations.docs_utils.SPS_Package")
    @patch("dags.operations.docs_utils.etree.XML")
    def test_get_xml_data_raise_except_error(self, MockXML, MockSPS_Package):
        xml_content = XML_FILE_CONTENT

        MockSPS_Package.side_effect = TypeError()
        self.assertRaises(PutXMLInObjectStoreException, get_xml_data, xml_content, None)


class TestRegisterUpdateDocIntoKernel(TestCase):
    """
    Payload do documento
    {
        "data": "http://minio/document-store/filename.xml",
        "assets": [
            {
                "asset_id": "image.jpg",
                "asset_url": "http://minio/document-store/image.jpg",
            }
        ]
    }
    """

    def setUp(self):
        self.xml_data = {
            "xml_package_name": "1806-907X-rba-53-01-1-8",
            "journal": "1806-907X",
            "scielo_id": "FX6F3cbyYmmwvtGmMB7WCgr",
            "xml_url": "http://minio/documentstore/1806-907X-rba-53-01-1-8.xml",
            "assets": [
                {
                    "asset_id": "1806-907X-rba-53-01-1-8-g01.jpg",
                    "asset_url": "http://minio/documentstore/1806-907X-rba-53-01-1-8-g01.jpg",
                },
                {
                    "asset_id": "1806-907X-rba-53-01-1-8-g02.jpg",
                    "asset_url": "http://minio/documentstore/1806-907X-rba-53-01-1-8-g02.jpg",
                },
            ],
            "pdfs": [
                {
                    "lang": "en",
                    "filename": "1806-907X-rba-53-01-1-8.pdf",
                    "mimetype": "application/pdf",
                    "data_url": "http://minio/documentstore/1806-907X-rba-53-01-1-8.pdf",
                    "size_bytes": 80000,
                },
                {
                    "lang": "pt",
                    "filename": "1806-907X-rba-53-01-1-8-pt.pdf",
                    "mimetype": "application/pdf",
                    "data_url": "http://minio/documentstore/1806-907X-rba-53-01-1-8-pt.pdf",
                    "size_bytes": 90000,
                },
                {
                    "lang": "de",
                    "filename": "1806-907X-rba-53-01-1-8-de.pdf",
                    "mimetype": "application/pdf",
                    "data_url": "http://minio/documentstore/1806-907X-rba-53-01-1-8-de.pdf",
                    "size_bytes": 100000,
                },
            ],
        }

    @patch("dags.operations.docs_utils.hooks")
    def test_register_update_doc_into_kernel_put_to_kernel_doc(self, mk_hooks):

        payload = {"data": self.xml_data["xml_url"], "assets": self.xml_data["assets"]}
        register_update_doc_into_kernel(self.xml_data)
        mk_hooks.kernel_connect.assert_any_call(
            "/documents/FX6F3cbyYmmwvtGmMB7WCgr", "PUT", payload
        )

    @patch("dags.operations.docs_utils.hooks")
    def test_register_update_doc_into_kernel_put_to_kernel_pdfs(self, mk_hooks):

        register_update_doc_into_kernel(self.xml_data)
        for pdf_payload in self.xml_data["pdfs"]:
            with self.subTest(pdf_payload=pdf_payload):
                mk_hooks.kernel_connect.assert_any_call(
                    "/documents/FX6F3cbyYmmwvtGmMB7WCgr/renditions",
                    "PATCH",
                    pdf_payload,
                )

    @patch("dags.operations.docs_utils.hooks")
    def test_register_update_doc_into_kernel_put_to_kernel_doc_hook_HttpError(
        self, mk_hooks
    ):

        payload = {"data": self.xml_data["xml_url"], "assets": self.xml_data["assets"]}
        mk_hooks.kernel_connect.side_effect = requests.exceptions.HTTPError(
            "404 Client Error: Not Found"
        )

        self.assertRaises(
            RegisterUpdateDocIntoKernelException,
            register_update_doc_into_kernel,
            self.xml_data,
        )

    @patch("dags.operations.docs_utils.Logger")
    @patch("dags.operations.docs_utils.hooks")
    def test_register_update_doc_into_kernel_put_to_kernel_pdfs_hook_HttpError(
        self, mk_hooks, MockLogger
    ):

        payload = {"data": self.xml_data["xml_url"], "assets": self.xml_data["assets"]}
        mk_hooks.kernel_connect.side_effect = [
            None,
            None,
            requests.exceptions.HTTPError("404 Client Error: Not Found"),
        ]

        with self.assertRaises(RegisterUpdateDocIntoKernelException):
            register_update_doc_into_kernel(self.xml_data)

        MockLogger.info.assert_any_call(
            'Putting Rendition "%s" to Kernel', "1806-907X-rba-53-01-1-8.pdf"
        )
        mk_hooks.kernel_connect.assert_any_call(
            "/documents/FX6F3cbyYmmwvtGmMB7WCgr/renditions",
            "PATCH",
            self.xml_data["pdfs"][0],
        )


class TestPutAssetsAndPdfsInObjectStore(TestCase):
    def setUp(self):
        self.xml_data = {
            "issn": "1806-907X",
            "scielo_id": "FX6F3cbyYmmwvtGmMB7WCgr",
            "assets": [
                {"asset_id": "1806-907X-rba-53-01-1-8-g01.jpg"},
                {"asset_id": "1806-907X-rba-53-01-1-8-g02.jpg"},
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
            ],
        }

    @patch("dags.operations.docs_utils.put_object_in_object_store")
    def test_put_assets_and_pdfs_in_object_store_reads_each_asset_from_xml(
        self, mk_put_object_in_object_store
    ):
        MockZipFile = MagicMock()
        put_assets_and_pdfs_in_object_store(MockZipFile, self.xml_data)
        for asset in self.xml_data["assets"]:
            with self.subTest(asset=asset):
                MockZipFile.read.assert_any_call(asset["asset_id"])
                mk_put_object_in_object_store.assert_any_call(
                    MockZipFile.read.return_value,
                    self.xml_data["issn"],
                    self.xml_data["scielo_id"],
                    asset["asset_id"],
                )

    @patch("dags.operations.docs_utils.put_object_in_object_store")
    def test_put_assets_and_pdfs_in_object_store_reads_each_pdf_from_xml(
        self, mk_put_object_in_object_store
    ):
        MockZipFile = MagicMock()
        MockZipFile.read.return_value = b""
        put_assets_and_pdfs_in_object_store(MockZipFile, self.xml_data)

        for pdf in self.xml_data["pdfs"]:
            with self.subTest(pdf=pdf):
                MockZipFile.read.assert_any_call(pdf["filename"])
                mk_put_object_in_object_store.assert_any_call(
                    MockZipFile.read.return_value,
                    self.xml_data["issn"],
                    self.xml_data["scielo_id"],
                    pdf["filename"],
                )

    @patch("dags.operations.docs_utils.Logger")
    @patch("dags.operations.docs_utils.put_object_in_object_store")
    def test_put_assets_and_pdfs_in_object_store_logs_error_if_file_not_found_in_zip(
        self, mk_put_object_in_object_store, MockLogger
    ):
        MockZipFile = MagicMock()
        MockZipFile.read.side_effect = [
            b"",
            KeyError("File not found in the archive"),
            KeyError("File not found in the archive"),
            b"",
        ]
        put_assets_and_pdfs_in_object_store(MockZipFile, self.xml_data)
        MockLogger.info.assert_any_call(
            'Could not read asset "%s" from zipfile "%s": %s',
            self.xml_data["assets"][1]["asset_id"],
            MockZipFile,
            "'File not found in the archive'",
        )
        MockLogger.info.assert_any_call(
            'Could not read PDF "%s" from zipfile "%s": %s',
            self.xml_data["pdfs"][0]["filename"],
            MockZipFile,
            "'File not found in the archive'",
        )

    @patch("dags.operations.docs_utils.Logger")
    @patch("dags.operations.docs_utils.put_object_in_object_store")
    def test_put_assets_and_pdfs_in_object_store_returns_only_read_assets_and_pdfs(
        self, mk_put_object_in_object_store, MockLogger
    ):
        MockZipFile = MagicMock()
        MockZipFile.read.side_effect = [
            b"",
            KeyError("File not found in the archive"),
            KeyError("File not found in the archive"),
            b"",
        ]
        expected = {
            "assets": self.xml_data["assets"][:1],
            "pdfs": self.xml_data["pdfs"][1:],
        }
        mk_minio_result = [
            "http://minio/documentstore/{}".format(expected["assets"][0]["asset_id"]),
            "http://minio/documentstore/{}".format(expected["pdfs"][0]["filename"]),
        ]
        mk_put_object_in_object_store.side_effect = mk_minio_result
        expected["assets"][0]["asset_url"] = mk_minio_result[0]
        expected["pdfs"][0]["data_url"] = mk_minio_result[1]
        expected["pdfs"][0]["size_bytes"] = 0

        result = put_assets_and_pdfs_in_object_store(MockZipFile, self.xml_data)
        self.assertEqual(result, expected)

    @patch("dags.operations.docs_utils.put_object_in_object_store")
    def test_put_assets_and_pdfs_in_object_store_return_data_asset(
        self, mk_put_object_in_object_store
    ):
        expected = copy.deepcopy(self.xml_data)
        for asset in expected["assets"]:
            asset["asset_url"] = "http://minio/documentstore/{}".format(
                asset["asset_id"]
            )
        MockZipFile = MagicMock()
        MockZipFile.read.return_value = b""
        mk_put_object_in_object_store.side_effect = [
            asset["asset_url"] for asset in expected["assets"]
        ] + [None, None, None]

        result = put_assets_and_pdfs_in_object_store(MockZipFile, self.xml_data)
        for expected_asset, result_asset in zip(expected["assets"], result["assets"]):

            self.assertEqual(expected_asset["asset_id"], result_asset["asset_id"])
            self.assertEqual(expected_asset["asset_url"], result_asset["asset_url"])

    @patch("dags.operations.docs_utils.put_object_in_object_store")
    def test_put_assets_and_pdfs_in_object_store_return_data_pdf(
        self, mk_put_object_in_object_store
    ):
        expected = copy.deepcopy(self.xml_data)
        pdfs_size = []
        for pdf in expected["pdfs"]:
            pdf["data_url"] = "http://minio/documentstore/{}".format(pdf["filename"])
            pdf["size_bytes"] = random.randint(80000, 100000)
            pdfs_size.append(pdf["size_bytes"])

        mk_read_file = MagicMock(return_value=b"")
        mk_read_file.__len__.side_effect = pdfs_size
        MockZipFile = Mock()
        MockZipFile.read.return_value = mk_read_file
        mk_put_object_in_object_store.side_effect = (
            [None, None] + [pdf["data_url"] for pdf in expected["pdfs"]] + [None]
        )

        result = put_assets_and_pdfs_in_object_store(MockZipFile, self.xml_data)
        for expected_pdf, result_pdf in zip(expected["pdfs"], result["pdfs"]):

            self.assertEqual(expected_pdf["filename"], result_pdf["filename"])
            self.assertEqual(expected_pdf["data_url"], result_pdf["data_url"])
            self.assertEqual(expected_pdf["size_bytes"], result_pdf["size_bytes"])


class TestPutObjectInObjectStore(TestCase):
    @patch("dags.operations.docs_utils.files_sha1")
    @patch("dags.operations.docs_utils.hooks")
    def test_put_object_in_object_store_call_files_sha1(self, mk_hooks, mk_files_sha1):

        MockFile = Mock()
        put_object_in_object_store(
            MockFile,
            "1806-907X",
            "FX6F3cbyYmmwvtGmMB7WCgr",
            "1806-907X-rba-53-01-1-8.xml",
        )
        mk_files_sha1.assert_called_once_with(MockFile)

    @patch("dags.operations.docs_utils.files_sha1")
    @patch("dags.operations.docs_utils.hooks")
    def test_put_object_in_object_store_call_hook(self, mk_hooks, mk_files_sha1):

        mk_files_sha1.return_value = "da39a3ee5e6b4b0d3255bfef95601890afd80709"
        MockFile = Mock()
        put_object_in_object_store(
            MockFile,
            "1806-907X",
            "FX6F3cbyYmmwvtGmMB7WCgr",
            "1806-907X-rba-53-01-1-8.xml",
        )
        mk_hooks.object_store_connect.assert_called_once_with(
            MockFile,
            "1806-907X/FX6F3cbyYmmwvtGmMB7WCgr/da39a3ee5e6b4b0d3255bfef95601890afd80709.xml",
            "documentstore",
        )

    @patch("dags.operations.docs_utils.files_sha1")
    @patch("dags.operations.docs_utils.hooks")
    def test_put_object_in_object_store_return_url_object(
        self, mk_hooks, mk_files_sha1
    ):

        MockFile = Mock()
        mk_hooks.object_store_connect.return_value = (
            "http://minio/documentstore/1806-907X-rba-53-01-1-8.xml"
        )

        result = put_object_in_object_store(
            MockFile,
            "1806-907X",
            "FX6F3cbyYmmwvtGmMB7WCgr",
            "1806-907X-rba-53-01-1-8.xml",
        )
        self.assertEqual(
            "http://minio/documentstore/1806-907X-rba-53-01-1-8.xml", result
        )

    @patch("dags.operations.docs_utils.files_sha1")
    @patch("dags.operations.docs_utils.hooks")
    def test_put_object_in_object_store_raise_exception_error(
        self, mk_hooks, mk_files_sha1
    ):

        mk_files_sha1.return_value = "da39a3ee5e6b4b0d3255bfef95601890afd80709"
        MockFile = Mock()
        filepath = "{}/{}/{}".format(
            "1806-907X",
            "FX6F3cbyYmmwvtGmMB7WCgr",
            "da39a3ee5e6b4b0d3255bfef95601890afd80709.xml",
        )
        mk_hooks.object_store_connect.side_effect = Exception("ConnectionError")
        with self.assertRaises(ObjectStoreError) as exc_info:
            put_object_in_object_store(
                MockFile,
                "1806-907X",
                "FX6F3cbyYmmwvtGmMB7WCgr",
                "1806-907X-rba-53-01-1-8.xml",
            )
        self.assertEqual(
            str(exc_info.exception),
            'Could not put object "{}" in object store : ConnectionError'.format(
                filepath, str(exc_info)
            ),
        )


class TestPutXMLIntoObjectStore(TestCase):
    def setUp(self):
        self.xml_data = {
            "issn": "1806-907X",
            "scielo_id": "FX6F3cbyYmmwvtGmMB7WCgr",
            "assets": [
                {"asset_id": "1806-907X-rba-53-01-1-8-g01.jpg"},
                {"asset_id": "1806-907X-rba-53-01-1-8-g02.jpg"},
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
            ],
        }

    @patch("dags.operations.docs_utils.put_object_in_object_store")
    @patch("dags.operations.docs_utils.get_xml_data")
    def test_put_xml_into_object_reads_xml_from_zip(
        self, mk_get_xml_data, mk_put_object_in_object_store
    ):
        MockZipFile = Mock()
        put_xml_into_object_store(MockZipFile, "1806-907X-rba-53-01-1-8.xml")
        MockZipFile.read.assert_any_call("1806-907X-rba-53-01-1-8.xml")

    @patch("dags.operations.docs_utils.put_object_in_object_store")
    @patch("dags.operations.docs_utils.get_xml_data")
    def test_put_xml_into_object_store_calls_get_xml_data(
        self, mk_get_xml_data, mk_put_object_in_object_store
    ):
        MockZipFile = Mock()
        MockZipFile.read.return_value = b"1806-907X-rba-53-01-1-8.xml"
        put_xml_into_object_store(MockZipFile, "1806-907X-rba-53-01-1-8.xml")
        mk_get_xml_data.assert_any_call(
            b"1806-907X-rba-53-01-1-8.xml", "1806-907X-rba-53-01-1-8"
        )

    @patch("dags.operations.docs_utils.put_object_in_object_store")
    @patch("dags.operations.docs_utils.get_xml_data")
    def test_put_xml_into_object_store_error_if_zip_read_error(
        self, mk_get_xml_data, mk_put_object_in_object_store
    ):
        MockZipFile = MagicMock()
        MockZipFile.__str__.return_value = "MockZipFile"
        MockZipFile.read.side_effect = KeyError("File not found in the archive")
        with self.assertRaises(PutXMLInObjectStoreException) as exc_info:
            put_xml_into_object_store(MockZipFile, "1806-907X-rba-53-01-1-8.xml")
        self.assertEqual(
            str(exc_info.exception),
            'Could not read file "1806-907X-rba-53-01-1-8.xml" from zipfile "MockZipFile": '
            "'File not found in the archive'",
        )

    @patch("dags.operations.docs_utils.put_object_in_object_store")
    @patch("dags.operations.docs_utils.get_xml_data")
    def test_put_xml_into_object_store_puts_xml_in_object_store(
        self, mk_get_xml_data, mk_put_object_in_object_store
    ):
        MockZipFile = Mock()
        MockZipFile.read.return_value = b""
        mk_get_xml_data.return_value = self.xml_data
        put_xml_into_object_store(MockZipFile, "1806-907X-rba-53-01-1-8.xml")
        mk_put_object_in_object_store.assert_any_call(
            MockZipFile.read.return_value,
            self.xml_data["issn"],
            self.xml_data["scielo_id"],
            "1806-907X-rba-53-01-1-8.xml",
        )

    @patch("dags.operations.docs_utils.put_object_in_object_store")
    @patch("dags.operations.docs_utils.get_xml_data")
    def test_put_xml_into_object_store_return_data_xml(
        self, mk_get_xml_data, mk_put_object_in_object_store
    ):
        MockZipFile = Mock()
        MockZipFile.read.return_value = b""
        mk_get_xml_data.return_value = self.xml_data
        mk_put_object_in_object_store.return_value = (
            "http://minio/documentstore/1806-907X-rba-53-01-1-8.xml"
        )

        result = put_xml_into_object_store(MockZipFile, "1806-907X-rba-53-01-1-8.xml")
        self.assertEqual(
            "http://minio/documentstore/1806-907X-rba-53-01-1-8.xml", result["xml_url"]
        )


class TestFilesSha1(TestCase):
    def test_files_sha1_return_value(self):

        file = b"1806-907X-rba-53-01-1-8.xml"
        self.assertEqual(
                         "3a4dae699f59a3b89b231845def80efe89a5a15e", files_sha1(file)
                         )


class TestRegisterDocumentsToDocumentsBundle(TestCase):

    def setUp(self):
        self.payload = [
                        {"id": "0034-8910-rsp-48-2-0347", "order": "01"},
                        {"id": "0034-8910-rsp-48-2-0348", "order": "02"}
                       ]

    @patch("dags.operations.docs_utils.hooks")
    def test_register_document_documentsbundle_to_documentsbundle_calls_kernel_connect(self, mk_hooks):
        """
            Verifica se register_document invoca kernel_connect com os parâmetros corretos.
        """

        register_document_to_documentsbundle("0066-782X-1999-v72-n0",
                                             self.payload)

        mk_hooks.kernel_connect.assert_called_once_with(
            "/bundles/0066-782X-1999-v72-n0/documents",
            "PUT",
            [
                {"id": "0034-8910-rsp-48-2-0347", "order": "01"},
                {"id": "0034-8910-rsp-48-2-0348", "order": "02"}
            ]
        )

    @patch("dags.operations.docs_utils.hooks")
    def test_register_document_documentsbundle_raise_error_when_documentsbundle_not_found(self, mk_hooks):
        """
            Verifica se register_document levanda uma exceção quando o conteúdo não foi encontrado.
        """

        mk_hooks.kernel_connect.side_effect = requests.exceptions.HTTPError(
            "Not Found"
        )

        self.assertRaises(LinkDocumentToDocumentsBundleException,
                          register_document_to_documentsbundle,
                          "0066-782X-1999-v72-n0",
                          self.payload)

    @patch("dags.operations.docs_utils.hooks")
    def test_if_register_document_documentsbundle_return_status_code_204_with_correct_params(self, mk_hooks):
        """
            Verifica se ao invocarmos register_document_to_documentsbundle com o ID do bundle e payload corretos o retorno é o esperado.

            Status code 204 significa que os documentos foram atualizado com sucesso.
        """
        mk_hooks.kernel_connect.return_value.status_code = requests.codes.no_content

        payload = [
                    {"id": "0034-8910-rsp-48-2-0347", "order": "01"},
                    {"id": "0034-8910-rsp-48-2-0348", "order": "02"},
                  ]

        response = register_document_to_documentsbundle("0066-782X-1999-v72-n0", payload)

        self.assertEqual(response.status_code, 204)


if __name__ == "__main__":
    main()
