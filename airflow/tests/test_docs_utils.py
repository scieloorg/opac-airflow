import os
import copy
import random
from unittest import TestCase, main, skip
from unittest.mock import patch, Mock, MagicMock

import requests
from airflow import DAG

from operations.docs_utils import (
    get_xml_data,
    read_file_from_zip,
    register_update_doc_into_kernel,
    put_object_in_object_store,
    put_assets_and_pdfs_in_object_store,
    put_xml_into_object_store,
)
from operations.exceptions import (
    PutDocInObjectStoreException,
    RegisterUpdateDocIntoKernelException,
)

from tests.fixtures import XML_FILE_CONTENT


class TestGetXMLData(TestCase):
    @patch("operations.docs_utils.SPS_Package")
    @patch("operations.docs_utils.etree")
    def test_get_xml_data_creates_etree_parser(self, mk_etree, MockSPS_Package):
        get_xml_data(XML_FILE_CONTENT, None)
        mk_etree.XMLParser.assert_called_once_with(
            remove_blank_text=True, no_network=True
        )

    @patch("operations.docs_utils.SPS_Package")
    @patch("operations.docs_utils.etree")
    def test_get_xml_data_creates_etree_xml(self, mk_etree, MockSPS_Package):
        xml_content = XML_FILE_CONTENT
        MockParser = Mock()
        mk_etree.XMLParser.return_value = MockParser
        get_xml_data(xml_content, None)
        mk_etree.XML.assert_called_once_with(xml_content, MockParser)

    @patch("operations.docs_utils.SPS_Package")
    @patch("operations.docs_utils.etree")
    def test_get_xml_data_creates_SPS_Package_instance(self, mk_etree, MockSPS_Package):
        xml_content = XML_FILE_CONTENT
        MockXML = Mock()
        mk_etree.XML.return_value = MockXML
        get_xml_data(xml_content, None)
        MockSPS_Package.assert_called_once_with(MockXML, None)

    def test_get_xml_data_returns_xml_metadata(self):
        xml_content = XML_FILE_CONTENT
        result = get_xml_data(xml_content, "1806-907X-rba-53-01-1-8")
        self.assertEqual(result["xml_package_name"], "1806-907X-rba-53-01-1-8")
        self.assertEqual(result["scielo_id"], "FX6F3cbyYmmwvtGmMB7WCgr")
        self.assertEqual(result["issn"], "1806-907X")
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

    @patch("operations.docs_utils.SPS_Package")
    @patch("operations.docs_utils.etree")
    def test_get_xml_data_raise_except_error(self, mk_etree, MockSPS_Package):
        xml_content = XML_FILE_CONTENT

        MockSPS_Package.side_effect = TypeError()
        self.assertRaises(PutDocInObjectStoreException, get_xml_data, xml_content, None)


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

    @patch("operations.docs_utils.hooks")
    def test_register_update_doc_into_kernel_put_to_kernel_doc(self, mk_hooks):

        payload = {"data": self.xml_data["xml_url"], "assets": self.xml_data["assets"]}
        register_update_doc_into_kernel(self.xml_data)
        mk_hooks.kernel_connect.assert_any_call(
            "/documents/FX6F3cbyYmmwvtGmMB7WCgr", "PUT", payload
        )

    @patch("operations.docs_utils.hooks")
    def test_register_update_doc_into_kernel_put_to_kernel_pdfs(self, mk_hooks):

        register_update_doc_into_kernel(self.xml_data)
        for pdf_payload in self.xml_data["pdfs"]:
            with self.subTest(pdf_payload=pdf_payload):
                mk_hooks.kernel_connect.assert_any_call(
                    "/documents/FX6F3cbyYmmwvtGmMB7WCgr/renditions",
                    "PATCH",
                    pdf_payload,
                )

    @patch("operations.docs_utils.hooks")
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

    @patch("operations.docs_utils.Logger")
    @patch("operations.docs_utils.hooks")
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

    @patch("operations.docs_utils.read_file_from_zip")
    @patch("operations.docs_utils.put_object_in_object_store")
    def test_put_assets_and_pdfs_in_object_store_reads_each_asset_from_xml(
        self, mk_put_object_in_object_store, mk_read_file_from_zip
    ):
        MockZipFile = Mock()
        put_assets_and_pdfs_in_object_store(MockZipFile, self.xml_data)
        for asset in self.xml_data["assets"]:
            with self.subTest(asset=asset):
                mk_read_file_from_zip.assert_any_call(MockZipFile, asset["asset_id"])
                mk_put_object_in_object_store.assert_any_call(
                    mk_read_file_from_zip.return_value,
                    self.xml_data["issn"],
                    self.xml_data["scielo_id"],
                    asset["asset_id"],
                )

    @patch("operations.docs_utils.read_file_from_zip")
    @patch("operations.docs_utils.put_object_in_object_store")
    def test_put_assets_and_pdfs_in_object_store_reads_each_pdf_from_xml(
        self, mk_put_object_in_object_store, mk_read_file_from_zip
    ):
        MockZipFile = Mock()
        mk_read_file_from_zip.return_value = b""
        put_assets_and_pdfs_in_object_store(MockZipFile, self.xml_data)

        for pdf in self.xml_data["pdfs"]:
            with self.subTest(pdf=pdf):
                mk_read_file_from_zip.assert_any_call(MockZipFile, pdf["filename"])
                mk_put_object_in_object_store.assert_any_call(
                    mk_read_file_from_zip.return_value,
                    self.xml_data["issn"],
                    self.xml_data["scielo_id"],
                    pdf["filename"],
                )

    @patch("operations.docs_utils.read_file_from_zip")
    @patch("operations.docs_utils.put_object_in_object_store")
    def test_put_assets_and_pdfs_in_object_store_return_data_asset(
        self, mk_put_object_in_object_store, mk_read_file_from_zip
    ):
        expected = copy.deepcopy(self.xml_data)
        for asset in expected["assets"]:
            asset["asset_url"] = "http://minio/documentstore/{}".format(
                asset["asset_id"]
            )
        mk_read_file_from_zip.return_value = b""
        mk_put_object_in_object_store.side_effect = [
            asset["asset_url"] for asset in expected["assets"]
        ] + [None, None, None]

        result = put_assets_and_pdfs_in_object_store(
            Mock(), self.xml_data
        )
        for expected_asset, result_asset in zip(expected["assets"], result["assets"]):

            self.assertEqual(expected_asset["asset_id"], result_asset["asset_id"])
            self.assertEqual(expected_asset["asset_url"], result_asset["asset_url"])

    @patch("operations.docs_utils.read_file_from_zip")
    @patch("operations.docs_utils.put_object_in_object_store")
    def test_put_assets_and_pdfs_in_object_store_return_data_pdf(
        self, mk_put_object_in_object_store, mk_read_file_from_zip
    ):
        expected = copy.deepcopy(self.xml_data)
        pdfs_size = []
        for pdf in expected["pdfs"]:
            pdf["data_url"] = "http://minio/documentstore/{}".format(pdf["filename"])
            pdf["size_bytes"] = random.randint(80000, 100000)
            pdfs_size.append(pdf["size_bytes"])

        mk_read_file = MagicMock(return_value=b"")
        mk_read_file.__len__.side_effect = pdfs_size
        mk_read_file_from_zip.return_value = mk_read_file
        mk_put_object_in_object_store.side_effect = (
            [None, None] + [pdf["data_url"] for pdf in expected["pdfs"]] + [None]
        )

        result = put_assets_and_pdfs_in_object_store(
            Mock(), self.xml_data
        )
        for expected_pdf, result_pdf in zip(expected["pdfs"], result["pdfs"]):

            self.assertEqual(expected_pdf["filename"], result_pdf["filename"])
            self.assertEqual(expected_pdf["data_url"], result_pdf["data_url"])
            self.assertEqual(expected_pdf["size_bytes"], result_pdf["size_bytes"])


class TestReadFileFromZip(TestCase):
    def test_read_file_from_zip_call_read(self):

        MockZipFile = Mock()
        read_file_from_zip(MockZipFile, "filename.pdf")
        MockZipFile.read.assert_called_once_with("filename.pdf")

    def test_read_file_from_zip_return_value(self):

        MockZipFile = Mock()
        MockZipFile.read.return_value = b"1806-907X-rba-53-01-1-8-g01"
        result = read_file_from_zip(MockZipFile, "filename.pdf")
        self.assertEqual(result, b"1806-907X-rba-53-01-1-8-g01")

    def test_read_file_from_zip_raise_except_error(self):

        MockZipFile = Mock()
        MockZipFile.read.side_effect = KeyError()
        self.assertRaises(
            PutDocInObjectStoreException,
            read_file_from_zip,
            MockZipFile,
            "filename.pdf",
        )


class TestPutObjectInObjectStore(TestCase):
    @patch("operations.docs_utils.hooks")
    def test_put_object_in_object_store_call_hook(self, mk_hooks):

        MockFile = Mock()
        put_object_in_object_store(
            MockFile,
            "1806-907X",
            "FX6F3cbyYmmwvtGmMB7WCgr",
            "1806-907X-rba-53-01-1-8.xml",
        )
        mk_hooks.object_store_connect.assert_called_once_with(
            MockFile,
            "1806-907X/FX6F3cbyYmmwvtGmMB7WCgr/1806-907X-rba-53-01-1-8.xml",
            "documentstore",
        )

    @patch("operations.docs_utils.hooks")
    def test_put_object_in_object_store_return_url_object(self, mk_hooks):

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

    @patch("operations.docs_utils.hooks")
    def test_put_object_in_object_store_raise_exception_error(self, mk_hooks):

        MockFile = Mock()

        mk_hooks.object_store_connect.side_effect = Exception()
        self.assertRaises(
            PutDocInObjectStoreException,
            put_object_in_object_store,
            MockFile,
            "1806-907X",
            "FX6F3cbyYmmwvtGmMB7WCgr",
            "1806-907X-rba-53-01-1-8.xml",
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

    @patch("operations.docs_utils.read_file_from_zip")
    @patch("operations.docs_utils.put_object_in_object_store")
    @patch("operations.docs_utils.get_xml_data")
    def test_put_xml_into_object_store_calls_get_xml_data(
        self, mk_get_xml_data, mk_put_object_in_object_store, mk_read_file_from_zip
    ):
        MockZipFile = Mock()
        mk_read_file_from_zip.return_value = b"1806-907X-rba-53-01-1-8.xml"
        put_xml_into_object_store(MockZipFile, "1806-907X-rba-53-01-1-8.xml")
        mk_get_xml_data.assert_any_call(
            b"1806-907X-rba-53-01-1-8.xml", "1806-907X-rba-53-01-1-8"
        )

    @patch("operations.docs_utils.read_file_from_zip")
    @patch("operations.docs_utils.put_object_in_object_store")
    @patch("operations.docs_utils.get_xml_data")
    def test_put_xml_into_object_store_reads_xml(
        self, mk_get_xml_data, mk_put_object_in_object_store, mk_read_file_from_zip
    ):
        MockZipFile = Mock()
        mk_read_file_from_zip.return_value = b""
        mk_get_xml_data.return_value = self.xml_data
        put_xml_into_object_store(MockZipFile, "1806-907X-rba-53-01-1-8.xml")

        mk_read_file_from_zip.assert_any_call(
            MockZipFile, "1806-907X-rba-53-01-1-8.xml"
        )
        mk_put_object_in_object_store.assert_any_call(
            mk_read_file_from_zip.return_value,
            self.xml_data["issn"],
            self.xml_data["scielo_id"],
            "1806-907X-rba-53-01-1-8.xml",
        )

    @patch("operations.docs_utils.read_file_from_zip")
    @patch("operations.docs_utils.put_object_in_object_store")
    @patch("operations.docs_utils.get_xml_data")
    def test_put_xml_into_object_store_return_data_xml(
        self, mk_get_xml_data, mk_put_object_in_object_store, mk_read_file_from_zip
    ):
        MockZipFile = Mock()
        mk_read_file_from_zip.return_value = b""
        mk_get_xml_data.return_value = self.xml_data
        mk_put_object_in_object_store.return_value = \
            "http://minio/documentstore/1806-907X-rba-53-01-1-8.xml"

        result = put_xml_into_object_store(
            MockZipFile, "1806-907X-rba-53-01-1-8.xml"
        )
        self.assertEqual(
            "http://minio/documentstore/1806-907X-rba-53-01-1-8.xml", result["xml_url"]
        )


if __name__ == "__main__":
    main()
