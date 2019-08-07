import os
import http.client
import shutil
import tempfile
from unittest import TestCase, main
from unittest.mock import patch, Mock, MagicMock

import requests
from airflow import DAG
from lxml import etree

from operations.sync_documents_to_kernel_operations import (
    list_documents,
    documents_to_delete,
    delete_documents,
)


XML_FILE_CONTENT = b"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE article PUBLIC "-//NLM//DTD JATS (Z39.96) Journal Publishing DTD v1.0 20120330//EN" "JATS-journalpublishing1.dtd">
<article article-type="research-article" dtd-version="1.0" specific-use="sps-1.5" xml:lang="en" xmlns:mml="http://www.w3.org/1998/Math/MathML" xmlns:xlink="http://www.w3.org/1999/xlink">
    <article-meta>
        <article-id pub-id-type="publisher-id" specific-use="scielo">FX6F3cbyYmmwvtGmMB7WCgr</article-id>
    </article-meta>
</article>
"""


class TestListDocuments(TestCase):
    def setUp(self):
        self.sps_package = "dir/destination/abc_v50.zip"

    @patch("operations.sync_documents_to_kernel_operations.ZipFile")
    def test_list_document_opens_all_zips(self, MockZipFile):
        list_documents(self.sps_package)
        MockZipFile.assert_called_once_with(self.sps_package)

    @patch("operations.sync_documents_to_kernel_operations.ZipFile")
    def test_list_document_opens_all_zips(self, MockZipFile):
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


class TestDocumentsToDelete(TestCase):
    @patch("operations.sync_documents_to_kernel_operations.ZipFile")
    def test_documents_to_delete_opens_zip(self, MockZipFile):
        documents_to_delete("dir/destination/abc_v50.zip", [])
        MockZipFile.assert_called_once_with("dir/destination/abc_v50.zip")

    @patch("operations.sync_documents_to_kernel_operations.ZipFile")
    def test_documents_to_delete_reads_each_xml_from_zip(self, MockZipFile):
        sps_xml_files = ["0123-4567-abc-50-1-8.xml", "0123-4567-abc-50-9-18.xml"]
        MockZipFile.return_value.__enter__.return_value.read.return_value = b""
        documents_to_delete("dir/destination/abc_v50.zip", sps_xml_files)
        for sps_xml_file in sps_xml_files:
            with self.subTest(sps_xml_file=sps_xml_file):
                for sps_xml_file in sps_xml_files:
                    MockZipFile.return_value.__enter__.return_value.read.assert_any_call(
                        sps_xml_file
                    )

    @patch("operations.sync_documents_to_kernel_operations.ZipFile")
    def test_documents_to_delete_returns_empty_list_if_no_docs_to_delete(
        self, MockZipFile
    ):
        xml_file = etree.XML(XML_FILE_CONTENT)
        xml_file = etree.tostring(xml_file)
        MockZipFile.return_value.__enter__.return_value.read.return_value = xml_file
        result = documents_to_delete(
            "dir/destination/abc_v50.zip", ["1806-907X-rba-53-01-1-8.xml"]
        )
        self.assertEqual(result, ([], []))

    @patch("operations.sync_documents_to_kernel_operations.ZipFile")
    @patch("operations.sync_documents_to_kernel_operations.Logger")
    def test_documents_to_delete_logs_error_if_no_scielo_id_in_xml(
        self, MockLogger, MockZipFile
    ):
        xml_file = etree.XML(XML_FILE_CONTENT)
        scielo_id = xml_file.find(".//article-id[@specific-use='scielo']")
        scielo_id.getparent().remove(scielo_id)
        xml_file = etree.tostring(xml_file)
        MockZipFile.return_value.__enter__.return_value.read.return_value = xml_file
        documents_to_delete(
            "dir/destination/abc_v50.zip", ["1806-907X-rba-53-01-1-8.xml"]
        )
        MockLogger.info.assert_any_call(
            'Cannot read SciELO ID from "1806-907X-rba-53-01-1-8.xml": '
            "missing element in XML"
        )

    @patch("operations.sync_documents_to_kernel_operations.ZipFile")
    def test_documents_to_delete_returns_documents_id_to_delete_and_xmls_to_delete(
        self, MockZipFile
    ):
        article_id = etree.Element("article-id")
        article_id.set("specific-use", "delete")
        xml_file = etree.XML(XML_FILE_CONTENT)
        am_tag = xml_file.find(".//article-meta")
        am_tag.append(article_id)
        deleted_xml_file = etree.tostring(xml_file)
        MockZipFile.return_value.__enter__.return_value.read.return_value = (
            deleted_xml_file
        )
        result = documents_to_delete(
            "dir/destination/abc_v50.zip", ["1806-907X-rba-53-01-1-8.xml"]
        )
        self.assertEqual(
            result, (["1806-907X-rba-53-01-1-8.xml"], ["FX6F3cbyYmmwvtGmMB7WCgr"])
        )  # SciELO ID de XML_FILE_CONTENT


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

    @patch("operations.sync_documents_to_kernel_operations.hooks")
    @patch("operations.sync_documents_to_kernel_operations.documents_to_delete")
    def test_delete_documents_calls_documents_to_delete(
        self, mk_documents_to_delete, mk_hooks
    ):
        mk_documents_to_delete.return_value = ([], [])
        delete_documents(**self.kwargs)
        mk_documents_to_delete.assert_called_once_with(
            self.kwargs["sps_package"], self.kwargs["xmls_filenames"]
        )

    @patch("operations.sync_documents_to_kernel_operations.hooks")
    @patch("operations.sync_documents_to_kernel_operations.Logger")
    @patch("operations.sync_documents_to_kernel_operations.documents_to_delete")
    def test_delete_documents_calls_kernel_connect_with_docs_to_delete(
        self, mk_documents_to_delete, MockLogger, mk_hooks
    ):
        docs_to_delete = (
            [
                "1806-907X-rba-53-01-1-8.xml",
                "1806-907X-rba-53-01-9-18.xml",
                "1806-907X-rba-53-01-19-25.xml",
            ],
            [
                "FX6F3cbyYmmwvtGmMB7WCgr",
                "GZ5K2cbyYmmwvtGmMB71243",
                "KU890cbyYmmwvtGmMB7JUk4",
            ],
        )
        mk_documents_to_delete.return_value = docs_to_delete
        mk_hooks.kernel_connect.return_value = Mock(status_code=http.client.NO_CONTENT)
        delete_documents(**self.kwargs)
        for doc_to_delete in docs_to_delete[1]:
            with self.subTest(doc_to_delete=doc_to_delete):
                mk_hooks.kernel_connect.assert_any_call(
                    "/documents/" + doc_to_delete, "DELETE"
                )
                MockLogger.info.assert_any_call(
                    "Document %s deleted from kernel status: %d"
                    % (doc_to_delete, http.client.NO_CONTENT)
                )

    @patch("operations.sync_documents_to_kernel_operations.hooks")
    @patch("operations.sync_documents_to_kernel_operations.Logger")
    @patch("operations.sync_documents_to_kernel_operations.documents_to_delete")
    def test_delete_documents_logs_error_if_kernel_connect_error(
        self, mk_documents_to_delete, MockLogger, mk_hooks
    ):
        mk_documents_to_delete.return_value = (
            ["1806-907X-rba-53-01-1-8.xml"],
            ["FX6F3cbyYmmwvtGmMB7WCgr"],
        )
        mk_hooks.kernel_connect.side_effect = requests.exceptions.HTTPError(
            "404 Client Error: Not Found"
        )
        delete_documents(**self.kwargs)
        MockLogger.info.assert_any_call(
            'Cannot delete "FX6F3cbyYmmwvtGmMB7WCgr" from kernel status: '
            "404 Client Error: Not Found"
        )

    @patch("operations.sync_documents_to_kernel_operations.hooks")
    @patch("operations.sync_documents_to_kernel_operations.Logger")
    @patch("operations.sync_documents_to_kernel_operations.documents_to_delete")
    def test_delete_documents_returns_xmls_to_preserve(
        self, mk_documents_to_delete, MockLogger, mk_hooks
    ):
        docs_to_delete = (
            ["1806-907X-rba-53-01-1-8.xml", "1806-907X-rba-53-01-9-18.xml"],
            ["FX6F3cbyYmmwvtGmMB7WCgr", "GZ5K2cbyYmmwvtGmMB71243"],
        )
        mk_documents_to_delete.return_value = docs_to_delete
        mk_hooks.kernel_connect.return_value = Mock(status_code=http.client.NO_CONTENT)
        result = delete_documents(**self.kwargs)
        self.assertEqual(
            result, list(set(self.kwargs["xmls_filenames"]) - set(docs_to_delete[0]))
        )


if __name__ == "__main__":
    main()
