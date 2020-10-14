import os
import copy
import tempfile
import builtins
import json
import shutil
import pathlib
import zipfile
from unittest import TestCase, main
from unittest.mock import patch, Mock, MagicMock, ANY, mock_open, call
from tempfile import mkdtemp, mkstemp

import requests
from airflow import DAG

from operations.sync_documents_to_kernel_operations import (
    get_documents_from_packages,
    list_documents,
    delete_documents_from_packages,
    delete_documents,
    optimize_sps_pkg_zip_file,
    put_documents_in_kernel,
    register_update_documents,
    link_docs_from_packages_to_bundle,
    link_documents_to_documentsbundle,
)
from operations.exceptions import (
    DeleteDocFromKernelException,
    DocumentToDeleteException,
    PutXMLInObjectStoreException,
    RegisterUpdateDocIntoKernelException,
    LinkDocumentToDocumentsBundleException,
    Pidv3Exception
)


def create_fake_sps_packages(sps_packages: dict):
    """
    Cria pacotes fake com base em dicionário contendo o nome do pacote e os arquivos que
    devem estar contidos em cada um deles.
    """
    for package_path, package_files in sps_packages.items():
        with zipfile.ZipFile(package_path, "w") as zip_file:
            for package_file in package_files:
                zip_file.writestr(package_file, package_file)


class TestGetDocumentsFromPackages(TestCase):
    def setUp(self):
        self.sps_package = ["dir/destination/abc_v50.zip"]
        self.proc_dir_path = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.proc_dir_path)

    def test_raises_error_if_zipfile_not_found(self):
        sps_packages_files = {
            f"{self.proc_dir_path}/2020-01-01-00-01-09-090901_abc_v1n1.zip": [
                "0123-4567-abc-50-1-8.xml",
            ]
        }
        create_fake_sps_packages(sps_packages_files)

        self.assertRaises(
            FileNotFoundError, get_documents_from_packages, ["other_package.zip"]
        )

    def test_reads_all_xmls_from_zip(self):
        sps_packages_files = {
            f"{self.proc_dir_path}/2020-01-01-00-01-09-090901_abc_v1n1.zip": [
                "0123-4567-abc-50-1-8.xml",
                "v53n1a01.pdf",
            ],
            f"{self.proc_dir_path}/2020-01-01-00-01-09-090902_abc_v1n1.zip": [
                "0123-4567-abc-50-1-8.xml",
                "v53n1a01.pdf",
                "0123-4567-abc-50-1-8-gpn1a01t1.htm",
                "0123-4567-abc-50-1-8-gpn1a01g1.htm",
            ],
            f"{self.proc_dir_path}/2020-01-01-00-01-09-090903_abc_v1n1.zip": [
                "0123-4567-abc-50-1-8.xml",
                "v53n1a01.pdf",
                "0123-4567-abc-50-1-8-gpn1a01t1.htm",
                "0123-4567-abc-50-1-8-gpn1a01g1.htm",
                "0123-4567-abc-50-9-18.xml",
                "v53n1a02.pdf",
            ],
        }
        create_fake_sps_packages(sps_packages_files)

        xmls = get_documents_from_packages(list(sps_packages_files.keys()))

        expected = {
            f"{self.proc_dir_path}/2020-01-01-00-01-09-090901_abc_v1n1.zip": [
                "0123-4567-abc-50-1-8.xml",
            ],
            f"{self.proc_dir_path}/2020-01-01-00-01-09-090902_abc_v1n1.zip": [
                "0123-4567-abc-50-1-8.xml",
            ],
            f"{self.proc_dir_path}/2020-01-01-00-01-09-090903_abc_v1n1.zip": [
                "0123-4567-abc-50-1-8.xml",
                "0123-4567-abc-50-9-18.xml",
            ],
        }
        self.assertEqual(xmls, expected)

    def test_returns_empty_dict_if_no_xml_in_zip(self):
        package_path = f"{self.proc_dir_path}/2020-01-01-00-01-09-090903_abc_v1n1.zip"
        sps_packages_files = {
            package_path: [
                "v53n1a01.pdf",
                "0123-4567-abc-50-1-8-gpn1a01t1.htm",
                "0123-4567-abc-50-1-8-gpn1a01g1.htm",
                "v53n1a02.pdf",
            ],
        }
        create_fake_sps_packages(sps_packages_files)

        xmls = get_documents_from_packages(list(sps_packages_files.keys()))

        self.assertEqual(xmls, {})


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


class TestDeleteDocumentsFromPackages(TestCase):
    def setUp(self):
        self.proc_dir_path = tempfile.mkdtemp()
        self.kwargs = {
            "bundle_xmls": {
                f"{self.proc_dir_path}/2020-01-01-00-01-09-090901_abc_v1n1.zip": [
                    "0123-4567-abc-50-1-8.xml",
                ],
                f"{self.proc_dir_path}/2020-01-01-00-01-09-090902_abc_v1n1.zip": [
                    "0123-4567-abc-50-1-8.xml",
                    "0123-4567-abc-50-1-18.xml",
                ],
                f"{self.proc_dir_path}/2020-01-01-00-01-09-090903_abc_v1n1.zip": [
                    "0123-4567-abc-50-1-8.xml",
                    "0123-4567-abc-50-1-18.xml",
                    "0123-4567-abc-50-1-20.xml",
                ],
            },
        }
        self.are_docs_to_delete = [
            (True, "FX6F3cbyYmmwvtGmMB7WCgr",),
            (True, "FX6F3cbyYmmwvtGmMB7WCgr",),
            (True, "GZ5K2cbyYmmwvtGmMB71243",),
            (True, "FX6F3cbyYmmwvtGmMB7WCgr",),
            (True, "GZ5K2cbyYmmwvtGmMB71243",),
            (True, "KU890cbyYmmwvtGmMB7JUk4",),
        ]
        self.expected_pids_calls = [
            ("FX6F3cbyYmmwvtGmMB7WCgr",),
            (
                "FX6F3cbyYmmwvtGmMB7WCgr", "GZ5K2cbyYmmwvtGmMB71243",
            ),
            (
                "FX6F3cbyYmmwvtGmMB7WCgr",
                "GZ5K2cbyYmmwvtGmMB71243",
                "KU890cbyYmmwvtGmMB7JUk4",
            ),
        ]

    def tearDown(self):
        shutil.rmtree(self.proc_dir_path)

    @patch("operations.sync_documents_to_kernel_operations.delete_documents")
    def test_calls_delete_documents_for_each_package(self, mk_delete_documents):
        mk_delete_documents.return_value = (None, None,)
        delete_documents_from_packages(**self.kwargs)
        for sps_package, xmls_filenames in self.kwargs["bundle_xmls"].items():
            with self.subTest(sps_package=sps_package, xmls_filenames=xmls_filenames):
                mk_delete_documents.assert_any_call(sps_package, xmls_filenames)

    @patch("operations.sync_documents_to_kernel_operations.delete_documents")
    def test_returns_no_xmls_to_preserve_nor_executions(self, mk_delete_documents):
        mk_delete_documents.side_effect = [
            ([], [],),
            ([], [],),
            ([], [],),
        ]

        result, executions = delete_documents_from_packages(**self.kwargs)
        self.assertEqual(result, {})
        self.assertEqual(executions, [])

    @patch("operations.sync_documents_to_kernel_operations.delete_documents")
    def test_returns_xmls_to_preserve(self, mk_delete_documents):
        expected_to_preserve = {
            f"{self.proc_dir_path}/2020-01-01-00-01-09-090901_abc_v1n1.zip": [
                "0123-4567-abc-50-1-8.xml",
            ],
            f"{self.proc_dir_path}/2020-01-01-00-01-09-090902_abc_v1n1.zip": [
                "0123-4567-abc-50-1-18.xml",
            ],
            f"{self.proc_dir_path}/2020-01-01-00-01-09-090903_abc_v1n1.zip": [
                "0123-4567-abc-50-1-8.xml",
                "0123-4567-abc-50-1-20.xml",
            ]
        }
        expected_executions = [
            {
                "package_name": "2020-01-01-00-01-09-090902_abc_v1n1.zip",
                "file_name": "0123-4567-abc-50-1-8.xml",
                "deletion": True,
                "pid": "FX6F3cbyYmmwvtGmMB7WCgr",
            },
            {
                "package_name": "2020-01-01-00-01-09-090903_abc_v1n1.zip",
                "file_name": "0123-4567-abc-50-1-18.xml",
                "deletion": True,
                "failed": True, "error": "SciELO PID V3 is None",
            },
        ]
        xmls_lists = [xmls for xmls in expected_to_preserve.values()]
        executions_returns = [[]] + [[execution] for execution in expected_executions]
        deleted_documents_returns = [
            (xmls_to_preserve, executions)
            for xmls_to_preserve, executions in zip(xmls_lists, executions_returns)
        ]
        mk_delete_documents.side_effect = deleted_documents_returns

        result, executions = delete_documents_from_packages(**self.kwargs)
        self.assertEqual(result, expected_to_preserve)
        self.assertEqual(executions, expected_executions)


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
            (True, "FX6F3cbyYmmwvtGmMB7WCgr",),
            (True, "GZ5K2cbyYmmwvtGmMB71243",),
            (True, "KU890cbyYmmwvtGmMB7JUk4",),
        ]

    @patch("operations.sync_documents_to_kernel_operations.delete_doc_from_kernel")
    @patch("operations.sync_documents_to_kernel_operations.is_document_to_delete")
    @patch("operations.sync_documents_to_kernel_operations.ZipFile")
    def test_delete_documents_opens_zip(
        self, MockZipFile, mk_document_to_delete, mk_delete_doc_from_kernel
    ):
        mk_document_to_delete.return_value = (None, None,)
        delete_documents(**self.kwargs)
        MockZipFile.assert_called_once_with(self.kwargs["sps_package"])

    @patch("operations.sync_documents_to_kernel_operations.delete_doc_from_kernel")
    @patch("operations.sync_documents_to_kernel_operations.is_document_to_delete")
    @patch("operations.sync_documents_to_kernel_operations.ZipFile")
    def test_delete_documents_calls_document_to_delete_for_each_xml(
        self, MockZipFile, mk_document_to_delete, mk_delete_doc_from_kernel
    ):
        mk_document_to_delete.return_value = (None, None,)
        delete_documents(**self.kwargs)
        for sps_xml_file in self.kwargs["xmls_filenames"]:
            with self.subTest(sps_xml_file=sps_xml_file):
                mk_document_to_delete.assert_any_call(
                    MockZipFile.return_value.__enter__.return_value,
                    sps_xml_file
                )

    @patch("operations.sync_documents_to_kernel_operations.delete_doc_from_kernel")
    @patch("operations.sync_documents_to_kernel_operations.Logger")
    @patch("operations.sync_documents_to_kernel_operations.is_document_to_delete")
    @patch("operations.sync_documents_to_kernel_operations.ZipFile")
    def test_delete_documents_logs_error_if_document_to_delete_error(
        self, MockZipFile, mk_document_to_delete, MockLogger, mk_delete_doc_from_kernel
    ):
        mk_document_to_delete.side_effect = DocumentToDeleteException("XML Error")
        delete_documents(**self.kwargs)
        mk_delete_doc_from_kernel.assert_not_called()
        for xml_filename in self.kwargs["xmls_filenames"]:
            with self.subTest(xml_filename=xml_filename):
                MockLogger.error.assert_any_call(
                    'Error reading document "%s": %s', xml_filename, "XML Error"
                )

    @patch("operations.sync_documents_to_kernel_operations.delete_doc_from_kernel")
    @patch("operations.sync_documents_to_kernel_operations.Logger")
    @patch("operations.sync_documents_to_kernel_operations.is_document_to_delete")
    @patch("operations.sync_documents_to_kernel_operations.ZipFile")
    def test_delete_documents_calls_delete_doc_from_kernel(
        self, MockZipFile, mk_document_to_delete, MockLogger, mk_delete_doc_from_kernel
    ):
        mk_document_to_delete.side_effect = self.docs_to_delete
        delete_documents(**self.kwargs)
        for is_doc_to_delete, doc_to_delete in self.docs_to_delete:
            with self.subTest(
                is_doc_to_delete=is_doc_to_delete, doc_to_delete=doc_to_delete
            ):
                mk_delete_doc_from_kernel.assert_any_call(doc_to_delete)

    @patch("operations.sync_documents_to_kernel_operations.delete_doc_from_kernel")
    @patch("operations.sync_documents_to_kernel_operations.Logger")
    @patch("operations.sync_documents_to_kernel_operations.is_document_to_delete")
    @patch("operations.sync_documents_to_kernel_operations.ZipFile")
    def test_delete_documents_logs_error_if_kernel_connect_error(
        self, MockZipFile, mk_document_to_delete, MockLogger, mk_delete_doc_from_kernel
    ):
        mk_document_to_delete.side_effect = self.docs_to_delete
        mk_delete_doc_from_kernel.side_effect = DeleteDocFromKernelException(
            "404 Client Error: Not Found"
        )
        delete_documents(**self.kwargs)
        for sps_xml_file, doc_to_delete_info in zip(
                self.kwargs["xmls_filenames"], self.docs_to_delete
            ):
            doc_to_delete = doc_to_delete_info[1]
            with self.subTest(sps_xml_file=sps_xml_file, doc_to_delete=doc_to_delete):
                MockLogger.info.assert_any_call(
                    'Could not delete "%s" (scielo_id: "%s") from kernel: %s',
                    sps_xml_file,
                    doc_to_delete,
                    "404 Client Error: Not Found"
                )

    @patch("operations.sync_documents_to_kernel_operations.delete_doc_from_kernel")
    @patch("operations.sync_documents_to_kernel_operations.Logger")
    @patch("operations.sync_documents_to_kernel_operations.is_document_to_delete")
    @patch("operations.sync_documents_to_kernel_operations.ZipFile")
    def test_delete_documents_logs_document_deletion_success(
        self, MockZipFile, mk_document_to_delete, MockLogger, mk_delete_doc_from_kernel
    ):
        mk_document_to_delete.side_effect = self.docs_to_delete
        delete_documents(**self.kwargs)
        for sps_xml_file, doc_to_delete_info in zip(
                self.kwargs["xmls_filenames"], self.docs_to_delete
            ):
            doc_to_delete = doc_to_delete_info[1]
            with self.subTest(sps_xml_file=sps_xml_file, doc_to_delete=doc_to_delete):
                MockLogger.info.assert_any_call(
                    'Document "%s" (scielo_id: "%s") deleted from kernel',
                    sps_xml_file,
                    doc_to_delete
                )

    @patch("operations.sync_documents_to_kernel_operations.delete_doc_from_kernel")
    @patch("operations.sync_documents_to_kernel_operations.Logger")
    @patch("operations.sync_documents_to_kernel_operations.is_document_to_delete")
    @patch("operations.sync_documents_to_kernel_operations.ZipFile")
    def test_delete_documents_logs_error_if_no_scielo_doc_id(
        self, MockZipFile, mk_document_to_delete, MockLogger, mk_delete_doc_from_kernel
    ):
        mk_document_to_delete.side_effect = self.docs_to_delete[:2] + [(True, None,)]
        result = delete_documents(**self.kwargs)
        MockLogger.error.assert_any_call(
            'Document "%s" will not be deleted because SciELO PID is None',
            self.kwargs["xmls_filenames"][-1]
        )

    @patch("operations.sync_documents_to_kernel_operations.delete_doc_from_kernel")
    @patch("operations.sync_documents_to_kernel_operations.Logger")
    @patch("operations.sync_documents_to_kernel_operations.is_document_to_delete")
    @patch("operations.sync_documents_to_kernel_operations.ZipFile")
    def test_delete_documents_returns_xmls_to_preserve(
        self, MockZipFile, mk_document_to_delete, MockLogger, mk_delete_doc_from_kernel
    ):
        mk_document_to_delete.side_effect = self.docs_to_delete[:2] + [(False, None,)]
        result, _ = delete_documents(**self.kwargs)
        self.assertEqual(
            result,
            list(
                set(self.kwargs["xmls_filenames"]) - set(self.kwargs["xmls_filenames"][:-1])
            )
        )


class TestPutDocumentsInKernel(TestCase):
    def setUp(self):
        self.proc_dir_path = tempfile.mkdtemp()
        self.kwargs = {
            "bundle_xmls": {
                f"{self.proc_dir_path}/2020-01-01-00-01-09-090901_abc_v1n1.zip": [
                    "0123-4567-abc-50-1-8.xml",
                ],
                f"{self.proc_dir_path}/2020-01-01-00-01-09-090902_abc_v1n1.zip": [
                    "0123-4567-abc-50-1-8.xml",
                    "0123-4567-abc-50-1-18.xml",
                ],
                f"{self.proc_dir_path}/2020-01-01-00-01-09-090903_abc_v1n1.zip": [
                    "0123-4567-abc-50-1-8.xml",
                    "0123-4567-abc-50-1-18.xml",
                    "0123-4567-abc-50-1-20.xml",
                ],
            },
        }

    @patch("operations.sync_documents_to_kernel_operations.register_update_documents")
    def test_calls_register_update_documents_for_each_package(
        self, mk_register_update_documents
    ):
        mk_register_update_documents.return_value = ({}, [])

        put_documents_in_kernel(**self.kwargs)
        for sps_package, xmls_to_preserve in self.kwargs["bundle_xmls"].items():
            with self.subTest(
                sps_package=sps_package, xmls_to_preserve=xmls_to_preserve
            ):
                mk_register_update_documents.assert_any_call(
                    sps_package, xmls_to_preserve
                )

    @patch("operations.sync_documents_to_kernel_operations.register_update_documents")
    def test_returns_executions_from_register_update_documents(
        self, mk_register_update_documents
    ):
        expected_executions = [
            {"failed": True, "error": "Erro PIDv3"},
            {"failed": True, "error": "Erro Obj Store"},
            {"pid": "nhaNDdoijdadoi2", "failed": True, "error": "Erro Kernel"},
        ]
        mk_register_update_documents.side_effect = [
            (
                [],
                [
                    {"failed": True, "error": "Erro PIDv3"},
                ]
            ),
            ([{}],[]),
            (
                [{}],
                [
                    {"failed": True, "error": "Erro Obj Store"},
                    {"pid": "nhaNDdoijdadoi2", "failed": True, "error": "Erro Kernel"},
                ]
            ),
        ]

        __, executions = put_documents_in_kernel(**self.kwargs)
        self.assertEqual(executions, expected_executions)

    @patch("operations.sync_documents_to_kernel_operations.register_update_documents")
    def test_returns_docs_metadata_from_register_update_documents(
        self, mk_register_update_documents
    ):
        expected_docs_data = {
            f"{self.proc_dir_path}/2020-01-01-00-01-09-090902_abc_v1n1.zip": [
                {"scielo_id": "pid-v3-2", "xml": "0123-4567-abc-50-1-18.xml"}
            ],
            f"{self.proc_dir_path}/2020-01-01-00-01-09-090903_abc_v1n1.zip": [
                {"scielo_id": "pid-v3-3", "xml": "0123-4567-abc-50-1-8.xml"}
            ],
        }
        expected_executions = [
            {"failed": True, "error": "Erro PIDv3"},
            {"failed": True, "error": "Erro Obj Store"},
            {"pid": "nhaNDdoijdadoi2", "failed": True, "error": "Erro Kernel"},
        ]
        mk_register_update_documents.side_effect = [
            (
                [],
                [
                    {"failed": True, "error": "Erro PIDv3"},
                ]
            ),
            ([{"scielo_id": "pid-v3-2", "xml": "0123-4567-abc-50-1-18.xml"}], []),
            (
                [{"scielo_id": "pid-v3-3", "xml": "0123-4567-abc-50-1-8.xml"}],
                [
                    {"failed": True, "error": "Erro Obj Store"},
                    {"pid": "nhaNDdoijdadoi2", "failed": True, "error": "Erro Kernel"},
                ]
            ),
        ]

        sync_data, executions = put_documents_in_kernel(**self.kwargs)
        self.assertEqual(sync_data, expected_docs_data)
        self.assertEqual(executions, expected_executions)


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
        MockLogger.error.assert_any_call(
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
    @patch("operations.sync_documents_to_kernel_operations.Logger")
    @patch("operations.sync_documents_to_kernel_operations.ZipFile")
    def test_register_update_documents_logs_error_if_put_xml_into_object_store_return_pidv3exception(
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
            Pidv3Exception("Could not get scielo id v3"),
            {},
        ]
        register_update_documents(**self.kwargs)
        MockLogger.error.assert_any_call(
            'Could not put document "%s" in object store: %s',
            self.kwargs["xmls_to_preserve"][1],
            "Could not get scielo id v3",
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
        MockLogger.error.assert_any_call(
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

        result, _ = register_update_documents(**self.kwargs)
        self.assertEqual(result, expected)

    @patch(
        "operations.sync_documents_to_kernel_operations.register_update_doc_into_kernel"
    )
    @patch(
        "operations.sync_documents_to_kernel_operations.put_assets_and_pdfs_in_object_store"
    )
    @patch("operations.sync_documents_to_kernel_operations.put_xml_into_object_store")
    @patch("operations.sync_documents_to_kernel_operations.Logger")
    @patch("operations.sync_documents_to_kernel_operations.ZipFile")
    def test_register_update_documents_logs_error_if_put_xml_into_object_store_return_pidv3exception(
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
            Pidv3Exception("Could not get scielo id v3"),
            {},
        ]
        register_update_documents(**self.kwargs)
        MockLogger.error.assert_any_call(
            'Could not put document "%s" in object store: %s',
            self.kwargs["xmls_to_preserve"][1],
            "Could not get scielo id v3",
        )


@patch(
    "operations.sync_documents_to_kernel_operations.link_documents_to_documentsbundle"
)
class TestLinkDocsFromPackagesTobundle(TestCase):
    def setUp(self):
        self.documents = {
            "path_to_package/package-1.zip": [
                {"scielo_id": "S0034-8910.2014048004923"},
            ],
            "path_to_package/package-2.zip": [
                {"scielo_id": "S0034-8910.2014048004924"},
            ],
            "path_to_package/package-3.zip": [
                {"scielo_id": "S0034-8910.20140078954641"},
                {"scielo_id": "S0034-8910.20140078954642"},
            ],
        }

    def test_calls_link_documents_to_documentsbundle(
        self, mk_link_documents_to_documentsbundle
    ):
        mk_link_documents_to_documentsbundle.side_effect = [
            ([], []), ([], []), ([], []),
        ]

        link_docs_from_packages_to_bundle(self.documents, "issn_index.json")
        mk_link_documents_to_documentsbundle.assert_has_calls([
            call(package, documents, "issn_index.json")
            for package, documents in self.documents.items()
        ])

    def test_returns_executions_from_link_documents_to_documentsbundle(
        self, mk_link_documents_to_documentsbundle
    ):
        expected_executions = [
            {"pid": "scielo-pid-v3-1", "bundle_id": None, "error": "Error",},
            {
                "pid": "scielo-pid-v3-2",
                "bundle_id": "1518-8787-v1-n1",
                "failed": True,
                "error": "Error",
            },
            {"pid": "scielo-pid-v3-3", "bundle_id": "1518-8787-v1-n1",},
            {"pid": "scielo-pid-v3-4", "bundle_id": "1518-8787-v1-n1",},
        ]
        mk_link_documents_to_documentsbundle.side_effect = [
            ([], [expected_executions[0]]),
            ([], [expected_executions[1]]),
            ([], expected_executions[2:]),
        ]
        __, executions = link_docs_from_packages_to_bundle(
            self.documents, "issn_index.json"
        )
        self.assertEqual(executions, expected_executions)

    def test_returns_linked_bundle_from_link_documents_to_documentsbundle(
        self, mk_link_documents_to_documentsbundle
    ):
        expected_executions = [
            {"pid": "scielo-pid-v3-1", "bundle_id": None, "error": "Error",},
            {
                "pid": "scielo-pid-v3-2",
                "bundle_id": "1518-8787-v1-n1",
                "failed": True,
                "error": "Error",
            },
            {"pid": "scielo-pid-v3-3", "bundle_id": "1518-8787-v1-n1",},
            {"pid": "scielo-pid-v3-4", "bundle_id": "1518-8787-v1-n1",},
        ]
        link_docs_return = [
            {"id": "1518-8787-v1-n1", "status": 404},
            {"id": "1518-8787-v1-n1", "status": 204},
            {"id": "1518-8787-v1-n1", "status": 204},
        ]
        mk_link_documents_to_documentsbundle.side_effect = [
            ([], [expected_executions[0]]),
            ([link_docs_return[0]], [expected_executions[1]]),
            (link_docs_return[1:], expected_executions[2:]),
        ]
        linked_result, executions = link_docs_from_packages_to_bundle(
            self.documents, "issn_index.json"
        )
        self.assertEqual(
            linked_result,
            {
                "path_to_package/package-2.zip": [link_docs_return[0]],
                "path_to_package/package-3.zip": link_docs_return[1:],
            }
        )


@patch("operations.sync_documents_to_kernel_operations.Logger")
@patch(
    "operations.sync_documents_to_kernel_operations.update_documents_in_bundle"
)
@patch("operations.sync_documents_to_kernel_operations.get_bundle_id")
@patch("operations.sync_documents_to_kernel_operations.get_or_create_bundle")
@patch("operations.sync_documents_to_kernel_operations.update_aop_bundle_items")
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
                "scielo_id": "S0034-8910.20140078954642",
                "issn": "1518-8787",
                "year": "2014",
                "volume": "02",
                "number": "2",
                "order": "979",
                "supplement": "1",
             }
        ]
        self.issn_index_json = json.dumps({
            "0034-8910": "0034-8910",
            "1518-8787": "0034-8910",
        })

    def test_if_link_documents_to_documentsbundle_return_none_when_param_document_empty(
        self,
        mk_update_aop_bundle_items,
        mk_get_or_create_bundle,
        mk_get_bundle_id,
        mk_regdocument,
        MockLogger
    ):

        self.assertIsNone(link_documents_to_documentsbundle(
            "path_to_sps_package/package.zip", [], None),
            None
        )

    @patch.object(builtins, "open")
    def test_link_documents_to_documentsbundle_logs_journal_issn_id_error(
        self,
        mk_open,
        mk_update_aop_bundle_items,
        mk_get_or_create_bundle,
        mk_get_bundle_id,
        mk_regdocument,
        MockLogger
    ):
        mk_open.return_value.__enter__.return_value.read.return_value = '{}'
        link_documents_to_documentsbundle(
            "path_to_sps_package/package.zip", self.documents[:1], "/json/index.json"
        )
        MockLogger.info.assert_any_call(
            'Could not get journal ISSN ID: ISSN id "%s" not found', "0034-8910"
        )

    @patch.object(builtins, "open")
    def test_link_documents_to_documentsbundle_calls_get_bundle_id_with_issn_id(
        self,
        mk_open,
        mk_update_aop_bundle_items,
        mk_get_or_create_bundle,
        mk_get_bundle_id,
        mk_regdocument,
        MockLogger
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

    def test_if_link_documents_to_documentsbundle_register_on_document_store(
        self,
        mk_update_aop_bundle_items,
        mk_get_or_create_bundle,
        mk_get_bundle_id,
        mk_regdocument,
        MockLogger
    ):
        mock_response = Mock(status_code=204)

        mk_regdocument.return_value = mock_response

        mk_get_bundle_id.side_effect = [
                                   '0034-8910-2014-v48-n2',
                                   '0034-8910-2014-v48-n2',
                                   '0034-8910-2014-v2-n2',
                                   '0034-8910-2014-v2-n2-s1'
                                   ]

        with tempfile.TemporaryDirectory() as tmpdirname:
            issn_index_json_path = os.path.join(tmpdirname, "issn_index.json")
            with open(issn_index_json_path, "w") as index_file:
                index_file.write(self.issn_index_json)

            result, _ = link_documents_to_documentsbundle(
                "path_to_sps_package/package.zip", self.documents, issn_index_json_path
            )
            self.assertEqual(
                result,
                [
                    {'id': '0034-8910-2014-v48-n2', 'status': 204},
                    {'id': '0034-8910-2014-v2-n2', 'status': 204},
                    {'id': '0034-8910-2014-v2-n2-s1', 'status': 204}
                ])

    def test_if_some_documents_are_not_register_on_document_store(
        self,
        mk_update_aop_bundle_items,
        mk_get_or_create_bundle,
        mk_get_bundle_id,
        mk_regdocument,
        MockLogger
    ):
        mk_regdocument.side_effect = [
                                      Mock(status_code=204),
                                      Mock(status_code=422),
                                      Mock(status_code=404)
                                      ]

        mk_get_bundle_id.side_effect = [
                                   '0034-8910-2014-v48-n2',
                                   '0034-8910-2014-v48-n2',
                                   '0034-8910-2014-v2-n2',
                                   '0034-8910-2014-v2-n2-s1'
                                   ]

        with tempfile.TemporaryDirectory() as tmpdirname:
            issn_index_json_path = os.path.join(tmpdirname, "issn_index.json")
            with open(issn_index_json_path, "w") as index_file:
                index_file.write(self.issn_index_json)

            result, _ = link_documents_to_documentsbundle(
                "path_to_sps_package/package.zip", self.documents, issn_index_json_path
            )
            self.assertEqual(
                result,
                [
                    {'id': '0034-8910-2014-v48-n2', 'status': 204},
                    {'id': '0034-8910-2014-v2-n2', 'status': 422},
                    {'id': '0034-8910-2014-v2-n2-s1', 'status': 404}
                ])

    @patch.object(builtins, "open")
    def test_link_documents_to_documentsbundle_should_not_emit_an_update_call_if_the_payload_wont_change(
        self,
        mk_open,
        mk_update_aop_bundle_items,
        mk_get_or_create_bundle,
        mk_get_bundle_id,
        mk_regdocument,
        MockLogger
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
        mk_regdocument.assert_not_called()

    @patch.object(builtins, "open")
    def test_link_documents_to_documentsbundle_should_not_reset_item_list_when_new_documents_arrives(
        self,
        mk_open,
        mk_update_aop_bundle_items,
        mk_get_or_create_bundle,
        mk_get_bundle_id,
        mk_regdocument,
        MockLogger
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

        mk_regdocument.assert_called_with(
            "0034-8910-2014-v48-n2", new_payload_list
        )

    @patch.object(builtins, "open")
    def test_calls_update_aop_bundle_items(
        self,
        mk_open,
        mk_update_aop_bundle_items,
        mk_get_or_create_bundle,
        mk_get_bundle_id,
        mk_regdocument,
        MockLogger
    ):
        mk_open.return_value.__enter__.return_value.read.return_value = (
            self.issn_index_json
        )
        mk_get_bundle_id.return_value = "0034-8910-2014-v48-n2"
        bundle_data = {
            "id": "0034-8910-2014-v48-n2",
            "items": [
                {"id": f"item-{number}", "order": number}
                for number in range(1, 5)
            ],
        }
        mk_get_or_create_bundle.return_value.json.return_value = bundle_data
        link_documents_to_documentsbundle(
            "path_to_sps_package/package.zip", self.documents, "/json/index.json"
        )
        expected = bundle_data["items"]
        expected += [
            {"id": document["scielo_id"], "order": document["order"]}
            for document in self.documents
        ]
        mk_update_aop_bundle_items.assert_called_with("0034-8910", expected)

    @patch.object(builtins, "open")
    def test_logs_exception_if_update_aop_bundle_items_raise_exception(
        self,
        mk_open,
        mk_update_aop_bundle_items,
        mk_get_or_create_bundle,
        mk_get_bundle_id,
        mk_regdocument,
        MockLogger
    ):
        mk_open.return_value.__enter__.return_value.read.return_value = (
            '{"0034-8910": "0101-0101"}'
        )
        mk_get_bundle_id.return_value = "0034-8910-2014-v48-n2"
        error = LinkDocumentToDocumentsBundleException("No AOP Bundle in Journal")
        error.response = Mock(status_code=404)
        mk_update_aop_bundle_items.side_effect = error
        link_documents_to_documentsbundle(
            "path_to_sps_package/package.zip", self.documents, "/json/index.json"
        )
        MockLogger.error.assert_called_with("No AOP Bundle in Journal")


@patch("operations.sync_documents_to_kernel_operations.Logger")
@patch(
    "operations.sync_documents_to_kernel_operations.update_documents_in_bundle"
)
@patch("operations.sync_documents_to_kernel_operations.get_bundle_id")
@patch("operations.sync_documents_to_kernel_operations.get_or_create_bundle")
@patch("operations.sync_documents_to_kernel_operations.update_aop_bundle_items")
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
            "1518-8787": "0034-8910",
        })

    @patch.object(builtins, "open")
    def test_calls_get_or_create_bundle(
        self,
        mk_open,
        mk_update_aop_bundle_items,
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
        mk_update_aop_bundle_items,
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
        result, _ = link_documents_to_documentsbundle(
            "path_to_sps_package/2019nahead.zip", self.documents[:1], "/json/index.json"
        )
        self.assertEqual(result, [{'id': '0101-0101-aop', 'status': 404}])
        MockLogger.info.assert_called_with(
            "Could not get bundle %: Bundle not found", "0101-0101-aop"
        )

    @patch.object(builtins, "open")
    def test_does_not_call_update_aop_bundle_items(
        self,
        mk_open,
        mk_update_aop_bundle_items,
        mk_get_or_create_bundle,
        mk_get_bundle_id,
        mk_regdocument,
        MockLogger
    ):
        mk_open.return_value.__enter__.return_value.read.return_value = (
            '{"0034-8910": "0101-0101"}'
        )
        mk_get_bundle_id.return_value = "0101-0101-aop"
        link_documents_to_documentsbundle(
            "path_to_sps_package/2019nahead.zip", self.documents[:1], "/json/index.json"
        )
        mk_update_aop_bundle_items.assert_not_called()


class TestOptimizeSPPackage(TestCase):

    @patch("operations.sync_documents_to_kernel_operations.ZipFile")
    @patch("operations.sync_documents_to_kernel_operations.Logger")
    @patch("operations.sync_documents_to_kernel_operations.SPPackage")
    @patch("operations.sync_documents_to_kernel_operations.os.path.isfile")
    def test_optimize_sps_pkg_zip_file_write_log_messages_in_and_out(
        self,
        mock_isfile,
        MockSPPackage,
        MockLogger,
        MockZipFile,
    ):
        mock_isfile.return_value = True
        MockZipFile = mock_open
        mock_optimise = Mock("optimise")
        mock_optimise.return_value = None
        mock_package = Mock("package")
        mock_package.optimise = mock_optimise
        MockSPPackage.from_file.return_value = mock_package
        new_sps_zip_dir = mkdtemp()

        ret = optimize_sps_pkg_zip_file("dir/destination/rba_v53n1.zip", new_sps_zip_dir)
        self.assertEqual(ret, os.path.join(new_sps_zip_dir, "rba_v53n1.zip"))


if __name__ == "__main__":
    main()
