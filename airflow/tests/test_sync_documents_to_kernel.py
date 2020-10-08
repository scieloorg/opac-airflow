import os
from unittest import TestCase, main
from unittest.mock import patch, MagicMock, ANY, call
from tempfile import mkdtemp
from copy import deepcopy

from airflow import DAG

from sync_documents_to_kernel import (
    list_documents,
    delete_documents,
    register_update_documents,
    link_documents_to_documentsbundle,
    optimize_package,
)


class TestListDocuments(TestCase):
    @patch("sync_documents_to_kernel.sync_documents_to_kernel_operations.list_documents")
    def test_list_document_gets_sps_package_from_dag_run_conf(self, mk_list_documents):
        mk_dag_run = MagicMock()
        kwargs = {"ti": MagicMock(), "dag_run": mk_dag_run}
        list_documents(**kwargs)
        mk_dag_run.conf.get.assert_called_once_with("sps_package")

    @patch("sync_documents_to_kernel.sync_documents_to_kernel_operations.list_documents")
    def test_list_document_calls_list_documents_operation(self, mk_list_documents):
        mk_dag_run = MagicMock()
        mk_dag_run.conf.get.return_value = "path_to_sps_package/package.zip"
        kwargs = {"ti": MagicMock(), "dag_run": mk_dag_run}
        list_documents(**kwargs)
        mk_list_documents.assert_called_once_with("path_to_sps_package/package.zip")

    @patch("sync_documents_to_kernel.sync_documents_to_kernel_operations.list_documents")
    def test_list_document_pushes_xmls_from_packages(self, mk_list_documents):
        expected = [
            "1806-907X-rba-53-01-1-8.xml",
            "1806-907X-rba-53-01-9-18.xml",
            "1806-907X-rba-53-01-19-25.xml",
        ]
        mk_dag_run = MagicMock()
        mk_dag_run.conf.get.return_value = "path_to_sps_package/package.zip"
        kwargs = {"ti": MagicMock(), "dag_run": mk_dag_run}
        mk_list_documents.return_value = expected
        list_documents(**kwargs)
        kwargs["ti"].xcom_push.assert_called_once_with(
            key="xmls_filenames", value=expected
        )

    @patch("sync_documents_to_kernel.sync_documents_to_kernel_operations.list_documents")
    def test_list_document_doesnt_call_ti_xcom_push_if_no_xml_files(
        self, mk_list_documents
    ):
        mk_dag_run = MagicMock()
        mk_dag_run.conf.get.return_value = "path_to_sps_package/package.zip"
        kwargs = {"ti": MagicMock(), "dag_run": mk_dag_run}
        mk_list_documents.return_value = []
        list_documents(**kwargs)
        kwargs["ti"].xcom_push.assert_not_called()


class TestDeleteDocuments(TestCase):
    @patch(
        "sync_documents_to_kernel.sync_documents_to_kernel_operations.delete_documents_from_packages"
    )
    def test_gets_ti_xcom_info(self, mk_delete_documents):
        mk_dag_run = MagicMock()
        kwargs = {"ti": MagicMock(), "dag_run": mk_dag_run}
        mk_delete_documents.return_value = {}, []

        delete_documents(**kwargs)
        kwargs["ti"].xcom_pull.assert_called_once_with(
            key="xmls_filenames", task_ids="list_docs_task_id"
        )

    @patch(
        "sync_documents_to_kernel.sync_documents_to_kernel_operations.delete_documents_from_packages"
    )
    def test_empty_ti_xcom_info(self, mk_delete_documents):
        kwargs = {"ti": MagicMock(), "dag_run": MagicMock()}
        kwargs["ti"].xcom_pull.return_value = None

        task_result = delete_documents(**kwargs)
        mk_delete_documents.assert_not_called()
        kwargs["ti"].xcom_push.assert_not_called()
        self.assertFalse(task_result)

    @patch(
        "sync_documents_to_kernel.sync_documents_to_kernel_operations.delete_documents_from_packages"
    )
    def test_calls_delete_documents_operation(self, mk_delete_documents):
        xmls_filenames = {
            "path_to_sps_package/package-1.zip": [
                "1806-907X-rba-53-01-1-8.xml",
            ],
            "path_to_sps_package/package-2.zip": [
                "1806-907X-rba-53-01-1-8.xml",
                "1806-907X-rba-53-01-9-18.xml",
            ],
            "path_to_sps_package/package-3.zip": [
                "1806-907X-rba-53-01-1-8.xml",
                "1806-907X-rba-53-01-9-18.xml",
                "1806-907X-rba-53-01-19-25.xml",
            ],
        }
        kwargs = {"ti": MagicMock(), "dag_run": MagicMock()}
        kwargs["ti"].xcom_pull.return_value = xmls_filenames
        mk_delete_documents.return_value = xmls_filenames, []

        delete_documents(**kwargs)
        mk_delete_documents.assert_called_once_with(xmls_filenames)

    @patch("sync_documents_to_kernel.add_execution_in_database")
    @patch(
        "sync_documents_to_kernel.sync_documents_to_kernel_operations.delete_documents_from_packages"
    )
    def test_adds_execution_in_database(
        self, mk_delete_documents, mk_add_execution_in_database
    ):
        xmls_filenames = {
            "path_to_sps_package/package-1.zip": [
                "1806-907X-rba-53-01-1-8.xml",
            ],
            "path_to_sps_package/package-2.zip": [
                "1806-907X-rba-53-01-1-8.xml",
                "1806-907X-rba-53-01-9-18.xml",
            ],
            "path_to_sps_package/package-3.zip": [
                "1806-907X-rba-53-01-1-8.xml",
                "1806-907X-rba-53-01-9-18.xml",
                "1806-907X-rba-53-01-19-25.xml",
            ],
        }
        xmls_to_preserve = {
            "path_to_sps_package/package-1.zip": [],
            "path_to_sps_package/package-2.zip": [],
            "path_to_sps_package/package-3.zip": [],
        }
        executions = []
        for package, deleted_xmls in xmls_filenames.items():
            for deleted_xml in deleted_xmls:
                executions.append(
                    {
                        "package_name": package,
                        "file_name": deleted_xml,
                        "deletion": True,
                        "pid": ANY
                    }
                )

        mk_dag_run = MagicMock()
        mk_dag_run.conf.get.return_value = "pre_sync_dag_run_id"
        kwargs = {"ti": MagicMock(), "dag_run": mk_dag_run, "run_id": "dag_run_id"}
        kwargs["ti"].xcom_pull.return_value = xmls_filenames
        mk_delete_documents.return_value = xmls_to_preserve, executions
        expected_executions = deepcopy(executions)

        task_result = delete_documents(**kwargs)
        for execution in expected_executions:
            execution["dag_run"] = "dag_run_id"
            execution["pre_sync_dag_run"] = "pre_sync_dag_run_id"
            mk_add_execution_in_database.assert_any_call(
                table="xml_documents", data=execution
            )

    @patch(
        "sync_documents_to_kernel.sync_documents_to_kernel_operations.delete_documents_from_packages"
    )
    def test_pushes_xmls_to_preserve(self, mk_delete_documents):
        xmls_filenames = {
            "path_to_sps_package/package-1.zip": [
                "1806-907X-rba-53-01-1-8.xml",
            ],
            "path_to_sps_package/package-2.zip": [
                "1806-907X-rba-53-01-1-8.xml",
                "1806-907X-rba-53-01-9-18.xml",
            ],
            "path_to_sps_package/package-3.zip": [
                "1806-907X-rba-53-01-1-8.xml",
                "1806-907X-rba-53-01-9-18.xml",
                "1806-907X-rba-53-01-19-25.xml",
            ],
        }
        xmls_to_preserve = {
            "path_to_sps_package/package-1.zip": [
                "1806-907X-rba-53-01-1-8.xml",
            ],
            "path_to_sps_package/package-2.zip": [
                "1806-907X-rba-53-01-9-18.xml",
            ],
            "path_to_sps_package/package-3.zip": [
                "1806-907X-rba-53-01-1-8.xml",
                "1806-907X-rba-53-01-19-25.xml",
            ],
        }
        kwargs = {"ti": MagicMock(), "dag_run": MagicMock()}
        kwargs["ti"].xcom_pull.return_value = xmls_filenames
        mk_delete_documents.return_value = xmls_to_preserve, []

        task_result = delete_documents(**kwargs)
        kwargs["ti"].xcom_push.assert_called_once_with(
            key="xmls_to_preserve", value=xmls_to_preserve
        )
        self.assertTrue(task_result)


class TestRegisterUpdateDocuments(TestCase):
    def setUp(self):
        self.xmls_filenames = {
            "optimized/package-1.zip": [
                "1806-907X-rba-53-01-1-8.xml",
            ],
            "path_to_sps_package/package-2.zip": [
                "1806-907X-rba-53-01-1-8.xml",
                "1806-907X-rba-53-01-9-18.xml",
            ],
            "optimized/package-3.zip": [
                "1806-907X-rba-53-01-1-8.xml",
                "1806-907X-rba-53-01-9-18.xml",
                "1806-907X-rba-53-01-19-25.xml",
            ],
        }

    @patch("sync_documents_to_kernel.sync_documents_to_kernel_operations.put_documents_in_kernel")
    def test_gets_ti_xcom_info(self, mk_put_documents_in_kernel):
        mk_dag_run = MagicMock()
        kwargs = {"ti": MagicMock(), "dag_run": mk_dag_run}
        mk_put_documents_in_kernel.return_value = {}, []
        register_update_documents(**kwargs)
        kwargs["ti"].xcom_pull.assert_any_call(
            key="optimized_packages", task_ids="optimize_package_task_id"
        )

    @patch("sync_documents_to_kernel.sync_documents_to_kernel_operations.put_documents_in_kernel")
    def test_empty_ti_xcom_info(self, mk_put_documents_in_kernel):
        mk_dag_run = MagicMock()
        kwargs = {"ti": MagicMock(), "dag_run": mk_dag_run}
        kwargs["ti"].xcom_pull.return_value = None
        register_update_documents(**kwargs)
        mk_put_documents_in_kernel.assert_not_called()
        kwargs["ti"].xcom_push.assert_not_called()

    @patch("sync_documents_to_kernel.sync_documents_to_kernel_operations.put_documents_in_kernel")
    def test_calls_put_documents_in_kernel_operation(
        self, mk_put_documents_in_kernel
    ):
        mk_dag_run = MagicMock()
        kwargs = {"ti": MagicMock(), "dag_run": mk_dag_run}
        kwargs["ti"].xcom_pull.return_value = self.xmls_filenames
        mk_put_documents_in_kernel.return_value = {}, []

        register_update_documents(**kwargs)

        mk_put_documents_in_kernel.assert_called_once_with(self.xmls_filenames)

    @patch("sync_documents_to_kernel.sync_documents_to_kernel_operations.put_documents_in_kernel")
    def test_does_not_push_if_no_documents_into_kernel(self, mk_put_documents_in_kernel):
        mk_dag_run = MagicMock()
        kwargs = {"ti": MagicMock(), "dag_run": mk_dag_run}
        kwargs["ti"].xcom_pull.return_value = self.xmls_filenames
        mk_put_documents_in_kernel.return_value = {}, []
        register_update_documents(**kwargs)
        kwargs["ti"].xcom_push.assert_not_called()

    @patch("sync_documents_to_kernel.add_execution_in_database")
    @patch("sync_documents_to_kernel.sync_documents_to_kernel_operations.put_documents_in_kernel")
    def test_adds_execution_in_database(
        self, mk_put_documents_in_kernel, mk_add_execution_in_database
    ):
        documents = {
            package: [
                {f"scielo_id": "pid-v3-{i}", "xml_filename": xml}
                for i, xml in enumerate(xmls, 1)
            ]
            for package, xmls in self.xmls_filenames.items()
            if package != "optimized/package-1.zip"
        }
        executions = [
            {
                "package_name": "optimized/package-1.zip",
                "file_name": "document.xml",
                "failed": True,
                "error": "Error"
            }
        ]
        mk_dag_run = MagicMock()
        kwargs = {"ti": MagicMock(), "dag_run": mk_dag_run}
        kwargs["ti"].xcom_pull.return_value = self.xmls_filenames
        mk_put_documents_in_kernel.return_value = documents, executions
        register_update_documents(**kwargs)
        mk_add_execution_in_database.assert_has_calls(
            [call(table="xml_documents", data=execution) for execution in executions]
        )

    @patch("sync_documents_to_kernel.sync_documents_to_kernel_operations.put_documents_in_kernel")
    def test_pushes_documents(self, mk_put_documents_in_kernel):
        documents = {
            package: [
                {f"scielo_id": "pid-v3-{i}", "xml_filename": xml}
                for i, xml in enumerate(xmls, 1)
            ]
            for package, xmls in self.xmls_filenames.items()
            if package != "optimized/package-1.zip"
        }
        executions = [
            {
                "package_name": "optimized/package-1.zip",
                "file_name": "document.xml",
                "failed": True,
                "error": "Error"
            }
        ]
        mk_dag_run = MagicMock()
        kwargs = {"ti": MagicMock(), "dag_run": mk_dag_run}
        kwargs["ti"].xcom_pull.return_value = self.xmls_filenames
        mk_put_documents_in_kernel.return_value = documents, []
        register_update_documents(**kwargs)
        kwargs["ti"].xcom_push.assert_called_once_with(
            key="documents", value=documents
        )


@patch(
    "sync_documents_to_kernel.sync_documents_to_kernel_operations.link_documents_to_documentsbundle"
)
class TestLinkDocumentsToDocumentsbundle(TestCase):
    def test_link_documents_to_documentsbundle_gets_sps_package_from_dag_run_conf(
        self, mk_link_documents
    ):
        mk_dag_run = MagicMock()
        kwargs = {"ti": MagicMock(), "dag_run": mk_dag_run}
        mk_link_documents.return_value = [], []
        link_documents_to_documentsbundle(**kwargs)
        mk_dag_run.conf.get.assert_called_once_with("sps_package")

    def test_link_documents_to_documentsbundle_gets_ti_xcom_documents(self, mk_link_documents):

        kwargs = {"ti": MagicMock(), "dag_run": MagicMock()}

        mk_link_documents.return_value = [], []
        link_documents_to_documentsbundle(**kwargs)

        kwargs["ti"].xcom_pull.assert_any_call(
            key="documents", task_ids="register_update_docs_id"
        )

    def test_link_documents_to_documentsbundle_gets_ti_xcom_title_json_path(self, mk_link_documents):

        kwargs = {"ti": MagicMock(), "dag_run": MagicMock()}
        mk_link_documents.return_value = [], []
        link_documents_to_documentsbundle(**kwargs)

        kwargs["ti"].xcom_pull.assert_any_call(
            task_ids="process_journals_task",
            dag_id="sync_isis_to_kernel",
            key="issn_index_json_path",
            include_prior_dates=True
        )

    def test_link_documents_to_documentsbundle_empty_ti_xcom_documents(self, mk_link_documents):

        kwargs = {"ti": MagicMock(), "dag_run": MagicMock()}

        kwargs["ti"].xcom_pull.return_value = None

        link_documents_to_documentsbundle(**kwargs)

        mk_link_documents.assert_not_called()

        kwargs["ti"].xcom_push.assert_not_called()

    def test_link_documents_to_documentsbundle_calls_link_documents_to_documentsbundle_operation(
        self, mk_link_documents
    ):

        documents = [
                        {
                            'scielo_id': 'JV5Lb3v3HBYmPPdG6QD9jGQ',
                            'issn': '1806-907X',
                            'year': '2003',
                            'order': '00001',
                            'xml_package_name': '1806-907X-rba-53-01-1-8',
                            'assets': [],
                            'pdfs': [
                                {
                                    'lang': 'pt',
                                    'filename': '1806-907X-rba-53-01-1-8.pdf',
                                    'mimetype': 'application/pdf'
                                }],
                            'volume': '53',
                            'number': '01',
                            'xml_url': 'http://192.168.169.185:9000/documentstore/1806-907X/JV5Lb3v3HBYmPPdG6QD9jGQ/e8a6df175375a6f922cf8a3bf2ef4a0ce2b09c93.xml'
                        }
                    ]

        mk_dag_run = MagicMock()
        mk_dag_run.conf.get.return_value = "path_to_sps_package/package.zip"
        kwargs = {"ti": MagicMock(), "dag_run": mk_dag_run}

        kwargs["ti"].xcom_pull.side_effect = [documents, "/json/title.json"]

        mk_link_documents.return_value = [], []
        link_documents_to_documentsbundle(**kwargs)

        mk_link_documents.assert_called_once_with(
            "path_to_sps_package/package.zip", documents, "/json/title.json"
        )

    def test_link_documents_to_documentsbundle_does_not_push_if_no_documents(self, mk_link_documents):
        documents = []

        kwargs = {"ti": MagicMock(), "dag_run": MagicMock()}

        kwargs["ti"].xcom_pull.return_value = documents

        mk_link_documents.return_value = []

        link_documents_to_documentsbundle(**kwargs)

        kwargs["ti"].xcom_push.assert_not_called()

    def test_link_documents_to_documentsbundle_pushes_documents(self, mk_link_documents):

        documents = [
                        {
                            'scielo_id': 'JV5Lb3v3HBYmPPdG6QD9jGQ',
                            'issn': '1806-907X',
                            'year': '2003',
                            'order': '00001',
                            'xml_package_name': '1806-907X-rba-53-01-1-8',
                            'assets': [],
                            'pdfs': [
                                {
                                    'lang': 'pt',
                                    'filename': '1806-907X-rba-53-01-1-8.pdf',
                                    'mimetype': 'application/pdf'
                                }],
                            'volume': '53',
                            'number': '01',
                            'xml_url': 'http://192.168.169.185:9000/documentstore/1806-907X/JV5Lb3v3HBYmPPdG6QD9jGQ/e8a6df175375a6f922cf8a3bf2ef4a0ce2b09c93.xml'
                        }
                    ]

        pushed_documents = [{'1806-907X-2003-v53-n1': 204}]

        kwargs = {"ti": MagicMock(), "dag_run": MagicMock()}

        kwargs["ti"].xcom_pull.return_value = documents

        mk_link_documents.return_value = pushed_documents, []

        link_documents_to_documentsbundle(**kwargs)

        kwargs["ti"].xcom_push.assert_called_once_with(
            key="linked_bundle", value=pushed_documents
        )



class TestOptimizeDocuments(TestCase):

    def setUp(self):
        self.xmls_to_preserve = {
            "path_to_sps_package/package-1.zip": [
                "1806-907X-rba-53-01-1-8.xml",
            ],
            "path_to_sps_package/package-2.zip": [
                "1806-907X-rba-53-01-9-18.xml",
            ],
            "path_to_sps_package/package-3.zip": [
                "1806-907X-rba-53-01-1-8.xml",
                "1806-907X-rba-53-01-19-25.xml",
            ],
        }

    @patch("sync_documents_to_kernel.Variable")
    @patch("sync_documents_to_kernel.sync_documents_to_kernel_operations.optimize_sps_pkg_zip_file")
    def test_optimize_package_gets_ti_xcom_info(self, mk_optimize_package, mk_variable):
        kwargs = {"ti": MagicMock(), "dag_run": MagicMock()}
        mk_optimize_package.return_value = [], []
        new_sps_zip_dir = mkdtemp()
        mk_variable.get.return_value = new_sps_zip_dir
        optimize_package(**kwargs)
        kwargs["ti"].xcom_pull.assert_called_once_with(
            key="xmls_to_preserve", task_ids="delete_docs_task_id"
        )

    @patch("sync_documents_to_kernel.sync_documents_to_kernel_operations.optimize_sps_pkg_zip_file")
    def test_optimize_package_does_not_call_optimize_package_operation(self, mk_optimize_package):
        kwargs = {"ti": MagicMock(), "dag_run": MagicMock()}
        kwargs["ti"].xcom_pull.return_value = None
        optimize_package(**kwargs)
        mk_optimize_package.assert_not_called()
        kwargs["ti"].xcom_push.assert_not_called()

    @patch("sync_documents_to_kernel.Variable")
    @patch("sync_documents_to_kernel.sync_documents_to_kernel_operations.optimize_sps_pkg_zip_file")
    def test_optimize_package_calls_optimize_package_operation(
        self, mk_optimize_package, mk_variable
    ):
        kwargs = {"ti": MagicMock(), "dag_run": MagicMock()}
        kwargs["ti"].xcom_pull.return_value = self.xmls_to_preserve
        mk_optimize_package.return_value = None
        new_sps_zip_dir = mkdtemp()
        mk_variable.get.return_value = new_sps_zip_dir
        optimize_package(**kwargs)
        for package_to_optimize in self.xmls_to_preserve.keys():
            with self.subTest(package_to_optimize=package_to_optimize):
                mk_optimize_package.assert_any_call(
                    package_to_optimize, new_sps_zip_dir
                )

    @patch("sync_documents_to_kernel.Variable")
    @patch("sync_documents_to_kernel.sync_documents_to_kernel_operations.optimize_sps_pkg_zip_file")
    def test_optimize_package_does_not_push_if_none_was_optimized(self, mk_optimize_package, mk_variable):
        kwargs = {"ti": MagicMock(), "dag_run": MagicMock()}
        kwargs["ti"].xcom_pull.return_value = self.xmls_to_preserve
        mk_optimize_package.return_value = None
        new_sps_zip_dir = mkdtemp()
        mk_variable.get.return_value = new_sps_zip_dir
        optimize_package(**kwargs)
        kwargs["ti"].xcom_push.assert_called_once_with(
            key="optimized_packages", value=self.xmls_to_preserve
        )

    @patch("sync_documents_to_kernel.Variable")
    @patch("sync_documents_to_kernel.sync_documents_to_kernel_operations.optimize_sps_pkg_zip_file")
    def test_optimize_package_pushes_optimized_package(self, mk_optimize_package, mk_variable):
        kwargs = {"ti": MagicMock(), "dag_run": MagicMock()}
        kwargs["ti"].xcom_pull.return_value = self.xmls_to_preserve
        optimize_paths = [
            "optimized/package-1.zip",
            None,
            "optimized/package-3.zip",
        ]
        mk_optimize_package.side_effect = optimize_paths
        new_sps_zip_dir = mkdtemp()
        mk_variable.get.return_value = new_sps_zip_dir

        optimize_package(**kwargs)

        optimized_packages = {
            "optimized/package-1.zip": [
                "1806-907X-rba-53-01-1-8.xml",
            ],
            "path_to_sps_package/package-2.zip": [
                "1806-907X-rba-53-01-9-18.xml",
            ],
            "optimized/package-3.zip": [
                "1806-907X-rba-53-01-1-8.xml",
                "1806-907X-rba-53-01-19-25.xml",
            ],
        }
        kwargs["ti"].xcom_push.assert_called_once_with(
            key="optimized_packages", value=optimized_packages
        )


if __name__ == "__main__":
    main()
