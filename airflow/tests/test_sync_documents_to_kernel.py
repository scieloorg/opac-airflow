from unittest import TestCase, main
from unittest.mock import patch, MagicMock
from tempfile import mkdtemp

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
    @patch("sync_documents_to_kernel.sync_documents_to_kernel_operations.delete_documents")
    def test_delete_documents_gets_sps_package_from_dag_run_conf(
        self, mk_delete_documents
    ):
        mk_dag_run = MagicMock()
        kwargs = {"ti": MagicMock(), "dag_run": mk_dag_run}
        mk_delete_documents.return_value = [], []
        delete_documents(**kwargs)
        mk_dag_run.conf.get.assert_called_once_with("sps_package")

    @patch("sync_documents_to_kernel.sync_documents_to_kernel_operations.delete_documents")
    def test_delete_documents_gets_ti_xcom_info(self, mk_delete_documents):
        mk_dag_run = MagicMock()
        kwargs = {"ti": MagicMock(), "dag_run": mk_dag_run}
        mk_delete_documents.return_value = [], []
        delete_documents(**kwargs)
        kwargs["ti"].xcom_pull.assert_called_once_with(
            key="xmls_filenames", task_ids="list_docs_task_id"
        )

    @patch("sync_documents_to_kernel.sync_documents_to_kernel_operations.delete_documents")
    def test_delete_documents_empty_ti_xcom_info(self, mk_delete_documents):
        mk_dag_run = MagicMock()
        kwargs = {"ti": MagicMock(), "dag_run": mk_dag_run}
        kwargs["ti"].xcom_pull.return_value = None
        delete_documents(**kwargs)
        mk_delete_documents.assert_not_called()
        kwargs["ti"].xcom_push.assert_not_called()

    @patch("sync_documents_to_kernel.sync_documents_to_kernel_operations.delete_documents")
    def test_delete_documents_calls_delete_documents_operation(
        self, mk_delete_documents
    ):
        xmls_filenames = [
            "1806-907X-rba-53-01-1-8.xml",
            "1806-907X-rba-53-01-9-18.xml",
            "1806-907X-rba-53-01-19-25.xml",
        ]
        mk_dag_run = MagicMock()
        mk_dag_run.conf.get.return_value = "path_to_sps_package/package.zip"
        kwargs = {"ti": MagicMock(), "dag_run": mk_dag_run}
        kwargs["ti"].xcom_pull.return_value = xmls_filenames
        mk_delete_documents.return_value = xmls_filenames, []
        delete_documents(**kwargs)
        mk_delete_documents.assert_called_once_with(
            "path_to_sps_package/package.zip", xmls_filenames
        )

    @patch("sync_documents_to_kernel.sync_documents_to_kernel_operations.delete_documents")
    def test_delete_documents_pushes_xmls_to_preserve(self, mk_delete_documents):
        xmls_filenames = [
            "1806-907X-rba-53-01-1-8.xml",
            "1806-907X-rba-53-01-9-18.xml",
            "1806-907X-rba-53-01-19-25.xml",
        ]
        xmls_to_preserve = [
            "1806-907X-rba-53-01-9-18.xml",
            "1806-907X-rba-53-01-19-25.xml",
        ]
        mk_dag_run = MagicMock()
        mk_dag_run.conf.get.return_value = "path_to_sps_package/package.zip"
        kwargs = {"ti": MagicMock(), "dag_run": mk_dag_run}
        kwargs["ti"].xcom_pull.return_value = xmls_filenames
        mk_delete_documents.return_value = xmls_to_preserve, []
        delete_documents(**kwargs)
        kwargs["ti"].xcom_push.assert_called_once_with(
            key="xmls_to_preserve", value=xmls_to_preserve
        )


class TestRegisterUpdateDocuments(TestCase):
    @patch("sync_documents_to_kernel.sync_documents_to_kernel_operations.register_update_documents")
    def test_register_update_documents_gets_ti_xcom_info(self, mk_register_update_documents):
        mk_dag_run = MagicMock()
        kwargs = {"ti": MagicMock(), "dag_run": mk_dag_run}
        mk_register_update_documents.return_value = [], []
        register_update_documents(**kwargs)
        kwargs["ti"].xcom_pull.assert_any_call(
            key="xmls_to_preserve", task_ids="delete_docs_task_id"
        )
        kwargs["ti"].xcom_pull.assert_any_call(
            key="optimized_package", task_ids="optimize_package_task_id"
        )

    @patch("sync_documents_to_kernel.sync_documents_to_kernel_operations.register_update_documents")
    def test_register_update_documents_empty_ti_xcom_info(self, mk_register_update_documents):
        mk_dag_run = MagicMock()
        kwargs = {"ti": MagicMock(), "dag_run": mk_dag_run}
        kwargs["ti"].xcom_pull.return_value = None
        register_update_documents(**kwargs)
        mk_register_update_documents.assert_not_called()
        kwargs["ti"].xcom_push.assert_not_called()

    @patch("sync_documents_to_kernel.sync_documents_to_kernel_operations.register_update_documents")
    def test_register_update_documents_calls_register_update_documents_operation(
        self, mk_register_update_documents
    ):
        xmls_filenames = [
            "1806-907X-rba-53-01-1-8.xml",
            "1806-907X-rba-53-01-9-18.xml",
            "1806-907X-rba-53-01-19-25.xml",
        ]
        mk_dag_run = MagicMock()
        mk_dag_run.conf.get.return_value = "path_to_sps_package/package.zip"
        kwargs = {"ti": MagicMock(), "dag_run": mk_dag_run}
        kwargs["ti"].xcom_pull.side_effect = [
            xmls_filenames,
            "path_to_optimized_package/package.zip"
        ]
        mk_register_update_documents.return_value = [], []
        register_update_documents(**kwargs)
        mk_register_update_documents.assert_called_once_with(
            "path_to_optimized_package/package.zip", xmls_filenames
        )

    @patch("sync_documents_to_kernel.sync_documents_to_kernel_operations.register_update_documents")
    def test_register_update_documents_does_not_push_if_no_documents_into_kernel(self, mk_register_update_documents):
        xmls_filenames = [
            "1806-907X-rba-53-01-1-8.xml",
            "1806-907X-rba-53-01-9-18.xml",
            "1806-907X-rba-53-01-19-25.xml",
        ]
        mk_dag_run = MagicMock()
        mk_dag_run.conf.get.return_value = "path_to_sps_package/package.zip"
        kwargs = {"ti": MagicMock(), "dag_run": mk_dag_run}
        kwargs["ti"].xcom_pull.return_value = xmls_filenames
        mk_register_update_documents.return_value = [], []
        register_update_documents(**kwargs)
        kwargs["ti"].xcom_push.assert_not_called()

    @patch("sync_documents_to_kernel.sync_documents_to_kernel_operations.register_update_documents")
    def test_register_update_documents_pushes_documents(self, mk_register_update_documents):
        xmls_filenames = [
            "1806-907X-rba-53-01-1-8.xml",
            "1806-907X-rba-53-01-9-18.xml",
            "1806-907X-rba-53-01-19-25.xml",
        ]
        documents = [
            "1806-907X-rba-53-01-9-18.xml",
            "1806-907X-rba-53-01-19-25.xml",
        ]
        mk_dag_run = MagicMock()
        mk_dag_run.conf.get.return_value = "path_to_sps_package/package.zip"
        kwargs = {"ti": MagicMock(), "dag_run": mk_dag_run}
        kwargs["ti"].xcom_pull.return_value = xmls_filenames
        mk_register_update_documents.return_value = documents, []
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

    @patch("sync_documents_to_kernel.Variable")
    @patch("sync_documents_to_kernel.sync_documents_to_kernel_operations.optimize_sps_pkg_zip_file")
    def test_optimize_package_gets_sps_package_from_dag_run_conf(
        self, mk_optimize_package, mk_variable
    ):
        mk_dag_run = MagicMock()
        kwargs = {"ti": MagicMock(), "dag_run": mk_dag_run}
        mk_optimize_package.return_value = [], []
        new_sps_zip_dir = mkdtemp()
        mk_variable.get.return_value = new_sps_zip_dir
        optimize_package(**kwargs)
        mk_dag_run.conf.get.assert_called_once_with("sps_package")

    @patch("sync_documents_to_kernel.Variable")
    @patch("sync_documents_to_kernel.sync_documents_to_kernel_operations.optimize_sps_pkg_zip_file")
    def test_optimize_package_gets_ti_xcom_info(self, mk_optimize_package, mk_variable):
        mk_dag_run = MagicMock()
        kwargs = {"ti": MagicMock(), "dag_run": mk_dag_run}
        mk_optimize_package.return_value = [], []
        new_sps_zip_dir = mkdtemp()
        mk_variable.get.return_value = new_sps_zip_dir
        optimize_package(**kwargs)
        kwargs["ti"].xcom_pull.assert_called_once_with(
            key="xmls_to_preserve", task_ids="delete_docs_task_id"
        )

    @patch("sync_documents_to_kernel.sync_documents_to_kernel_operations.optimize_sps_pkg_zip_file")
    def test_optimize_package_does_not_call_optimize_package_operation(self, mk_optimize_package):
        mk_dag_run = MagicMock()
        kwargs = {"ti": MagicMock(), "dag_run": mk_dag_run}
        kwargs["ti"].xcom_pull.return_value = None
        optimize_package(**kwargs)
        mk_optimize_package.assert_not_called()
        kwargs["ti"].xcom_push.assert_not_called()

    @patch("sync_documents_to_kernel.Variable")
    @patch("sync_documents_to_kernel.sync_documents_to_kernel_operations.optimize_sps_pkg_zip_file")
    def test_optimize_package_calls_optimize_package_operation(
        self, mk_optimize_package, mk_variable
    ):
        xmls_filenames = [
            "1806-907X-rba-53-01-1-8.xml",
            "1806-907X-rba-53-01-9-18.xml",
            "1806-907X-rba-53-01-19-25.xml",
        ]
        mk_dag_run = MagicMock()
        mk_dag_run.conf.get.return_value = "path_to_sps_package/package.zip"
        kwargs = {"ti": MagicMock(), "dag_run": mk_dag_run}
        kwargs["ti"].xcom_pull.return_value = xmls_filenames
        mk_optimize_package.return_value = "path_to_otimized_package/package.zip"
        new_sps_zip_dir = mkdtemp()
        mk_variable.get.return_value = new_sps_zip_dir
        optimize_package(**kwargs)
        mk_optimize_package.assert_called_once_with(
            "path_to_sps_package/package.zip", new_sps_zip_dir
        )

    @patch("sync_documents_to_kernel.Variable")
    @patch("sync_documents_to_kernel.sync_documents_to_kernel_operations.optimize_sps_pkg_zip_file")
    def test_optimize_package_does_not_push_if_none_was_optimized(self, mk_optimize_package, mk_variable):
        xmls_filenames = [
            "1806-907X-rba-53-01-1-8.xml",
            "1806-907X-rba-53-01-9-18.xml",
            "1806-907X-rba-53-01-19-25.xml",
        ]
        mk_dag_run = MagicMock()
        mk_dag_run.conf.get.return_value = "path_to_sps_package/package.zip"
        kwargs = {"ti": MagicMock(), "dag_run": mk_dag_run}
        kwargs["ti"].xcom_pull.return_value = xmls_filenames
        mk_optimize_package.return_value = None
        new_sps_zip_dir = mkdtemp()
        mk_variable.get.return_value = new_sps_zip_dir
        optimize_package(**kwargs)
        kwargs["ti"].xcom_push.assert_not_called()

    @patch("sync_documents_to_kernel.Variable")
    @patch("sync_documents_to_kernel.sync_documents_to_kernel_operations.optimize_sps_pkg_zip_file")
    def test_optimize_package_pushes_optimized_package(self, mk_optimize_package, mk_variable):
        xmls_filenames = [
            "1806-907X-rba-53-01-1-8.xml",
            "1806-907X-rba-53-01-9-18.xml",
            "1806-907X-rba-53-01-19-25.xml",
        ]
        
        mk_dag_run = MagicMock()
        mk_dag_run.conf.get.return_value = "path_to_sps_package/package.zip"
        kwargs = {"ti": MagicMock(), "dag_run": mk_dag_run}
        kwargs["ti"].xcom_pull.return_value = xmls_filenames
        mk_optimize_package.return_value = "path_to_otimized_package/package.zip"
        new_sps_zip_dir = mkdtemp()
        mk_variable.get.return_value = new_sps_zip_dir
        optimize_package(**kwargs)
        kwargs["ti"].xcom_push.assert_called_once_with(
            key="optimized_package", value="path_to_otimized_package/package.zip"
        )


if __name__ == "__main__":
    main()
