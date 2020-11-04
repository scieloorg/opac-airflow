import tempfile
import shutil
import pathlib
from datetime import datetime
from unittest import TestCase, main
from unittest.mock import patch, MagicMock, ANY, call

from airflow import DAG
from airflow.models import Variable

from extract_packages_info import (
    optimize_packages,
    extract_packages_data,
    start_sync_documents,
)


@patch(
    "extract_packages_info.sync_documents_to_kernel_operations.optimize_sps_pkg_zip_file"
)
@patch("extract_packages_info.Variable")
class TestOptimizePackages(TestCase):
    def setUp(self):
        self.id_proc_gerapadrao = "2020-01-01-16412600000000"
        self.sps_pkg_zip_dir = tempfile.mkdtemp()
        self.optimize_path = pathlib.Path(self.sps_pkg_zip_dir) / self.id_proc_gerapadrao
        self.optimize_path.mkdir()
        self.mk_dag_run = MagicMock(
            conf={
                "bundle_label": "acron_v1n1",
                "gerapadrao_id": self.id_proc_gerapadrao,
            }
        )
        self.kwargs = {
            "ti": MagicMock(),
            "dag_run": self.mk_dag_run,
            "run_id": "test_run_id",
        }

    def tearDown(self):
        shutil.rmtree(self.sps_pkg_zip_dir)

    def test_returns_false_if_no_sps_packages(
        self, mk_variable, mk_optimize_sps_pkg_zip_file
    ):
        mk_variable.get.return_value = self.sps_pkg_zip_dir
        self.kwargs["dag_run"].conf["sps_packages"] = []
        ret = optimize_packages(**self.kwargs)
        self.assertFalse(ret)
        self.kwargs["ti"].xcom_push.assert_not_called()

    def test_returns_true_and_pushes_optimized_list(
        self, mk_variable, mk_optimize_sps_pkg_zip_file
    ):
        mk_variable.get.return_value = self.sps_pkg_zip_dir
        mk_optimize_sps_pkg_zip_file.side_effect = [
            "optimized_package1.zip",
            None,
            "optimized_package3.zip",
            None,
            "optimized_package5.zip",
        ]
        self.kwargs["dag_run"].conf["sps_packages"] = [
            "package1.zip",
            "package2.zip",
            "package3.zip",
            "package4.zip",
            "package5.zip",
        ]
        ret = optimize_packages(**self.kwargs)
        self.assertTrue(ret)
        self.kwargs["ti"].xcom_push.assert_called_once_with(
            key="optimized_packages",
            value=[
                "optimized_package1.zip",
                "package2.zip",
                "optimized_package3.zip",
                "package4.zip",
                "optimized_package5.zip",
            ]
        )


@patch("extract_packages_info.save_document_in_database")
@patch("extract_packages_info.sync_documents_to_kernel_operations.extract_package_data")
class TestExtractPackagesData(TestCase):
    def setUp(self):
        self.id_proc_gerapadrao = "2020-01-01-16412600000000"
        self.kwargs = {
            "ti": MagicMock(),
            "dag_run": MagicMock(
                conf={
                    "bundle_label": "acron_v1n1",
                    "gerapadrao_id": self.id_proc_gerapadrao,
                }
            ),
            "run_id": "test_run_id",
            "execution_date": datetime.utcnow().astimezone().isoformat()
        }
        self.kwargs["ti"].xcom_pull.return_value = [
            "optimized_package1.zip",
            "package2.zip",
            "optimized_package3.zip",
        ]
        self.extracted_data = [
            (
                {
                    "doc-xml-1.xml": {
                        "pid": "pid-v3-1",
                        "deletion": False,
                        "file_name": "doc-xml-1.xml",
                        "package_path": "optimized_package1.zip",
                    }
                },
                ["file_name", "package_path"],
            ),
            (
                {
                    "doc-xml-1.xml": {
                        "pid": "pid-v3-1",
                        "deletion": True,
                        "file_name": "doc-xml-1.xml",
                        "package_path": "package2.zip",
                    },
                    "doc-xml-2.xml": {
                        "pid": "pid-v3-2",
                        "deletion": False,
                        "file_name": "doc-xml-2.xml",
                        "package_path": "package2.zip",
                    },
                },
                ["file_name", "package_path"],
            ),
            (
                {
                    "doc-xml-1.xml": {
                        "pid": "pid-v3-1",
                        "deletion": False,
                        "file_name": "doc-xml-1.xml",
                        "package_path": "optimized_package3.zip",
                    },
                    "doc-xml-2.xml": {
                        "pid": "pid-v3-2",
                        "deletion": False,
                        "file_name": "doc-xml-2.xml",
                        "package_path": "optimized_package3.zip",
                    },
                    "doc-xml-3.xml": {
                        "pid": "pid-v3-3",
                        "deletion": False,
                        "file_name": "doc-xml-3.xml",
                        "package_path": "optimized_package3.zip",
                    },
                },
                ["file_name", "package_path"],
            ),
        ]


    def test_returns_false_if_no_documents_info(
        self, mk_extract_package_data, mk_save_document_in_database
    ):
        mk_extract_package_data.return_value = ({}, [])

        ret = extract_packages_data(**self.kwargs)

        self.assertFalse(ret)
        mk_save_document_in_database.assert_not_called()
        self.kwargs["ti"].xcom_push.assert_not_called()

    def test_calls_save_document_in_database_for_each_doc_in_each_package(
        self, mk_extract_package_data, mk_save_document_in_database
    ):
        mk_extract_package_data.side_effect = self.extracted_data
        ret = extract_packages_data(**self.kwargs)

        self.assertTrue(ret)
        data_to_complete = {
            "bundle_label": self.kwargs["dag_run"].conf["bundle_label"],
            "gerapadrao_id": self.id_proc_gerapadrao,
            "pre_sync_dag_run": self.kwargs["run_id"],
            "execution_date": self.kwargs["execution_date"],
        }
        filter = {"file_name": "doc-xml-1.xml", "package_path": "package1.zip"}
        filter.update(data_to_complete)

        for documents_info, fields_filter in self.extracted_data:
            for document_record in documents_info.values():
                filter = {
                    field: value
                    for field, value in document_record.items()
                    if field in fields_filter
                }
                filter.update(data_to_complete)
                document_record.update(data_to_complete)
                with self.subTest(document_record=document_record):
                    mk_save_document_in_database.assert_any_call(
                        "e_documents", filter, document_record
                    )

    def test_returns_true_and_pushes_scielo_ids_list(
        self, mk_extract_package_data, mk_save_document_in_database
    ):
        mk_extract_package_data.side_effect = self.extracted_data

        ret = extract_packages_data(**self.kwargs)

        self.assertTrue(ret)
        self.kwargs["ti"].xcom_push.assert_called_once_with(
            key="scielo_ids",
            value=set(["pid-v3-1", "pid-v3-2", "pid-v3-3"]),
        )


@patch("extract_packages_info.trigger_dag")
class TestStartSyncDocuments(TestCase):
    def test_start_sync_packages_calls_trigger_dag_for_each_package(
        self, mk_trigger_dag
    ):
        mk_dag_run = MagicMock(conf={"bundle_label": "acron_v1n1", "gerapadrao_id": "2020-01-01-162512123"},)
        kwargs = {"ti": MagicMock(), "dag_run": mk_dag_run, "run_id": "run_id-1234"}
        mk_scielo_pids = {"pid-v3-1", "pid-v3-2", "pid-v3-3"}
        kwargs["ti"].xcom_pull.return_value = mk_scielo_pids

        start_sync_documents(**kwargs)

        mk_trigger_dag.assert_has_calls(
            [
                call(
                    dag_id="sync_docs_to_kernel",
                    run_id=f"trig__2020-01-01-162512123_pid-v3-1",
                    execution_date=ANY,
                    replace_microseconds=False,
                    conf={
                        "scielo_id": "pid-v3-1",
                        "pre_syn_dag_run_id": "run_id-1234",
                    },
                ),
                call(
                    dag_id="sync_docs_to_kernel",
                    run_id=f"trig__2020-01-01-162512123_pid-v3-2",
                    execution_date=ANY,
                    replace_microseconds=False,
                    conf={
                        "scielo_id": "pid-v3-2",
                        "pre_syn_dag_run_id": "run_id-1234",
                    },
                ),
                call(
                    dag_id="sync_docs_to_kernel",
                    run_id=f"trig__2020-01-01-162512123_pid-v3-3",
                    execution_date=ANY,
                    replace_microseconds=False,
                    conf={
                        "scielo_id": "pid-v3-3",
                        "pre_syn_dag_run_id": "run_id-1234",
                    },
                ),
            ],
            any_order=True,
        )


if __name__ == "__main__":
    main()
