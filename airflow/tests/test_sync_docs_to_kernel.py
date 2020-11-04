from unittest import TestCase, main
from unittest.mock import patch, MagicMock, call
from copy import deepcopy

from airflow import DAG

from sync_docs_to_kernel import (
    apply_document_change,
    update_document_in_bundle,
)


@patch("sync_docs_to_kernel.sync_documents_to_kernel_operations.sync_document")
@patch("sync_docs_to_kernel.add_execution_in_database")
@patch("sync_docs_to_kernel.get_documents_in_database")
class TestApplyDocumentChange(TestCase):
    def setUp(self):
        self.kwargs = {"ti": MagicMock(), "dag_run": MagicMock(), "run_id": "test_run_id"}
        self.kwargs["dag_run"].conf = {
            "scielo_id": "pid-v3-1",
            "pre_syn_dag_run_id": "test_pre_run_id",
        }

    def test_calls_sync_document_operation(
        self,
        mk_get_documents_in_database,
        mk_add_execution_in_database,
        mk_sync_document,
    ):
        mk_sync_document.return_value = ({}, [])

        apply_document_change(**self.kwargs)

        mk_sync_document.assert_called_once_with(
            mk_get_documents_in_database.return_value
        )

    def test_adds_execution_in_database(
        self,
        mk_get_documents_in_database,
        mk_add_execution_in_database,
        mk_sync_document,
    ):
        executions = [
            {
                "package_name": "package-1.zip",
                "file_name": "document.xml",
                "failed": False,
                "deletion": True,
                "pid": "pid-v3-1",
                "payload": {"scielo_id": "pid-v3-1"},
            },
            {
                "package_name": "package-1.zip",
                "file_name": "document.xml",
                "failed": True,
                "error": "Connection error",
                "deletion": False,
                "pid": "pid-v3-1",
            },
        ]
        mk_sync_document.return_value = {}, executions
        updated_executions = deepcopy(executions)
        for execution in updated_executions:
            execution.update({
                "dag_run": self.kwargs["run_id"],
                "pre_sync_dag_run": self.kwargs["dag_run"].conf["pre_syn_dag_run_id"],
            })

        apply_document_change(**self.kwargs)

        mk_add_execution_in_database.assert_has_calls([
            call(table="xml_documents", data=execution)
            for execution in updated_executions
        ])

    def test_does_not_push_if_no_documents_into_kernel(
        self,
        mk_get_documents_in_database,
        mk_add_execution_in_database,
        mk_sync_document,
    ):
        mk_sync_document.return_value = {}, []
        ret = apply_document_change(**self.kwargs)

        self.assertFalse(ret)
        self.kwargs["ti"].xcom_push.assert_not_called()

    def test_pushes_document_data(
        self,
        mk_get_documents_in_database,
        mk_add_execution_in_database,
        mk_sync_document,
    ):
        document_data = {
            "scielo_id": "pid-v3-1",
            "deletion": False,
        }
        mk_sync_document.return_value = document_data, []
        ret = apply_document_change(**self.kwargs)

        self.assertTrue(ret)
        self.kwargs["ti"].xcom_push.assert_called_once_with(
            key="document_data", value=document_data
        )


@patch(
    "sync_docs_to_kernel.sync_documents_to_kernel_operations.update_document_in_bundle"
)
@patch("sync_docs_to_kernel.add_execution_in_database")
class TestUpdateDocumentInBundle(TestCase):
    def setUp(self):
        self.kwargs = {"ti": MagicMock(), "dag_run": MagicMock(), "run_id": "test_run_id"}
        self.kwargs["dag_run"].conf = {"pre_syn_dag_run_id": "test_pre_run_id"}
        self.kwargs["ti"].xcom_pull.side_effect = [
            {"scielo_id": "pid-v3-1", "deletion": False},
            "/json/title.json",
        ]

    def test_calls_update_document_in_bundle(
        self, mk_add_execution_in_database, mk_update_document_in_bundle
    ):
        mk_update_document_in_bundle.return_value = {}, []
        update_document_in_bundle(**self.kwargs)

        mk_update_document_in_bundle.assert_called_once_with(
            {"scielo_id": "pid-v3-1", "deletion": False},
            "/json/title.json",
        )

    def test_adds_execution_in_database(
        self, mk_add_execution_in_database, mk_update_document_in_bundle
    ):
        executions = [
            {
                "package_name": "package-1.zip",
                "pid": "pid-v3-1",
                "bundle_id": "0101-0101-2020-v1-n1",
            },
            {
                "pid": "pid-v3-1",
                "bundle_id": "0101-0101-2020-v1-n1",
                "ex_ahead": True,
                "removed": True,
            },
        ]
        mk_update_document_in_bundle.return_value = {}, executions
        updated_executions = deepcopy(executions)
        for execution in updated_executions:
            execution.update({
                "dag_run": self.kwargs["run_id"],
                "pre_sync_dag_run": self.kwargs["dag_run"].conf["pre_syn_dag_run_id"],
            })

        update_document_in_bundle(**self.kwargs)

        mk_add_execution_in_database.assert_has_calls([
            call(table="xml_documentsbundle", data=execution)
            for execution in updated_executions
        ])

    def test_pushes_document_data(
        self, mk_add_execution_in_database, mk_update_document_in_bundle
    ):
        result = {
            "scielo_id": "pid-v3-1",
            "deletion": False,
        }
        mk_update_document_in_bundle.return_value = result, []
        update_document_in_bundle(**self.kwargs)

        self.kwargs["ti"].xcom_push.assert_called_once_with(
            key="result", value=result
        )


if __name__ == "__main__":
    main()
