import unittest
from unittest.mock import MagicMock, patch, call, ANY

from airflow import DAG

from subdags.sync_kernel_to_website_subdag import (
    _group_documents_by_bundle,
    create_subdag_to_register_documents_grouped_by_bundle,
)


class TestGroupDocumentsByBundle(unittest.TestCase):

    def mock_get_relation_data(self, doc_id):
        data = {
            "RCgFV9MHSKmp6Msj5CPBZRb": (
                "issue_id",
                {"id": "RCgFV9MHSKmp6Msj5CPBZRb", "order": "00602"},
            ),
            "CGgFV9MHSKmp6Msj5CPBZRb": (
                "issue_id",
                {"id": "CGgFV9MHSKmp6Msj5CPBZRb", "order": "00604"},
            ),
            "HJgFV9MHSKmp6Msj5CPBZRb": (
                "issue_id",
                {"id": "HJgFV9MHSKmp6Msj5CPBZRb", "order": "00607"},
            ),
            "LLgFV9MHSKmp6Msj5CPBZRb": (
                "issue_id",
                {"id": "LLgFV9MHSKmp6Msj5CPBZRb", "order": "00609"},
            ),
            "RC13V9MHSKmp6Msj5CPBZRb": (
                "issue_id_2",
                {"id": "RC13V9MHSKmp6Msj5CPBZRb", "order": "00602"},
            ),
            "CG13V9MHSKmp6Msj5CPBZRb": (
                "issue_id_2",
                {"id": "CG13V9MHSKmp6Msj5CPBZRb", "order": "00604"},
            ),
            "HJ13V9MHSKmp6Msj5CPBZRb": (
                "issue_id_2",
                {"id": "HJ13V9MHSKmp6Msj5CPBZRb", "order": "00607"},
            ),
            "LL13V9MHSKmp6Msj5CPBZRb": (
                "issue_id_2",
                {"id": "LL13V9MHSKmp6Msj5CPBZRb", "order": "00609"},
            ),
        }
        return data.get(doc_id)

    def test__group_documents_by_bundle(self):
        document_ids = (
            "LL13V9MHSKmp6Msj5CPBZRb",
            "HJgFV9MHSKmp6Msj5CPBZRb",
            "CGgFV9MHSKmp6Msj5CPBZRb",
        )

        expected = {
            "issue_id_2": ["LL13V9MHSKmp6Msj5CPBZRb"],
            "issue_id": ["HJgFV9MHSKmp6Msj5CPBZRb", "CGgFV9MHSKmp6Msj5CPBZRb"],
        }
        result = _group_documents_by_bundle(
            document_ids, self.mock_get_relation_data)
        self.assertEqual(expected, result)

    def test__group_documents_by_bundle_returns_empty_dict(self):
        def mock_get_relation_data(doc_id):
            return (None, {})

        document_ids = (
            "XXX3V9MHSKmp6Msj5CPBZRb",
        )
        expected = {}
        result = _group_documents_by_bundle(
            document_ids, mock_get_relation_data)
        self.assertEqual(expected, result)


@patch("subdags.sync_kernel_to_website_subdag.DAG")
@patch("subdags.sync_kernel_to_website_subdag.PythonOperator")
class TestCreateSubdagToRegisterDocumentsGroupedByBundle(unittest.TestCase):

    def mock_get_relation_data(self, doc_id):
        data = {
            "CGgFV9MHSKmp6Msj5CPBZRb": (
                "issue_id",
                {"id": "CGgFV9MHSKmp6Msj5CPBZRb", "order": "00604"},
            ),
            "HJgFV9MHSKmp6Msj5CPBZRb": (
                "issue_id",
                {"id": "HJgFV9MHSKmp6Msj5CPBZRb", "order": "00607"},
            ),
            "LL13V9MHSKmp6Msj5CPBZRb": (
                "issue_id_2",
                {"id": "LL13V9MHSKmp6Msj5CPBZRb", "order": "00609"},
            ),
        }
        return data.get(doc_id)

    def mock_get_relation_data_which_returns_none(self, doc_id):
        return (None, {})

    def setUp(self):
        MockDAG = MagicMock(spec=DAG)

        self.mock_dag = MockDAG(ANY)
        self.mock_register_docs_callable = MagicMock(spec=callable)
        self.mock_register_renditions_callable = MagicMock(spec=callable)
        self.mock_args = {}

    def test_create_subdag_to_register_documents_grouped_by_bundle_creates_subdag_with_dummy_task(self,
            mock_python_op, MockSubDAG,
            ):
        # mock de DAG
        MockDAG = MagicMock(spec=DAG)

        # instancia mock de DAG
        mock_subdag = MockDAG(spec=DAG)

        # mockSubDAG é mock da DAG que está em uso em
        # `create_subdag_to_register_documents_grouped_by_bundle`
        MockSubDAG.return_value = mock_subdag

        document_ids = ("XXX3V9MHSKmp6Msj5CPBZRb", )
        renditions_documents_id = ("XXX3V9MHSKmp6Msj5CPBZRb", )

        create_subdag_to_register_documents_grouped_by_bundle(
            self.mock_dag, self.mock_register_docs_callable,
            document_ids, self.mock_get_relation_data_which_returns_none,
            self.mock_register_renditions_callable, renditions_documents_id,
            self.mock_args,
            )

        calls = [
            call(
                task_id="register_documents_groups_id_do_nothing",
                python_callable=ANY,
                dag=mock_subdag,
            ),
        ]
        self.assertEqual(calls, mock_python_op.call_args_list)

    def test_create_subdag_to_register_documents_grouped_by_bundle_creates_subdag_with_two_tasks(self,
            mock_python_op, MockSubDAG,
            ):
        # mock de DAG
        MockDAG = MagicMock(spec=DAG)

        # instancia mock de DAG
        mock_subdag = MockDAG(spec=DAG)

        # mockSubDAG é mock da DAG que está em uso em
        # `create_subdag_to_register_documents_grouped_by_bundle`
        MockSubDAG.return_value = mock_subdag

        document_ids = [
            "LL13V9MHSKmp6Msj5CPBZRb",
            "HJgFV9MHSKmp6Msj5CPBZRb",
            "CGgFV9MHSKmp6Msj5CPBZRb"
        ]
        renditions_documents_id = []
        create_subdag_to_register_documents_grouped_by_bundle(
            self.mock_dag, self.mock_register_docs_callable,
            document_ids, self.mock_get_relation_data,
            self.mock_register_renditions_callable, renditions_documents_id,
            self.mock_args,
            )

        calls = [
            call(
                task_id='register_documents_groups_id_issue_id_2_docs',
                python_callable=self.mock_register_docs_callable,
                op_args=(
                    ["LL13V9MHSKmp6Msj5CPBZRb"],
                    self.mock_get_relation_data
                ),
                dag=mock_subdag,
            ),
            call(
                task_id='register_documents_groups_id_issue_id_docs',
                python_callable=self.mock_register_docs_callable,
                op_args=(
                    ["HJgFV9MHSKmp6Msj5CPBZRb", "CGgFV9MHSKmp6Msj5CPBZRb"],
                    self.mock_get_relation_data
                ),
                dag=mock_subdag,
            ),
        ]
        self.assertListEqual(calls, mock_python_op.call_args_list)

    def test_create_subdag_to_register_documents_grouped_by_bundle_creates_subdag_with_two_tasks_to_register_docs_and_other_two_to_register_renditions(self,
            mock_python_op, MockSubDAG,
            ):
        # mock de DAG
        MockDAG = MagicMock(spec=DAG)

        # instancia mock de DAG
        mock_subdag = MockDAG(spec=DAG)

        # mockSubDAG é mock da DAG que está em uso em
        # `create_subdag_to_register_documents_grouped_by_bundle`
        MockSubDAG.return_value = mock_subdag

        document_ids = [
            "LL13V9MHSKmp6Msj5CPBZRb",
            "HJgFV9MHSKmp6Msj5CPBZRb",
            "CGgFV9MHSKmp6Msj5CPBZRb"
        ]
        # nem todos os documents_id estão em ambas listas
        renditions_documents_id = [
            "LL13V9MHSKmp6Msj5CPBZRb",
            "HJgFV9MHSKmp6Msj5CPBZRb",
        ]
        create_subdag_to_register_documents_grouped_by_bundle(
            self.mock_dag, self.mock_register_docs_callable,
            document_ids, self.mock_get_relation_data,
            self.mock_register_renditions_callable, renditions_documents_id,
            self.mock_args,
            )

        calls = [
            call(
                task_id='register_documents_groups_id_issue_id_2_docs',
                python_callable=self.mock_register_docs_callable,
                op_args=(
                    ["LL13V9MHSKmp6Msj5CPBZRb"],
                    self.mock_get_relation_data
                ),
                dag=mock_subdag,
            ),
            call(
                task_id='register_documents_groups_id_issue_id_2_renditions',
                python_callable=self.mock_register_renditions_callable,
                op_kwargs=(
                    {"renditions_to_get": {"LL13V9MHSKmp6Msj5CPBZRb"}}
                ),
                dag=mock_subdag,
            ),

            call(
                task_id='register_documents_groups_id_issue_id_docs',
                python_callable=self.mock_register_docs_callable,
                op_args=(
                    ["HJgFV9MHSKmp6Msj5CPBZRb", "CGgFV9MHSKmp6Msj5CPBZRb"],
                    self.mock_get_relation_data
                ),
                dag=mock_subdag,
            ),
            call(
                task_id='register_documents_groups_id_issue_id_renditions',
                python_callable=self.mock_register_renditions_callable,
                op_kwargs=(
                    {
                        "renditions_to_get": {
                            "HJgFV9MHSKmp6Msj5CPBZRb",
                        }
                    }
                ),
                dag=mock_subdag,
            ),
        ]
        self.assertListEqual(calls, mock_python_op.call_args_list)


    def test_create_subdag_to_register_documents_grouped_by_bundle_creates_subdag_with_task_to_register_renditions(self,
            mock_python_op, MockSubDAG,
            ):
        # mock de DAG
        MockDAG = MagicMock(spec=DAG)

        # instancia mock de DAG
        mock_subdag = MockDAG(spec=DAG)

        # mockSubDAG é mock da DAG que está em uso em
        # `create_subdag_to_register_documents_grouped_by_bundle`
        MockSubDAG.return_value = mock_subdag

        # nem todos os documents_id estão em ambas listas
        document_ids = []
        renditions_documents_id = [
            "LL13V9MHSKmp6Msj5CPBZRb",
            "HJgFV9MHSKmp6Msj5CPBZRb",
        ]
        create_subdag_to_register_documents_grouped_by_bundle(
            self.mock_dag, self.mock_register_docs_callable,
            document_ids, self.mock_get_relation_data,
            self.mock_register_renditions_callable, renditions_documents_id,
            self.mock_args,
            )

        calls = [
            call(
                task_id='register_documents_groups_id_renditions',
                python_callable=self.mock_register_renditions_callable,
                op_kwargs=(
                    {
                        "renditions_to_get": {
                            "LL13V9MHSKmp6Msj5CPBZRb",
                            "HJgFV9MHSKmp6Msj5CPBZRb",
                        }
                    }
                ),
                dag=mock_subdag,
            ),
        ]
        self.assertListEqual(calls, mock_python_op.call_args_list)
