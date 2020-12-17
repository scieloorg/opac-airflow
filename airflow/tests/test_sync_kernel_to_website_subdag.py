import unittest
from datetime import timedelta
from unittest.mock import MagicMock, patch, call, ANY

from airflow import DAG

from subdags.sync_kernel_to_website_subdag import (
    _group_documents_by_bundle,
    create_subdag_to_register_documents_grouped_by_bundle,
    finish,
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

    def test__group_documents_by_bundle_create_group_with_orphans_docs(self):
        def mock_get_relation_data(doc_id):
            return (None, {})

        document_ids = (
            "XXX3V9MHSKmp6Msj5CPBZRb",
        )
        expected = {None: ["XXX3V9MHSKmp6Msj5CPBZRb"]}
        result = _group_documents_by_bundle(
            document_ids, mock_get_relation_data)
        self.assertEqual(expected, result)


@patch("subdags.sync_kernel_to_website_subdag.finish")
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
            mock_finish,
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
        mock_get_data = MagicMock()
        mock_get_data.return_value = [
            document_ids,
            renditions_documents_id,
            self.mock_get_relation_data_which_returns_none
        ]
        create_subdag_to_register_documents_grouped_by_bundle(
            self.mock_dag,
            self.mock_register_docs_callable,
            self.mock_register_renditions_callable,
            mock_get_data,
            self.mock_args,
        )

        calls = [
            call(
                task_id="register_documents_groups_id_finish",
                provide_context=True,
                python_callable=mock_finish,
                op_args=([], ["XXX3V9MHSKmp6Msj5CPBZRb"], ["XXX3V9MHSKmp6Msj5CPBZRb"]),
                dag=mock_subdag,
            ),
        ]
        self.assertEqual(calls, mock_python_op.call_args_list)

    def test_create_subdag_to_register_documents_grouped_by_bundle_creates_subdag_with_two_tasks(self,
            mock_python_op, MockSubDAG,
            mock_finish,
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
        mock_get_data = MagicMock()
        mock_get_data.return_value = [
            document_ids,
            renditions_documents_id,
            self.mock_get_relation_data
        ]
        create_subdag_to_register_documents_grouped_by_bundle(
            self.mock_dag,
            self.mock_register_docs_callable,
            self.mock_register_renditions_callable,
            mock_get_data,
            self.mock_args,
        )

        calls = [
            call(
                task_id='register_documents_groups_id_finish',
                provide_context=True,
                python_callable=mock_finish,
                op_args=(["issue_id_2", "issue_id"], [], []),
                dag=mock_subdag,
            ),
            call(
                task_id='register_documents_groups_id_issue_id_2_docs',
                python_callable=self.mock_register_docs_callable,
                op_args=(
                    ["LL13V9MHSKmp6Msj5CPBZRb"],
                    self.mock_get_relation_data
                ),
                retries=1,
                retry_delay=timedelta(minutes=1),
                dag=mock_subdag,
            ),
            call(
                task_id='register_documents_groups_id_issue_id_docs',
                python_callable=self.mock_register_docs_callable,
                op_args=(
                    ["HJgFV9MHSKmp6Msj5CPBZRb", "CGgFV9MHSKmp6Msj5CPBZRb"],
                    self.mock_get_relation_data
                ),
                retries=1,
                retry_delay=timedelta(minutes=1),
                dag=mock_subdag,
            ),
        ]
        self.assertListEqual(calls, mock_python_op.call_args_list)

    def test_create_subdag_to_register_documents_grouped_by_bundle_creates_subdag_with_two_tasks_to_register_docs_and_other_two_to_register_renditions(self,
            mock_python_op, MockSubDAG,
            mock_finish,
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
        mock_get_data = MagicMock()
        mock_get_data.return_value = [
            document_ids,
            renditions_documents_id,
            self.mock_get_relation_data
        ]
        create_subdag_to_register_documents_grouped_by_bundle(
            self.mock_dag,
            self.mock_register_docs_callable,
            self.mock_register_renditions_callable,
            mock_get_data,
            self.mock_args,
            )

        calls = [
            call(
                task_id='register_documents_groups_id_finish',
                provide_context=True,
                python_callable=mock_finish,
                op_args=(["issue_id_2", "issue_id"], [], []),
                dag=mock_subdag,
            ),
            call(
                task_id='register_documents_groups_id_issue_id_2_docs',
                python_callable=self.mock_register_docs_callable,
                op_args=(
                    ["LL13V9MHSKmp6Msj5CPBZRb"],
                    self.mock_get_relation_data
                ),
                retries=1,
                retry_delay=timedelta(minutes=1),
                dag=mock_subdag,
            ),
            call(
                task_id='register_documents_groups_id_issue_id_2_renditions',
                python_callable=self.mock_register_renditions_callable,
                op_kwargs=(
                    {"renditions_to_get": {"LL13V9MHSKmp6Msj5CPBZRb"}}
                ),
                retries=1,
                retry_delay=timedelta(minutes=1),
                dag=mock_subdag,
            ),

            call(
                task_id='register_documents_groups_id_issue_id_docs',
                python_callable=self.mock_register_docs_callable,
                op_args=(
                    ["HJgFV9MHSKmp6Msj5CPBZRb", "CGgFV9MHSKmp6Msj5CPBZRb"],
                    self.mock_get_relation_data
                ),
                retries=1,
                retry_delay=timedelta(minutes=1),
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
                retries=1,
                retry_delay=timedelta(minutes=1),
                dag=mock_subdag,
            ),
        ]
        self.assertListEqual(calls, mock_python_op.call_args_list)


    def test_create_subdag_to_register_documents_grouped_by_bundle_creates_subdag_with_task_to_register_renditions(self,
            mock_python_op, MockSubDAG,
            mock_finish,
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
        mock_get_data = MagicMock()
        mock_get_data.return_value = [
            document_ids,
            renditions_documents_id,
            self.mock_get_relation_data
        ]
        create_subdag_to_register_documents_grouped_by_bundle(
            self.mock_dag,
            self.mock_register_docs_callable,
            self.mock_register_renditions_callable,
            mock_get_data,
            self.mock_args,
            )

        calls = [
            call(
                task_id='register_documents_groups_id_finish',
                provide_context=True,
                python_callable=mock_finish,
                op_args=([], [], []),
                dag=mock_subdag,
            ),
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


class TestFinish(unittest.TestCase):

    def setUp(self):
        self.kwargs = {"ti": MagicMock()}

    @patch("subdags.sync_kernel_to_website_subdag.Variable.set")
    def test_finish_set_no_orphans(self, mock_set):
        bundles = ['issue_id_1', 'issue_id_2']
        orphan_documents = []
        orphan_renditions = {}

        issue_id_1__orphan_docs = None
        issue_id_1__orphan_rends = None
        issue_id_2__orphan_docs = None
        issue_id_2__orphan_rends = None
        _orphan_rends = None

        self.kwargs["ti"].xcom_pull.side_effect = [
            issue_id_1__orphan_docs,
            issue_id_2__orphan_docs,
            issue_id_1__orphan_rends,
            issue_id_2__orphan_rends,
            _orphan_rends,
        ]
        finish(bundles, orphan_documents, orphan_renditions, **self.kwargs)
        mock_set.assert_not_called()

    @patch("subdags.sync_kernel_to_website_subdag.Variable.set")
    def test_finish_set_no_bundles_and_no_orphans(self, mock_set):
        bundles = []
        orphan_documents = []
        orphan_renditions = {}

        issue_id_1__orphan_docs = None
        issue_id_1__orphan_rends = None
        issue_id_2__orphan_docs = None
        issue_id_2__orphan_rends = None
        _orphan_rends = None

        self.kwargs["ti"].xcom_pull.side_effect = [
            issue_id_1__orphan_docs,
            issue_id_2__orphan_docs,
            issue_id_1__orphan_rends,
            issue_id_2__orphan_rends,
            _orphan_rends,
        ]
        finish(bundles, orphan_documents, orphan_renditions, **self.kwargs)
        mock_set.assert_not_called()

    @patch("subdags.sync_kernel_to_website_subdag.Variable.set")
    def test_finish_set_no_bundles_and_orphan_docs(self, mock_set):
        bundles = []
        orphan_documents = ['LL13V9MHSKmp6Msj5CPBZRb']
        orphan_renditions = {}

        issue_id_1__orphan_docs = None
        issue_id_1__orphan_rends = None
        issue_id_2__orphan_docs = None
        issue_id_2__orphan_rends = None
        _orphan_rends = None

        self.kwargs["ti"].xcom_pull.side_effect = [
            issue_id_1__orphan_docs,
            issue_id_2__orphan_docs,
            issue_id_1__orphan_rends,
            issue_id_2__orphan_rends,
            _orphan_rends,
        ]
        finish(bundles, orphan_documents, orphan_renditions, **self.kwargs)
        mock_set.assert_called_once_with(
            "orphan_documents",
            ['LL13V9MHSKmp6Msj5CPBZRb'], serialize_json=True)

    @patch("subdags.sync_kernel_to_website_subdag.Variable.set")
    def test_finish_set_no_bundles_and_orphan_docs_and_rends(self, mock_set):
        bundles = []
        orphan_documents = ['LL13V9MHSKmp6Msj5CPBZRb']
        orphan_renditions = {'LL13V9MHSKmp6Msj5CPBZRb'}

        issue_id_1__orphan_docs = None
        issue_id_1__orphan_rends = None
        issue_id_2__orphan_docs = None
        issue_id_2__orphan_rends = None
        _orphan_rends = None

        self.kwargs["ti"].xcom_pull.side_effect = [
            issue_id_1__orphan_docs,
            issue_id_2__orphan_docs,
            issue_id_1__orphan_rends,
            issue_id_2__orphan_rends,
            _orphan_rends,
        ]
        finish(bundles, orphan_documents, orphan_renditions, **self.kwargs)
        calls = [
            call("orphan_documents",
                 ['LL13V9MHSKmp6Msj5CPBZRb'], serialize_json=True),
            call("orphan_renditions",
                 ['LL13V9MHSKmp6Msj5CPBZRb'], serialize_json=True),
        ]
        mock_set.assert_has_calls(calls)

    @patch("subdags.sync_kernel_to_website_subdag.Variable.set")
    def test_finish_set_no_bundles_and_orphan_rends(self, mock_set):
        bundles = []
        orphan_documents = []
        orphan_renditions = {'LL13V9MHSKmp6Msj5CPBZRb'}

        issue_id_1__orphan_docs = None
        issue_id_1__orphan_rends = None
        issue_id_2__orphan_docs = None
        issue_id_2__orphan_rends = None
        _orphan_rends = None

        self.kwargs["ti"].xcom_pull.side_effect = [
            issue_id_1__orphan_docs,
            issue_id_2__orphan_docs,
            issue_id_1__orphan_rends,
            issue_id_2__orphan_rends,
            _orphan_rends,
        ]
        finish(bundles, orphan_documents, orphan_renditions, **self.kwargs)
        mock_set.assert_called_once_with(
            "orphan_renditions",
            ['LL13V9MHSKmp6Msj5CPBZRb'], serialize_json=True)

    @patch("subdags.sync_kernel_to_website_subdag.Variable.set")
    def test_finish_all_have_orphans(self, mock_set):
        bundles = ['issue_id_1', 'issue_id_2']
        orphan_documents = ['a113V9MHSKmp6Msj5CPBZRb']
        orphan_renditions = [
            'a2gFV9MHSKmp6Msj5CPBZRb', 'a3a3V9MHSKmp6Msj5CPBZRb']

        issue_id_1__orphan_docs = [
            'I1gFV9MHSKmp6Msj5CPBZRb', 'I113V9MHSKmp6Msj5CPBZRb']
        issue_id_1__orphan_rends = [
            'I1gFV9MHSKmp6Msj5CPBZRb', 'I113V9MHSKmp6Msj5CPBZRb']
        issue_id_2__orphan_docs = [
            'i213V9MHSKmp6Msj5CPBZRb', 'i203V9MHSKmp6Msj5CPBZRb']
        issue_id_2__orphan_rends = [
            'i213V9MHSKmp6Msj5CPBZRb', 'i203V9MHSKmp6Msj5CPBZRb']
        _orphan_rends = ['I313V9MHSKmp6Msj5CPBZRb', 'I313xxxxxKmp6Msj5CPBZRb']

        self.kwargs["ti"].xcom_pull.side_effect = [
            issue_id_1__orphan_docs,
            issue_id_2__orphan_docs,
            issue_id_1__orphan_rends,
            issue_id_2__orphan_rends,
            _orphan_rends,
        ]

        expected_orphan_documents = [
            'a113V9MHSKmp6Msj5CPBZRb',
            'I1gFV9MHSKmp6Msj5CPBZRb', 'I113V9MHSKmp6Msj5CPBZRb',
            'i213V9MHSKmp6Msj5CPBZRb', 'i203V9MHSKmp6Msj5CPBZRb'
        ]
        expected_orphan_renditions = [
            'a2gFV9MHSKmp6Msj5CPBZRb', 'a3a3V9MHSKmp6Msj5CPBZRb',
            'I1gFV9MHSKmp6Msj5CPBZRb', 'I113V9MHSKmp6Msj5CPBZRb',
            'i213V9MHSKmp6Msj5CPBZRb', 'i203V9MHSKmp6Msj5CPBZRb',
            'I313V9MHSKmp6Msj5CPBZRb', 'I313xxxxxKmp6Msj5CPBZRb',
        ]

        finish(bundles, orphan_documents, orphan_renditions, **self.kwargs)
        calls = [
            call("orphan_documents",
                 expected_orphan_documents, serialize_json=True),
            call("orphan_renditions",
                 expected_orphan_renditions, serialize_json=True),
        ]
        self.assertListEqual(calls, mock_set.call_args_list)

    @patch("subdags.sync_kernel_to_website_subdag.Variable.set")
    def test_finish_one_bundle_has_no_orphans(self, mock_set):
        bundles = ['issue_id_1', 'issue_id_2']
        orphan_documents = ['a113V9MHSKmp6Msj5CPBZRb']
        orphan_renditions = [
            'a2gFV9MHSKmp6Msj5CPBZRb', 'a3a3V9MHSKmp6Msj5CPBZRb']

        issue_id_1__orphan_docs = None
        issue_id_1__orphan_rends = None
        issue_id_2__orphan_docs = [
            'i213V9MHSKmp6Msj5CPBZRb', 'i203V9MHSKmp6Msj5CPBZRb']
        issue_id_2__orphan_rends = [
            'i213V9MHSKmp6Msj5CPBZRb', 'i203V9MHSKmp6Msj5CPBZRb']
        _orphan_rends = ['I313V9MHSKmp6Msj5CPBZRb', 'I313xxxxxKmp6Msj5CPBZRb']

        self.kwargs["ti"].xcom_pull.side_effect = [
            issue_id_1__orphan_docs,
            issue_id_2__orphan_docs,
            issue_id_1__orphan_rends,
            issue_id_2__orphan_rends,
            _orphan_rends,
        ]

        expected_orphan_documents = [
            'a113V9MHSKmp6Msj5CPBZRb',
            'i213V9MHSKmp6Msj5CPBZRb', 'i203V9MHSKmp6Msj5CPBZRb'
        ]
        expected_orphan_renditions = [
            'a2gFV9MHSKmp6Msj5CPBZRb', 'a3a3V9MHSKmp6Msj5CPBZRb',
            'i213V9MHSKmp6Msj5CPBZRb', 'i203V9MHSKmp6Msj5CPBZRb',
            'I313V9MHSKmp6Msj5CPBZRb', 'I313xxxxxKmp6Msj5CPBZRb',
        ]

        finish(bundles, orphan_documents, orphan_renditions, **self.kwargs)
        calls = [
            call("orphan_documents",
                 expected_orphan_documents, serialize_json=True),
            call("orphan_renditions",
                 expected_orphan_renditions, serialize_json=True),
        ]
        self.assertListEqual(calls, mock_set.call_args_list)
