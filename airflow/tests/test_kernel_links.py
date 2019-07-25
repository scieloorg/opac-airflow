from unittest import TestCase
from unittest.mock import MagicMock
from airflow import DAG
from kernel_links import fetch_previous_context


class TestFetchPreviousIssueContext(TestCase):

    def test_should_raise_keyerror_when_dag_run_does_not_passed(self):
        kwargs = {}
        self.assertRaises(KeyError, fetch_previous_context, **kwargs)

    def test_should_not_have_issues_in_context_if_conf_is_none(self):
        dag_run = MagicMock()
        dag_run.conf = None
        kwargs = {"dag_run": dag_run, "ti": MagicMock()}
        fetch_previous_context(**kwargs)
        kwargs["ti"].xcom_push.assert_not_called()

    def test_should_have_same_issues_in_context_and_conf(self):
        dag_run = MagicMock()
        dag_run.conf = {"issues": [{"id": "issue-1"}]}
        kwargs = {"dag_run": dag_run, "ti": MagicMock()}
        fetch_previous_context(**kwargs)
        kwargs["ti"].xcom_push.assert_called_once_with(
            key="issues", value=[{"id": "issue-1"}]
        )
