from unittest import TestCase
from unittest.mock import patch, MagicMock, call

from airflow import DAG
from airflow.utils.dates import days_ago

from subdags.check_website_subdags import (
    create_subdag_to_check_documents_deeply_grouped_by_issue_pid_v2,
)


class TestCreateSubdagToCheckDocumentsDeeplyGroupedByIssuePidV2(TestCase):

    @patch("subdags.check_website_subdags.DAG")
    @patch("subdags.check_website_subdags.PythonOperator")
    def test_create_subdag_to_check_documents_deeply_grouped_by_issue_pid_v2_(
            self, mock_python_op, mock_DAG):
        mock_DAG.return_value = MagicMock(spec=DAG)
        mock_dag = mock_DAG()

        default_args = {
            "owner": "airflow",
            "start_date": days_ago(2),
            "provide_context": True,
            "depends_on_past": False,
        }

        def _subdag_callable(uri_items, dag_run_data):
            return "do anything"

        def _group_callable(args, op=None):
            return {
                "0001-303520200005": [
                    "/scielo.php?script=sci_arttext"
                    "&pid=S0001-30352020000501101",
                ],
                "0203-199820200005": [
                    "/scielo.php?script=sci_arttext"
                    "&pid=S0203-19982020000501101",
                    "/scielo.php?script=sci_arttext"
                    "&pid=S0203-19982020000511111",
                ],
            }

        subdag_name = "check_documents_deeply_grouped_by_issue_pid_v2_id"
        create_subdag_to_check_documents_deeply_grouped_by_issue_pid_v2(
            mock_dag, _subdag_callable, _group_callable, default_args)
        calls = [
            call(task_id='{}_{}'.format(subdag_name, "0001-303520200005"),
                 python_callable=_subdag_callable,
                 op_args=(
                    ["/scielo.php?script=sci_arttext"
                     "&pid=S0001-30352020000501101"],
                    {}
                 ),
                 dag=mock_dag),
            call(task_id='{}_{}'.format(subdag_name, "0203-199820200005"),
                 python_callable=_subdag_callable,
                 op_args=(
                    ["/scielo.php?script=sci_arttext"
                     "&pid=S0203-19982020000501101",
                     "/scielo.php?script=sci_arttext"
                     "&pid=S0203-19982020000511111",
                     ],
                    {}
                    ),
                 dag=mock_dag),
        ]
        self.assertListEqual(calls, mock_python_op.call_args_list)

