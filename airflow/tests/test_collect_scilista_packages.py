import tempfile
import shutil
import pathlib
from unittest import TestCase, main
from unittest.mock import patch, MagicMock, ANY

from airflow import DAG

from collect_scilista_packages import (
    get_sps_packages,
    start_sync_packages,
)


class TestGetSPSPackages(TestCase):
    def setUp(self):
        self.dir_source = tempfile.mkdtemp()
        self.dir_dest = tempfile.mkdtemp()
        self.id_proc_gerapadrao = "2020-01-01-16412600000000"
        self._scilista_basename = f"scilista-{self.id_proc_gerapadrao}.lst"
        self._scilista_path = pathlib.Path(self.dir_source) / self._scilista_basename
        self._scilista_path.write_text("package 01")
        self.mk_dag_run = MagicMock(conf={"GERAPADRAO_ID": self.id_proc_gerapadrao})
        self.kwargs = {
            "ti": MagicMock(),
            "dag_run": self.mk_dag_run,
            "run_id": "test_run_id",
        }

    def tearDown(self):
        shutil.rmtree(self.dir_source)
        shutil.rmtree(self.dir_dest)

    @patch("collect_scilista_packages.Variable.get")
    def test_get_sps_packages_raises_error_if_no_xc_dir_from_variable(
        self, mk_variable_get
    ):
        mk_variable_get.side_effect = KeyError
        self.assertRaises(KeyError, get_sps_packages, **self.kwargs)

    @patch("collect_scilista_packages.pre_sync_documents_to_kernel_operations.get_sps_packages_on_scilista")
    @patch("collect_scilista_packages.Variable.get")
    def test_get_sps_packages_returns_true_to_execute_trigger_dags(
        self, mk_variable_get, mk_get_sps_packages_on_scilista
    ):
        mk_variable_get.side_effect = [
            self.dir_source,
            self.dir_dest,
            self.id_proc_gerapadrao,
        ]
        _sps_packages = ["package_01", "package_02", "package_03"]
        mk_get_sps_packages_on_scilista.return_value = _sps_packages
        _exec_start_sync_packages = get_sps_packages(**self.kwargs)
        self.kwargs["ti"].xcom_push.assert_called_once_with(
            key='sps_packages', value=_sps_packages
        )
        self.assertTrue(_exec_start_sync_packages)

    @patch("collect_scilista_packages.pre_sync_documents_to_kernel_operations.get_sps_packages_on_scilista")
    @patch("collect_scilista_packages.Variable.get")
    def test_get_sps_packages_returns_false_to_execute_trigger_dags(
        self, mk_variable_get, mk_get_sps_packages_on_scilista
    ):
        mk_variable_get.side_effect = [
            self.dir_source,
            self.dir_dest,
            self.id_proc_gerapadrao,
        ]
        mk_get_sps_packages_on_scilista.return_value = []
        _exec_start_sync_packages = get_sps_packages(**self.kwargs)
        self.kwargs["ti"].xcom_push.assert_not_called()
        self.assertFalse(_exec_start_sync_packages)


class TestStartSyncPackages(TestCase):
    @patch("collect_scilista_packages.Variable.get")
    @patch("collect_scilista_packages.trigger_dag")
    def test_start_sync_packages_calls_trigger_dag_for_each_package(
        self, mk_trigger_dag, mk_variable_get
    ):
        mk_dag_run = MagicMock(conf={"GERAPADRAO_ID": "2020-01-01-16412600000000"})
        kwargs = {"ti": MagicMock(), "dag_run": mk_dag_run, "run_id": "run_id-1234"}
        mk_sps_packages = {
            "abc_v50": ["dir/destination/abc_v50.zip"],
            "rba_2018nahead": ["dir/destination/rba_2018nahead.zip"],
            "rba_v53n1": ["dir/destination/rba_v53n1.zip"],
            "rsp_v10n2-3": ["dir/destination/rsp_v10n2-3.zip"],
        }
        kwargs["ti"].xcom_pull.return_value = mk_sps_packages
        start_sync_packages(**kwargs)
        for bundle_label, sps_packages in mk_sps_packages.items():
            with self.subTest(bundle_label=bundle_label, sps_packages=sps_packages):
                mk_trigger_dag.assert_any_call(
                    dag_id="extract_packages_info",
                    run_id=ANY,
                    execution_date=ANY,
                    replace_microseconds=False,
                    conf={
                        "bundle_label": bundle_label,
                        "sps_packages": sps_packages,
                        "pre_syn_dag_run_id": "run_id-1234",
                        "gerapadrao_id": "2020-01-01-16412600000000",
                    },
                )


if __name__ == "__main__":
    main()
