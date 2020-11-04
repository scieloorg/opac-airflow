import tempfile
import shutil
import pathlib
from unittest import TestCase, main
from unittest.mock import patch, MagicMock, ANY

from airflow import DAG

from pre_sync_documents_to_kernel import (
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
        self.kwargs = {
            "ti": MagicMock(),
            "conf": None,
            "run_id": "test_run_id",
        }

    def tearDown(self):
        shutil.rmtree(self.dir_source)
        shutil.rmtree(self.dir_dest)

    @patch("pre_sync_documents_to_kernel.Variable.get")
    def test_get_sps_packages_raises_error_if_no_xc_dir_from_variable(
        self, mk_variable_get
    ):
        mk_variable_get.side_effect = KeyError
        self.assertRaises(KeyError, get_sps_packages, None)

    @patch("pre_sync_documents_to_kernel.pre_sync_documents_to_kernel_operations.get_sps_packages")
    @patch("pre_sync_documents_to_kernel.Variable.get")
    def test_get_sps_packages_returns_true_to_execute_trigger_dags(
        self, mk_variable_get, mk_get_sps_packages
    ):
        mk_variable_get.side_effect = [
            self.dir_source,
            self.dir_dest,
            self.id_proc_gerapadrao,
        ]
        _sps_packages = ["package_01", "package_02", "package_03"]
        mk_get_sps_packages.return_value = _sps_packages
        _exec_start_sync_packages = get_sps_packages(**self.kwargs)
        self.kwargs["ti"].xcom_push.assert_called_once_with(
            key='sps_packages', value=_sps_packages
        )
        self.assertTrue(_exec_start_sync_packages)

    @patch("pre_sync_documents_to_kernel.pre_sync_documents_to_kernel_operations.get_sps_packages")
    @patch("pre_sync_documents_to_kernel.Variable.get")
    def test_get_sps_packages_returns_false_to_execute_trigger_dags(
        self, mk_variable_get, mk_get_sps_packages
    ):
        mk_variable_get.side_effect = [
            self.dir_source,
            self.dir_dest,
            self.id_proc_gerapadrao,
        ]
        mk_get_sps_packages.return_value = []
        _exec_start_sync_packages = get_sps_packages(**self.kwargs)
        self.kwargs["ti"].xcom_push.assert_not_called()
        self.assertFalse(_exec_start_sync_packages)


class TestStartSyncPackages(TestCase):
    @patch("pre_sync_documents_to_kernel.Variable.get")
    @patch("pre_sync_documents_to_kernel.trigger_dag")
    def test_start_sync_packages_calls_trigger_dag_for_each_package(
        self, mk_trigger_dag, mk_variable_get
    ):
        kwargs = {"ti": MagicMock(), "conf": None}
        mk_sps_packages = [
            "dir/destination/abc_v50.zip",
            "dir/destination/rba_2018nahead.zip",
            "dir/destination/rba_v53n1.zip",
            "dir/destination/rsp_v10n2-3.zip",
        ]
        kwargs["ti"].xcom_pull.return_value = mk_sps_packages
        start_sync_packages(**kwargs)
        for mk_sps_package in mk_sps_packages:
            with self.subTest(mk_sps_package=mk_sps_package):
                mk_trigger_dag.assert_any_call(
                    dag_id="sync_documents_to_kernel",
                    run_id=ANY,
                    execution_date=ANY,
                    replace_microseconds=False,
                    conf={"sps_package": mk_sps_package, "pre_syn_dag_run_id": ANY},
                )


if __name__ == "__main__":
    main()
