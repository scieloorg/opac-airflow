from unittest import TestCase, main
from unittest.mock import patch, MagicMock, ANY

from airflow import DAG

from pre_packages_process import get_sps_packages


class TestGetSPSPackages(TestCase):
    @patch("pre_packages_process.Variable.get")
    def test_get_sps_packages_raises_error_if_no_xc_dir_from_variable(
        self, mk_variable_get
    ):
        mk_variable_get.side_effect = KeyError
        self.assertRaises(KeyError, get_sps_packages, None)

    @patch("pre_packages_process.pre_packages_operations.get_sps_packages")
    @patch("pre_packages_process.Variable.get")
    @patch("pre_packages_process.trigger_dag")
    def test_get_sps_packages_calls_get_sps_packages_operation(
        self, mk_trigger_dag, mk_variable_get, mk_get_sps_packages
    ):

        mk_variable_get.side_effect = [
            "dir/path/scilista.lst",
            "dir/source",
            "dir/destination",
        ]
        kwargs = {"ti": MagicMock(), "conf": None}
        get_sps_packages(**kwargs)
        mk_get_sps_packages.assert_called_once_with(
            "dir/path/scilista.lst", "dir/source", "dir/destination"
        )

    @patch("pre_packages_process.pre_packages_operations.get_sps_packages")
    @patch("pre_packages_process.Variable.get")
    @patch("pre_packages_process.trigger_dag")
    def test_get_sps_packages_calls_trigger_dag_for_each_package(
        self, mk_trigger_dag, mk_variable_get, mk_get_sps_packages
    ):
        kwargs = {"ti": MagicMock(), "conf": None}
        mk_sps_packages = [
            "dir/destination/abc_v50.zip",
            "dir/destination/rba_2018nahead.zip",
            "dir/destination/rba_v53n1.zip",
            "dir/destination/rsp_v10n2-3.zip",
        ]
        mk_get_sps_packages.return_value = mk_sps_packages
        get_sps_packages(**kwargs)
        for mk_sps_package in mk_sps_packages:
            with self.subTest(mk_sps_package=mk_sps_package):
                mk_trigger_dag.assert_any_call(
                    dag_id="package_process",
                    run_id=ANY,
                    execution_date=ANY,
                    replace_microseconds=False,
                    conf={"sps_package": mk_sps_package},
                )


if __name__ == "__main__":
    main()
