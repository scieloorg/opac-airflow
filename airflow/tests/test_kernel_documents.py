import os
import shutil
import tempfile
from datetime import datetime
from unittest import main, TestCase, TestLoader, TextTestRunner
from unittest.mock import patch, MagicMock

from airflow import DAG

from kernel_documents import get_sps_packages


class TestGetSPSPackages(TestCase):

    def setUp(self):
        self.test_environ = {
            "XC_SPS_PACKAGES_DIR": "dir/source",
            "PROC_SPS_PACKAGES_DIR": "dir/destination",
        }

    @patch('kernel_documents.Path')
    @patch('kernel_documents.Variable.get')
    def test_get_sps_packages_xc_dir_from_env_var_is_None(self, mk_variable_get,
                                                          MockPath):
        mk_variable_get.return_value = None
        get_sps_packages(**{"scilista": ["rba v53n1"]})
        MockPath.assert_not_called()

    @patch('kernel_documents.Path')
    @patch('kernel_documents.Variable.get')
    def test_get_sps_packages_gets_xc_dir_from_env_var(self, mk_variable_get,
                                                       MockPath):

        mk_variable_get.side_effect = ["dir/source", "dir/destination"]
        get_sps_packages(**{"scilista": ["rba v53n1"]})
        MockPath.assert_any_call("dir/source")
        MockPath.assert_any_call("dir/destination")

    @patch('kernel_documents.shutil')
    @patch('kernel_documents.Variable.get')
    def test_get_sps_packages_moves_from_xc_dir_if_xc_source_exists(self,
                                                                    mk_variable_get,
                                                                    mk_shutil):
        mk_variable_get.return_value = None
        get_sps_packages(**{"scilista": {"rba": "v53n2"}})
        mk_shutil.move.assert_not_called()

    @patch('kernel_documents.shutil')
    @patch('kernel_documents.os.path.exists')
    @patch('kernel_documents.Variable.get')
    def test_get_sps_packages_moves_from_xc_dir_to_proc_dir(self,
                                                            mk_variable_get,
                                                            mk_path_exists,
                                                            mk_shutil):
        test_dest_dir_name = "sps_packages"
        mk_path_exists.return_value = True

        with tempfile.TemporaryDirectory() as tmpdirname:
            test_paths = [
                "../fixtures",
                tmpdirname,
            ]
            mk_variable_get.side_effect = test_paths
            kwargs = {
                "scilista": ["rba v53n1"],
                "ti": MagicMock(),
            }
            get_sps_packages(**kwargs)
            mk_shutil.move.assert_called_once_with(
                "../fixtures/rba_v53n1.zip",
                tmpdirname + "/rba_v53n1.zip"
            )

    @patch('kernel_documents.shutil')
    @patch('kernel_documents.os.path.exists')
    @patch('kernel_documents.Variable.get')
    def test_get_sps_packages_doesnt_call_ti_xcom_push_if_no_sps_packages_list(
        self,  mk_variable_get, mk_path_exists, mk_shutil
    ):
        mk_path_exists.return_value = False
        mk_variable_get.return_value = None
        kwargs = {
            "scilista": ["rba v53n1"],
            "ti": MagicMock(),
        }
        get_sps_packages(**kwargs)
        kwargs["ti"].xcom_push.assert_not_called()

    @patch('kernel_documents.shutil')
    @patch('kernel_documents.os.path.exists')
    @patch('kernel_documents.Variable.get')
    def test_get_sps_packages_calls_ti_xcom_push_with_sps_packages_list(
        self, mk_variable_get, mk_path_exists, mk_shutil
    ):
        mk_path_exists.return_value = True
        mk_variable_get.return_value = "dir/destination/"
        kwargs = {
            "scilista": [
                "rba v53n1",
                "rba 2018nahead",
                "rsp v10n2-3",
                "abc v50"
            ]
        }
        kwargs["ti"] = MagicMock()
        get_sps_packages(**kwargs)
        kwargs["ti"].xcom_push.assert_called_once_with(
            key="sps_packages",
            value=[
                "dir/destination/abc_v50.zip",
                "dir/destination/rba_2018nahead.zip",
                "dir/destination/rba_v53n1.zip",
                "dir/destination/rsp_v10n2-3.zip",
            ]
        )


suite = TestLoader().loadTestsFromTestCase(TestGetSPSPackages)
TextTestRunner(verbosity=2).run(suite)
