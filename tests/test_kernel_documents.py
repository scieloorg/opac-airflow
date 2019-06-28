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
    def test_get_sps_packages_xc_dir_from_env_var_is_None(self, MockPath):
        with patch.dict('os.environ', {}):
            get_sps_packages(**{"scilista": {"rba": "v53n1"}})
            MockPath.assert_not_called()

    @patch('kernel_documents.Path')
    def test_get_sps_packages_gets_xc_dir_from_env_var(self, MockPath):
        with patch.dict('os.environ', self.test_environ):
            get_sps_packages(**{"scilista": {"rba": "v53n1"}})
            MockPath.assert_any_call("dir/source")
            MockPath.assert_any_call("dir/destination")

    @patch('kernel_documents.shutil')
    def test_get_sps_packages_moves_from_xc_dir_if_xc_source_exists(self, mk_shutil):
        with patch.dict('os.environ', self.test_environ):
            get_sps_packages(**{"scilista": {"rba": "v53n2"}})
            mk_shutil.move.assert_not_called()

    @patch('kernel_documents.shutil')
    def test_get_sps_packages_moves_from_xc_dir_to_proc_dir(self, mk_shutil):
        test_dest_dir_name = "sps_packages"
        with tempfile.TemporaryDirectory() as tmpdirname:
            test_environ = {
                "XC_SPS_PACKAGES_DIR": "../fixtures",
                "PROC_SPS_PACKAGES_DIR": tmpdirname,
            }
            with patch.dict('os.environ', test_environ):
                kwargs = {
                    "scilista": {"rba": "v53n1"},
                    "ti": MagicMock(),
                }
                get_sps_packages(**kwargs)
                mk_shutil.move.assert_called_once_with(
                    "../fixtures/rba_v53n1.zip",
                    tmpdirname + "/rba_v53n1.zip"
                )

    @patch('kernel_documents.shutil')
    @patch('kernel_documents.os.path.exists')
    def test_get_sps_packages_doesnt_call_ti_xcom_push_if_no_sps_packages_list(
        self, mk_path_exists, mk_shutil
    ):
        mk_path_exists.return_value = False
        with patch.dict('os.environ', self.test_environ):
            kwargs = {
                "scilista": {"rba": "v53n1"},
                "ti": MagicMock(),
            }
            get_sps_packages(**kwargs)
            kwargs["ti"].xcom_push.assert_not_called()

    @patch('kernel_documents.shutil')
    @patch('kernel_documents.os.path.exists')
    def test_get_sps_packages_calls_ti_xcom_push_with_sps_packages_list(
        self, mk_path_exists, mk_shutil
    ):
        mk_path_exists.return_value = True
        with patch.dict('os.environ', self.test_environ):
            kwargs = {
                "scilista": {
                    "rba": "v53n1",
                    "rsp": "v10n2-3",
                    "abc": "v50"
                }
            }
            kwargs["ti"] = MagicMock()
            get_sps_packages(**kwargs)
            kwargs["ti"].xcom_push.assert_called_once_with(
                key="sps_packages",
                value=[
                    "dir/destination/abc_v50.zip",
                    "dir/destination/rba_v53n1.zip",
                    "dir/destination/rsp_v10n2-3.zip",
                ]
            )


suite = TestLoader().loadTestsFromTestCase(TestGetSPSPackages)
TextTestRunner(verbosity=2).run(suite)
