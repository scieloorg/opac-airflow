import os
import shutil
import tempfile
from datetime import datetime
from unittest import TestSuite, TestCase, TestLoader, TextTestRunner
from unittest.mock import patch, MagicMock

from airflow import DAG

from kernel_documents import get_sps_packages, list_documents


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


class TestListDocuments(TestCase):
    def test_list_document_gets_ti_sps_packages_list(self):
        kwargs = {"ti": MagicMock()}
        list_documents(**kwargs)
        kwargs["ti"].xcom_pull.assert_called_once_with(
            key="sps_packages",
            task_id="get_sps_packages_id"
        )

    @patch('kernel_documents.ZipFile')
    def test_list_document_opens_all_zips(self, MockZipFile):
        sps_packages = [
            "dir/destination/abc_v50.zip",
            "dir/destination/rba_v53n1.zip",
            "dir/destination/rsp_v10n2-3.zip",
        ]
        kwargs = {"ti": MagicMock()}
        kwargs["ti"].xcom_pull.return_value = sps_packages
        list_documents(**kwargs)
        for sps_package in sps_packages:
            MockZipFile.assert_any_call(sps_package)

    @patch('kernel_documents.ZipFile')
    def test_list_document_reads_all_xmls_from_all_zips(self, MockZipFile):
        sps_packages = [
            "dir/destination/rba_v53n1.zip",
        ]
        sps_packages_file_list = [
            '1806-907X-rba-53-01-1-8.xml',
            'v53n1a01.pdf',
            '1806-907X-rba-53-01-1-8-gpn1a01t1.htm',
            '1806-907X-rba-53-01-1-8-gpn1a01g1.htm',
            '1806-907X-rba-53-01-9-18.xml',
            'v53n1a02.pdf',
            '1806-907X-rba-53-01-19-25.xml',
            '1806-907X-rba-53-01-19-25-g1.jpg',
            'v53n1a03.pdf',
        ]
        kwargs = {"ti": MagicMock()}
        kwargs["ti"].xcom_pull.return_value = sps_packages
        MockZipFile.return_value.__enter__.return_value.namelist.return_value = \
            sps_packages_file_list
        list_documents(**kwargs)
        kwargs["ti"].xcom_push.assert_called_once_with(
            key="sps_xmls_list",
            value=[
                xml_filename
                for xml_filename in sps_packages_file_list
                if os.path.splitext(xml_filename)[-1] == '.xml'
            ]
        )

    @patch('kernel_documents.ZipFile')
    def test_list_document_doesnt_call_ti_xcom_push_if_no_xml_files(self, MockZipFile):
        sps_packages = [
            "dir/destination/rba_v53n1.zip",
        ]
        sps_packages_file_list = [
            'v53n1a01.pdf',
            'v53n1a02.pdf',
            'v53n1a03.pdf',
        ]
        kwargs = {"ti": MagicMock()}
        kwargs["ti"].xcom_pull.return_value = sps_packages
        MockZipFile.return_value.__enter__.return_value.namelist.return_value = \
            sps_packages_file_list
        list_documents(**kwargs)
        kwargs["ti"].xcom_push.assert_not_called()


if __name__ == '__main__':
    test_classes_to_run = [
        TestGetSPSPackages,
        TestListDocuments,
    ]

    loader = TestLoader()
    suites_list = [
        loader.loadTestsFromTestCase(test_class)
        for test_class in test_classes_to_run
    ]
    TextTestRunner().run(TestSuite(suites_list))
