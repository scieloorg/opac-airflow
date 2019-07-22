import os
import tempfile
from unittest import TestSuite, TestCase, TestLoader, TextTestRunner
from unittest.mock import patch, MagicMock

from airflow import DAG

from kernel_documents import get_sps_packages, list_documents


class TestGetSPSPackages(TestCase):
    @patch('kernel_documents.Variable.get')
    def test_get_sps_packages_raises_error_if_no_xc_dir_from_variable(self,
                                                                      mk_variable_get):
        mk_variable_get.side_effect = KeyError
        self.assertRaises(KeyError, get_sps_packages)

    @patch('kernel_documents.Path')
    @patch('kernel_documents.Variable.get')
    @patch('kernel_documents.open')
    def test_get_sps_packages_gets_xc_dir_from_variable(self, mk_open,
                                                        mk_variable_get,
                                                        MockPath):

        mk_variable_get.side_effect = ["dir/path/scilista.lst",
                                       "dir/source",
                                       "dir/destination"]
        kwargs = {
            "ti": MagicMock(),
        }
        get_sps_packages(**kwargs)
        MockPath.assert_any_call("dir/source")
        MockPath.assert_any_call("dir/destination")

    @patch('kernel_documents.shutil')
    @patch('kernel_documents.Variable.get')
    @patch('kernel_documents.open')
    def test_get_sps_packages_moves_from_xc_dir_if_xc_source_exists(self,
                                                                    mk_open,
                                                                    mk_variable_get,
                                                                    mk_shutil):
        mk_variable_get.return_value = ""
        kwargs = {
            "ti": MagicMock(),
        }
        get_sps_packages(**kwargs)
        mk_shutil.move.assert_not_called()

    @patch('kernel_documents.shutil')
    @patch('kernel_documents.os.path.exists')
    @patch('kernel_documents.Variable.get')
    @patch('kernel_documents.open')
    def test_get_sps_packages_moves_from_xc_dir_to_proc_dir(self,
                                                            mk_open,
                                                            mk_variable_get,
                                                            mk_path_exists,
                                                            mk_shutil):
        mk_path_exists.return_value = True
        mk_open.return_value.__enter__.return_value.readlines.return_value = [
            "rba v53n1",
        ]

        with tempfile.TemporaryDirectory() as tmpdirname:
            test_paths = [
                "dir/path/scilista.lst",
                "../fixtures",
                tmpdirname,
            ]
            mk_variable_get.side_effect = test_paths
            kwargs = {
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
    @patch('kernel_documents.open')
    def test_get_sps_packages_calls_ti_xcom_push_with_empty_list(
        self, mk_open, mk_variable_get, mk_path_exists, mk_shutil
    ):
        mk_path_exists.return_value = False
        mk_variable_get.return_value = ""
        kwargs = {
            "scilista": ["rba v53n1"],
            "ti": MagicMock(),
        }
        get_sps_packages(**kwargs)
        kwargs["ti"].xcom_push.assert_called_once_with(
            key="sps_packages",
            value=[]
        )

    @patch('kernel_documents.shutil')
    @patch('kernel_documents.os.path.exists')
    @patch('kernel_documents.Variable.get')
    @patch('kernel_documents.open')
    def test_get_sps_packages_calls_ti_xcom_push_with_sps_packages_list(
        self, mk_open, mk_variable_get, mk_path_exists, mk_shutil
    ):
        mk_path_exists.return_value = True
        mk_variable_get.return_value = "dir/destination/"
        mk_open.return_value.__enter__.return_value.readlines.return_value = [
            "rba v53n1",
            "rba 2018nahead",
            "rsp v10n2-3",
            "abc v50"
        ]
        kwargs = {
            "ti": MagicMock()
        }
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

    @patch('kernel_documents.open')
    @patch('kernel_documents.Variable.get')
    def test_read_scilista_from_file(self, mk_variable_get, mk_open):
        mk_variable_get.return_value = "dir/path/scilista.lst"
        kwargs = {
            "ti": MagicMock()
        }
        get_sps_packages(**kwargs)
        mk_open.assert_called_once_with("dir/path/scilista.lst")


class TestListDocuments(TestCase):
    def test_list_document_gets_ti_sps_packages_list(self):
        kwargs = {"ti": MagicMock()}
        list_documents(**kwargs)
        kwargs["ti"].xcom_pull.assert_called_once_with(
            key="sps_packages",
            task_ids="get_sps_packages_id"
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
