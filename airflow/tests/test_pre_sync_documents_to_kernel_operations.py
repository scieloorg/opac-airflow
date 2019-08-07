import tempfile
from unittest import TestCase, main
from unittest.mock import patch, MagicMock

from airflow import DAG

from operations.pre_sync_documents_to_kernel_operations import get_sps_packages


class TestGetSPSPackages(TestCase):
    def setUp(self):
        self.kwargs = {
            "scilista_file_path": "dir/path/scilista.lst",
            "xc_dir_name": "dir/source",
            "proc_dir_name": "dir/destination",
        }

    @patch("operations.pre_sync_documents_to_kernel_operations.Path")
    @patch("operations.pre_sync_documents_to_kernel_operations.open")
    def test_get_sps_packages_creates_path_dirs(self, mk_open, MockPath):
        get_sps_packages(**self.kwargs)
        MockPath.assert_any_call(self.kwargs["xc_dir_name"])
        MockPath.assert_any_call(self.kwargs["proc_dir_name"])

    @patch("operations.pre_sync_documents_to_kernel_operations.open")
    def test_read_scilista_from_file(self, mk_open):
        get_sps_packages(**self.kwargs)
        mk_open.assert_called_once_with("dir/path/scilista.lst")

    @patch("operations.pre_sync_documents_to_kernel_operations.open")
    def test_get_sps_packages_raises_error_if_scilista_open_error(self, mk_open):
        mk_open.side_effect = FileNotFoundError
        self.assertRaises(FileNotFoundError, get_sps_packages, *self.kwargs)

    @patch("operations.pre_sync_documents_to_kernel_operations.shutil")
    @patch("operations.pre_sync_documents_to_kernel_operations.os.path.exists")
    @patch("operations.pre_sync_documents_to_kernel_operations.open")
    def test_get_sps_packages_moves_from_xc_dir_to_proc_dir(
        self, mk_open, mk_path_exists, mk_shutil
    ):
        mk_path_exists.return_value = True
        scilista_lines = ["rba v53n1", "rba 2019nahead", "rsp v10n4s1"]
        mk_open.return_value.__enter__.return_value.readlines.return_value = (
            scilista_lines
        )

        with tempfile.TemporaryDirectory() as tmpdirname:
            self.kwargs["proc_dir_name"] = tmpdirname
            get_sps_packages(**self.kwargs)
            for scilista_line in scilista_lines:
                with self.subTest(scilista_line=scilista_line):
                    filename = "/{}_{}.zip".format(*scilista_line.split())
                    mk_shutil.move.assert_any_call(
                        self.kwargs["xc_dir_name"] + filename, tmpdirname + filename
                    )

    @patch("operations.pre_sync_documents_to_kernel_operations.shutil")
    @patch("operations.pre_sync_documents_to_kernel_operations.os.path.exists")
    @patch("operations.pre_sync_documents_to_kernel_operations.open")
    def test_get_sps_packages_moves_anything_if_no_source_file(
        self, mk_open, mk_path_exists, mk_shutil
    ):
        mk_path_exists.return_value = False
        mk_open.return_value.__enter__.return_value.readlines.return_value = [
            "rba v53n1"
        ]
        get_sps_packages(**self.kwargs)
        mk_shutil.move.assert_not_called()


if __name__ == "__main__":
    main()
