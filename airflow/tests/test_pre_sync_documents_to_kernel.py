import tempfile
import shutil
import pathlib
from unittest import TestCase, main
from unittest.mock import patch, MagicMock, ANY

from airflow import DAG

from pre_sync_documents_to_kernel import (
    get_scilista_file_path,
    get_sps_packages,
)


class TestGetScilistaFilePath(TestCase):
    def setUp(self):
        self.gate_dir = tempfile.mkdtemp()
        self.proc_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.gate_dir)
        shutil.rmtree(self.proc_dir)

    def test_scilista_does_not_exist_in_origin(self):
        with self.assertRaises(FileNotFoundError) as exc_info:
            get_scilista_file_path(
                pathlib.Path("no/dir/path"),
                pathlib.Path(self.proc_dir),
                "2020-12-31",
            )
        self.assertIn(str(exc_info.exception), "no/dir/path/scilista-2020-12-31.lst")

    def test_scilista_already_exists_in_proc(self):
        scilista_path_origin = pathlib.Path(self.gate_dir) / "scilista-2020-01-01.lst"
        scilista_path_origin.write_text("acron v1n22")
        scilista_path_proc = pathlib.Path(self.proc_dir) / "scilista-2020-01-01.lst"
        scilista_path_proc.write_text("acron v1n20")
        _scilista_file_path = get_scilista_file_path(
            pathlib.Path(self.gate_dir),
            pathlib.Path(self.proc_dir),
            "2020-01-01",
        )

        self.assertEqual(
            _scilista_file_path,
            str(pathlib.Path(self.proc_dir) / "scilista-2020-01-01.lst")
        )
        self.assertEqual(scilista_path_proc.read_text(), "acron v1n20")

    def test_scilista_must_be_copied(self):
        scilista_path = pathlib.Path(self.gate_dir) / "scilista-2020-01-01.lst"
        scilista_path.write_text("acron v1n20")
        _scilista_file_path = get_scilista_file_path(
            pathlib.Path(self.gate_dir),
            pathlib.Path(self.proc_dir),
            "2020-01-01",
        )
        self.assertEqual(
            _scilista_file_path,
            str(pathlib.Path(self.proc_dir) / "scilista-2020-01-01.lst")
        )


class TestGetSPSPackages(TestCase):
    @patch("pre_sync_documents_to_kernel.Variable.get")
    def test_get_sps_packages_raises_error_if_no_xc_dir_from_variable(
        self, mk_variable_get
    ):
        mk_variable_get.side_effect = KeyError
        self.assertRaises(KeyError, get_sps_packages, None)

    @patch("pre_sync_documents_to_kernel.pre_sync_documents_to_kernel_operations.get_sps_packages")
    @patch("pre_sync_documents_to_kernel.Variable.get")
    @patch("pre_sync_documents_to_kernel.trigger_dag")
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

    @patch("pre_sync_documents_to_kernel.pre_sync_documents_to_kernel_operations.get_sps_packages")
    @patch("pre_sync_documents_to_kernel.Variable.get")
    @patch("pre_sync_documents_to_kernel.trigger_dag")
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
                    dag_id="sync_documents_to_kernel",
                    run_id=ANY,
                    execution_date=ANY,
                    replace_microseconds=False,
                    conf={"sps_package": mk_sps_package, "pre_syn_dag_run_id": ANY},
                )


if __name__ == "__main__":
    main()
