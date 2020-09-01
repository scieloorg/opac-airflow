import tempfile
import shutil
import pathlib
import os
from unittest import TestCase, main
from unittest.mock import patch, MagicMock, ANY, call

import pendulum
from airflow import DAG

from check_website import (
    get_file_path_in_proc_dir,
    check_website_uri_list,
    get_uri_list_file_paths,
)


class TestGetFilePathInProcDir(TestCase):
    def setUp(self):
        self.gate_dir = tempfile.mkdtemp()
        self.proc_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.gate_dir)
        shutil.rmtree(self.proc_dir)

    def test_uri_list_does_not_exist_in_origin(self):
        with self.assertRaises(FileNotFoundError) as exc_info:
            get_file_path_in_proc_dir(
                pathlib.Path("no/dir/path"),
                pathlib.Path(self.proc_dir),
                "uri_list_2020-12-31.lst",
            )
        self.assertIn("no/dir/path/uri_list_2020-12-31.lst", str(exc_info.exception))

    def test_uri_list_already_exists_in_proc(self):
        uri_list_path_origin = pathlib.Path(self.gate_dir) / "uri_list_2020-01-01.lst"
        uri_list_path_origin.write_text("/scielo.php?script=sci_serial&pid=1234-5678")
        uri_list_path_proc = pathlib.Path(self.proc_dir) / "uri_list_2020-01-01.lst"
        uri_list_path_proc.write_text("/scielo.php?script=sci_serial&pid=5678-0909")
        _uri_list_file_path = get_file_path_in_proc_dir(
            pathlib.Path(self.gate_dir),
            pathlib.Path(self.proc_dir),
            "uri_list_2020-01-01.lst",
        )

        self.assertEqual(
            _uri_list_file_path,
            str(pathlib.Path(self.proc_dir) / "uri_list_2020-01-01.lst")
        )
        self.assertEqual(uri_list_path_proc.read_text(), "/scielo.php?script=sci_serial&pid=5678-0909")

    def test_uri_list_must_be_copied(self):
        uri_list_path = pathlib.Path(self.gate_dir) / "uri_list_2020-01-01.lst"
        uri_list_path.write_text("/scielo.php?script=sci_serial&pid=5678-0909")
        _uri_list_file_path = get_file_path_in_proc_dir(
            pathlib.Path(self.gate_dir),
            pathlib.Path(self.proc_dir),
            "uri_list_2020-01-01.lst",
        )
        self.assertEqual(
            _uri_list_file_path,
            str(pathlib.Path(self.proc_dir) / "uri_list_2020-01-01.lst")
        )


class TestGetUriListFilePaths(TestCase):

    def setUp(self):
        self.kwargs = {
            "ti": MagicMock(),
            "conf": None,
            "run_id": "test_run_id",
        }
        self.gate_dir = tempfile.mkdtemp()
        self.proc_dir = tempfile.mkdtemp()
        self.dag_proc_dir = str(pathlib.Path(self.proc_dir) / "test_run_id")
        os.makedirs(self.dag_proc_dir)
        for f in ("uri_list_2020-01-01.lst", "uri_list_2020-01-02.lst", "uri_list_2020-01-03.lst"):
            file_path = pathlib.Path(self.gate_dir) / f
            with open(file_path, "w") as fp:
                fp.write("")
        for f in ("any_2020-01-01.lst", "any_2020-01-02.lst", "any_2020-01-03.lst"):
            file_path = pathlib.Path(self.gate_dir) / f
            with open(file_path, "w") as fp:
                fp.write("")
        for f in ("any_2020-01-01.lst", "any_2020-01-02.lst", "any_2020-01-03.lst"):
            file_path = pathlib.Path(self.dag_proc_dir) / f
            with open(file_path, "w") as fp:
                fp.write("")

    def tearDown(self):
        shutil.rmtree(self.gate_dir)
        shutil.rmtree(self.proc_dir)

    @patch("check_website.Variable.get")
    def test_get_uri_list_file_paths_returns_the_two_files_copied_to_proc_dir_from_gate_dir(self, mock_get):
        expected = [
            str(pathlib.Path(self.proc_dir) / "test_run_id" / "uri_list_2020-01-01.lst"),
            str(pathlib.Path(self.proc_dir) / "test_run_id" / "uri_list_2020-01-03.lst"),
        ]
        mock_get.side_effect = [
            ['2020-01-01', '2020-01-03'],
            self.gate_dir,
            self.proc_dir,
        ]
        get_uri_list_file_paths(**self.kwargs)
        self.assertListEqual(
            [
                call('old_uri_list_file_paths', []),
                call('new_uri_list_file_paths', expected),
                call('uri_list_file_paths', expected),
            ],
            self.kwargs["ti"].xcom_push.call_args_list
        )

    @patch("check_website.Variable.get")
    def test_get_uri_list_file_paths_returns_all_files_found_in_proc_dir(self, mock_get):
        for f in ("uri_list_2020-03-01.lst", "uri_list_2020-03-02.lst", "uri_list_2020-03-03.lst"):
            file_path = pathlib.Path(self.dag_proc_dir) / f
            with open(file_path, "w") as fp:
                fp.write("")
        expected = [
            str(pathlib.Path(self.dag_proc_dir) / "uri_list_2020-03-01.lst"),
            str(pathlib.Path(self.dag_proc_dir) / "uri_list_2020-03-02.lst"),
            str(pathlib.Path(self.dag_proc_dir) / "uri_list_2020-03-03.lst"),
            str(pathlib.Path(self.dag_proc_dir) / "uri_list_2020-01-01.lst"),
            str(pathlib.Path(self.dag_proc_dir) / "uri_list_2020-01-03.lst"),
        ]
        mock_get.side_effect = [
            ['2020-01-01', '2020-01-03'],
            self.gate_dir,
            self.proc_dir,
        ]
        get_uri_list_file_paths(**self.kwargs)
        self.assertListEqual(
            [
                call('old_uri_list_file_paths', expected[:3]),
                call('new_uri_list_file_paths', expected[-2:]),
                call('uri_list_file_paths', expected),
            ],
            self.kwargs["ti"].xcom_push.call_args_list
        )

