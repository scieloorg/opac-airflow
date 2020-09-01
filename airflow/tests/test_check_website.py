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
    get_uri_items_from_uri_list_files,
    get_pid_list_csv_file_paths,
    get_uri_items_from_pid_list_csv_files,
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


class TestGetUriItemsFromUriListFiles(TestCase):

    def setUp(self):
        self.kwargs = {
            "ti": MagicMock(),
            "conf": None,
            "run_id": "test_run_id",
        }
        self.proc_dir = tempfile.mkdtemp()

        content = [
            (
                "/scielo.php?script=sci_serial&pid=0001-3765\n"
                "/scielo.php?script=sci_issues&pid=0001-3765\n"
                "/scielo.php?script=sci_issuetoc&pid=0001-376520200005\n"
                "/scielo.php?script=sci_arttext&pid=S0001-37652020000501101\n"
                ),
            (
                "/scielo.php?script=sci_serial&pid=0001-3035\n"
                "/scielo.php?script=sci_issues&pid=0001-3035\n"
                "/scielo.php?script=sci_issuetoc&pid=0001-303520200005\n"
                "/scielo.php?script=sci_arttext&pid=S0001-30352020000501101\n"
            ),
            (
                "/scielo.php?script=sci_serial&pid=0203-1998\n"
                "/scielo.php?script=sci_issues&pid=0203-1998\n"
                "/scielo.php?script=sci_issuetoc&pid=0203-199820200005\n"
                "/scielo.php?script=sci_arttext&pid=S0203-19982020000501101\n"
                "/scielo.php?script=sci_serial&pid=1213-1998\n"
                "/scielo.php?script=sci_issues&pid=1213-1998\n"
                "/scielo.php?script=sci_issuetoc&pid=1213-199821211115\n"
                "/scielo.php?script=sci_arttext&pid=S1213-19982121111511111\n"
            ),
        ]

        for i, f in enumerate(
                    ["uri_list_2020-01-01.lst",
                     "uri_list_2020-01-02.lst",
                     "uri_list_2020-01-03.lst"]
                ):
            file_path = pathlib.Path(self.proc_dir) / f
            with open(file_path, "w") as fp:
                fp.write(content[i])

    def tearDown(self):
        shutil.rmtree(self.proc_dir)

    def test_get_uri_items_from_uri_list_files_gets_uri_items(self):
        self.kwargs["ti"].xcom_pull.return_value = [
            pathlib.Path(self.proc_dir) / "uri_list_2020-01-01.lst",
            pathlib.Path(self.proc_dir) / "uri_list_2020-01-02.lst",
            pathlib.Path(self.proc_dir) / "uri_list_2020-01-03.lst",

        ]
        get_uri_items_from_uri_list_files(**self.kwargs)
        self.kwargs["ti"].xcom_push.assert_called_once_with(
            "uri_items",
            sorted([
                "/scielo.php?script=sci_serial&pid=0001-3765",
                "/scielo.php?script=sci_issues&pid=0001-3765",
                "/scielo.php?script=sci_issuetoc&pid=0001-376520200005",
                "/scielo.php?script=sci_arttext&pid=S0001-37652020000501101",
                "/scielo.php?script=sci_serial&pid=0001-3035",
                "/scielo.php?script=sci_issues&pid=0001-3035",
                "/scielo.php?script=sci_issuetoc&pid=0001-303520200005",
                "/scielo.php?script=sci_arttext&pid=S0001-30352020000501101",
                "/scielo.php?script=sci_serial&pid=0203-1998",
                "/scielo.php?script=sci_issues&pid=0203-1998",
                "/scielo.php?script=sci_issuetoc&pid=0203-199820200005",
                "/scielo.php?script=sci_arttext&pid=S0203-19982020000501101",
                "/scielo.php?script=sci_serial&pid=1213-1998",
                "/scielo.php?script=sci_issues&pid=1213-1998",
                "/scielo.php?script=sci_issuetoc&pid=1213-199821211115",
                "/scielo.php?script=sci_arttext&pid=S1213-19982121111511111",
            ]),
        )


class TestGetPidListCSVFilePaths(TestCase):

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
        for f in ("pid_list_2020-01-01.csv", "pid_list_2020-01-02.csv", "pid_list_2020-01-03.csv"):
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
    def test_get_pid_list_csv_file_paths_returns_the_two_files_copied_to_proc_dir_from_gate_dir(self, mock_get):
        expected = [
            str(pathlib.Path(self.proc_dir) / "test_run_id" / "pid_list_2020-01-01.csv"),
            str(pathlib.Path(self.proc_dir) / "test_run_id" / "pid_list_2020-01-03.csv"),
        ]
        mock_get.side_effect = [
            ['pid_list_2020-01-01.csv', 'pid_list_2020-01-03.csv'],
            self.gate_dir,
            self.proc_dir,
        ]
        get_pid_list_csv_file_paths(**self.kwargs)
        self.assertListEqual(
            [
                call('old_file_paths', []),
                call('new_file_paths', expected),
                call('file_paths', expected),
            ],
            self.kwargs["ti"].xcom_push.call_args_list
        )

    @patch("check_website.Variable.get")
    def test_get_pid_list_csv_file_paths_returns_all_files_found_in_proc_dir(self, mock_get):
        for f in ("pid_list_2020-03-01.csv", "pid_list_2020-03-02.csv", "pid_list_2020-03-03.csv"):
            file_path = pathlib.Path(self.dag_proc_dir) / f
            with open(file_path, "w") as fp:
                fp.write("")
        expected = [
            str(pathlib.Path(self.dag_proc_dir) / "pid_list_2020-03-01.csv"),
            str(pathlib.Path(self.dag_proc_dir) / "pid_list_2020-03-02.csv"),
            str(pathlib.Path(self.dag_proc_dir) / "pid_list_2020-03-03.csv"),
            str(pathlib.Path(self.dag_proc_dir) / "pid_list_2020-01-01.csv"),
            str(pathlib.Path(self.dag_proc_dir) / "pid_list_2020-01-03.csv"),
        ]
        mock_get.side_effect = [
            ['pid_list_2020-01-01.csv', 'pid_list_2020-01-03.csv'],
            self.gate_dir,
            self.proc_dir,
        ]
        get_pid_list_csv_file_paths(**self.kwargs)
        self.assertListEqual(
            [
                call('old_file_paths', expected[:3]),
                call('new_file_paths', expected[-2:]),
                call('file_paths', expected),
            ],
            self.kwargs["ti"].xcom_push.call_args_list
        )

class TestGetUriItemsFromPidFiles(TestCase):

    def setUp(self):
        self.kwargs = {
            "ti": MagicMock(),
            "conf": None,
            "run_id": "test_run_id",
        }
        self.proc_dir = tempfile.mkdtemp()

        content = [
            (
                "S0001-37652020000501101\n"
                ),
            (
                "S0001-30352020000501101\n"
            ),
            (
                "S0203-19982020000501101\n"
                "S1213-19982121111511111\n"
            ),
        ]

        for i, f in enumerate(
                    ["pid_2020-01-01.csv",
                     "pid_2020-01-02.csv",
                     "pid_2020-01-03.csv"]
                ):
            file_path = pathlib.Path(self.proc_dir) / f
            with open(file_path, "w") as fp:
                fp.write(content[i])

    def tearDown(self):
        shutil.rmtree(self.proc_dir)

    def test_get_uri_items_from_pid_list_csv_files_gets_uri_items(self):
        self.kwargs["ti"].xcom_pull.return_value = [
            pathlib.Path(self.proc_dir) / "pid_2020-01-01.csv",
            pathlib.Path(self.proc_dir) / "pid_2020-01-02.csv",
            pathlib.Path(self.proc_dir) / "pid_2020-01-03.csv",

        ]
        get_uri_items_from_pid_list_csv_files(**self.kwargs)
        self.kwargs["ti"].xcom_push.assert_called_once_with(
            "uri_items",
            [
                "/scielo.php?script=sci_serial&pid=0001-3035",
                "/scielo.php?script=sci_issues&pid=0001-3035",
                "/scielo.php?script=sci_issuetoc&pid=0001-303520200005",
                "/scielo.php?script=sci_arttext&pid=S0001-30352020000501101",
                "/scielo.php?script=sci_serial&pid=0001-3765",
                "/scielo.php?script=sci_issues&pid=0001-3765",
                "/scielo.php?script=sci_issuetoc&pid=0001-376520200005",
                "/scielo.php?script=sci_arttext&pid=S0001-37652020000501101",
                "/scielo.php?script=sci_serial&pid=0203-1998",
                "/scielo.php?script=sci_issues&pid=0203-1998",
                "/scielo.php?script=sci_issuetoc&pid=0203-199820200005",
                "/scielo.php?script=sci_arttext&pid=S0203-19982020000501101",
                "/scielo.php?script=sci_serial&pid=1213-1998",
                "/scielo.php?script=sci_issues&pid=1213-1998",
                "/scielo.php?script=sci_issuetoc&pid=1213-199821211115",
                "/scielo.php?script=sci_arttext&pid=S1213-19982121111511111",
            ],
        )
