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
    get_uri_items_grouped_by_script_name,
    check_sci_serial_uri_items,
    check_sci_issues_uri_items,
    check_sci_issuetoc_uri_items,
    check_sci_pdf_uri_items,
    check_sci_arttext_uri_items,
    check_any_uri_items,
    get_pid_v3_list,
    get_website_url_list,
    group_uri_items_from_uri_lists_by_script_name,
    merge_uri_items_from_different_sources,
    check_input_vs_processed_pids,
    check_documents_deeply,
)
from .test_check_website_operations import (
    MockClientResponse,
    START_TIME,
    END_TIME,
    DURATION,
    fixes_for_json,
)
print(DURATION)

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

    @patch("check_website.Variable.set")
    @patch("check_website.Variable.get")
    def test_get_uri_list_file_paths_identifies_the_two_files_copied_to_proc_dir_from_gate_dir(
            self, mock_get, mock_set):
        expected = [
            str(pathlib.Path(self.proc_dir) / "test_run_id" / "uri_list_2020-01-01.lst"),
            str(pathlib.Path(self.proc_dir) / "test_run_id" / "uri_list_2020-01-03.lst"),
        ]
        mock_get.side_effect = [
            ['2020-01-01', '2020-01-03'],
            self.gate_dir,
            self.proc_dir,
        ]
        result = get_uri_list_file_paths(**self.kwargs)
        self.assertListEqual(
            [
                call('old_uri_list_file_paths', []),
                call('new_uri_list_file_paths', expected),
                call('uri_list_file_paths', expected),
            ],
            self.kwargs["ti"].xcom_push.call_args_list
        )
        self.assertTrue(result)

    @patch("check_website.Variable.set")
    @patch("check_website.Variable.get")
    def test_get_uri_list_file_paths_identifies_all_files_found_in_proc_dir(
            self, mock_get, mock_set):
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
        result = get_uri_list_file_paths(**self.kwargs)
        self.assertListEqual(
            [
                call('old_uri_list_file_paths', sorted(expected[:3])),
                call('new_uri_list_file_paths', sorted(expected[-2:])),
                call('uri_list_file_paths', sorted(expected)),
            ],
            self.kwargs["ti"].xcom_push.call_args_list
        )
        self.assertTrue(result)

    @patch("check_website.Variable.set")
    @patch("check_website.Variable.get")
    def test_get_uri_list_file_paths_returns_false_because_there_is_no_uri_list_files(
            self, mock_get, mock_set):
        for f in ("uri_list_2020-01-01.lst", "uri_list_2020-01-02.lst", "uri_list_2020-01-03.lst"):
            file_path = pathlib.Path(self.gate_dir) / f
            os.unlink(file_path)

        mock_get.side_effect = [
            [],
            self.gate_dir,
            self.proc_dir,
        ]
        result = get_uri_list_file_paths(**self.kwargs)
        self.kwargs["ti"].xcom_push.assert_not_called()
        self.assertFalse(result)


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

    def test_get_uri_items_from_uri_list_files_gets_uri_items_returns_true(self):
        self.kwargs["ti"].xcom_pull.return_value = [
            pathlib.Path(self.proc_dir) / "uri_list_2020-01-01.lst",
            pathlib.Path(self.proc_dir) / "uri_list_2020-01-02.lst",
            pathlib.Path(self.proc_dir) / "uri_list_2020-01-03.lst",

        ]
        result = get_uri_items_from_uri_list_files(**self.kwargs)
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
        self.assertTrue(result)

    @patch("check_website.check_website_operations.read_file")
    def test_get_uri_items_from_uri_list_files_gets_uri_items_returns_false(
            self, mock_read_file):
        self.kwargs["ti"].xcom_pull.return_value = [
            pathlib.Path(self.proc_dir) / "uri_list_2020-01-01.lst",
            pathlib.Path(self.proc_dir) / "uri_list_2020-01-02.lst",
            pathlib.Path(self.proc_dir) / "uri_list_2020-01-03.lst",
        ]
        mock_read_file.side_effect = [
            "\n\n\n",
            "\n\n\n",
            "",
        ]
        result = get_uri_items_from_uri_list_files(**self.kwargs)
        self.kwargs["ti"].xcom_push.assert_not_called()
        self.assertFalse(result)


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
    def test_get_pid_list_csv_file_paths_finds_the_two_files_copied_to_proc_dir_from_gate_dir(self, mock_get):
        expected = [
            str(pathlib.Path(self.proc_dir) / "test_run_id" / "pid_list_2020-01-01.csv"),
            str(pathlib.Path(self.proc_dir) / "test_run_id" / "pid_list_2020-01-03.csv"),
        ]
        mock_get.side_effect = [
            ['pid_list_2020-01-01.csv', 'pid_list_2020-01-03.csv'],
            self.gate_dir,
            self.proc_dir,
        ]
        result = get_pid_list_csv_file_paths(**self.kwargs)
        self.assertListEqual(
            [
                call('old_file_paths', []),
                call('new_file_paths', expected),
                call('file_paths', expected),
            ],
            self.kwargs["ti"].xcom_push.call_args_list
        )
        self.assertTrue(result)

    @patch("check_website.Variable.get")
    def test_get_pid_list_csv_file_paths_finds_all_files_found_in_proc_dir(self, mock_get):
        for f in ("pid_list_2020-03-01.csv", "pid_list_2020-03-02.csv", "pid_list_2020-03-03.csv"):
            file_path = pathlib.Path(self.dag_proc_dir) / f
            with open(file_path, "w") as fp:
                fp.write("")
        expected =[
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
        result = get_pid_list_csv_file_paths(**self.kwargs)
        self.assertListEqual(
            [
                call('old_file_paths', sorted(expected[:3])),
                call('new_file_paths', sorted(expected[-2:])),
                call('file_paths', sorted(expected)),
            ],
            self.kwargs["ti"].xcom_push.call_args_list
        )
        self.assertTrue(result)

    @patch("check_website.Variable.get")
    def test_get_pid_list_csv_file_paths_returns_false(self, mock_get):
        for f in ("pid_list_2020-01-01.csv", "pid_list_2020-01-02.csv", "pid_list_2020-01-03.csv"):
            file_path = pathlib.Path(self.gate_dir) / f
            os.unlink(file_path)
        mock_get.side_effect = [
            [],
            self.gate_dir,
            self.proc_dir,
        ]
        result = get_pid_list_csv_file_paths(**self.kwargs)
        self.kwargs["ti"].xcom_push.assert_not_called()
        self.assertFalse(result)


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

    def test_get_uri_items_from_pid_list_csv_files_gets_uri_items_returns_true(self):
        self.kwargs["ti"].xcom_pull.return_value = [
            pathlib.Path(self.proc_dir) / "pid_2020-01-01.csv",
            pathlib.Path(self.proc_dir) / "pid_2020-01-02.csv",
            pathlib.Path(self.proc_dir) / "pid_2020-01-03.csv",

        ]
        result = get_uri_items_from_pid_list_csv_files(**self.kwargs)
        self.assertTrue(result)
        self.assertListEqual([
            call(
                "pid_items",
                [
                    "S0001-30352020000501101",
                    "S0001-37652020000501101",
                    "S0203-19982020000501101",
                    "S1213-19982121111511111",
                ]
            ),
            call(
                "uri_items",
                [
                    "/scielo.php?script=sci_serial&pid=0001-3035",
                    "/scielo.php?script=sci_issues&pid=0001-3035",
                    "/scielo.php?script=sci_issuetoc&pid=0001-303520200005",
                    "/scielo.php?script=sci_arttext&pid=S0001-30352020000501101",
                    "/scielo.php?script=sci_pdf&pid=S0001-30352020000501101",
                    "/scielo.php?script=sci_serial&pid=0001-3765",
                    "/scielo.php?script=sci_issues&pid=0001-3765",
                    "/scielo.php?script=sci_issuetoc&pid=0001-376520200005",
                    "/scielo.php?script=sci_arttext&pid=S0001-37652020000501101",
                    "/scielo.php?script=sci_pdf&pid=S0001-37652020000501101",
                    "/scielo.php?script=sci_serial&pid=0203-1998",
                    "/scielo.php?script=sci_issues&pid=0203-1998",
                    "/scielo.php?script=sci_issuetoc&pid=0203-199820200005",
                    "/scielo.php?script=sci_arttext&pid=S0203-19982020000501101",
                    "/scielo.php?script=sci_pdf&pid=S0203-19982020000501101",
                    "/scielo.php?script=sci_serial&pid=1213-1998",
                    "/scielo.php?script=sci_issues&pid=1213-1998",
                    "/scielo.php?script=sci_issuetoc&pid=1213-199821211115",
                    "/scielo.php?script=sci_arttext&pid=S1213-19982121111511111",
                    "/scielo.php?script=sci_pdf&pid=S1213-19982121111511111",
                ],
            )],
            self.kwargs["ti"].xcom_push.call_args_list
        )

    @patch("check_website.check_website_operations.get_pid_list_from_csv")
    def test_get_uri_items_from_pid_list_csv_files_gets_uri_items_returns_false(
            self, mock_get_pid_list_from_csv):
        self.kwargs["ti"].xcom_pull.return_value = [
            pathlib.Path(self.proc_dir) / "pid_2020-01-01.csv",
            pathlib.Path(self.proc_dir) / "pid_2020-01-02.csv",
            pathlib.Path(self.proc_dir) / "pid_2020-01-03.csv",
        ]
        mock_get_pid_list_from_csv.side_effect = [
            "invalidpidlist",
            "",
            "invalidpidlist\ninvalidpid",
        ]
        result = get_uri_items_from_pid_list_csv_files(**self.kwargs)
        self.assertFalse(result)
        self.kwargs["ti"].xcom_push.assert_not_called()


class TestGetUriItemsGroupedByScriptName(TestCase):
    def setUp(self):
        self.kwargs = {
            "ti": MagicMock(),
            "conf": None,
            "run_id": "test_run_id",
        }

    @patch("check_website.Logger.info")
    def test_get_uri_items_grouped_by_script_name_returns_true(self, mock_info):
        uri_list = [
            "/scielo.php?script=sci_arttext&pid=S0001-30352020000501101",
            "/scielo.php?script=sci_arttext&pid=S0001-37652020000501101",
            "/scielo.php?script=sci_arttext&pid=S0203-19982020000501101",
            "/scielo.php?script=sci_arttext&pid=S1213-19982121111511111",
            "/scielo.php?script=sci_arttext&pid=S1213-19982121111511112",
            "/scielo.php?script=sci_arttext&pid=S1213-19982121111511113",
            "/scielo.php?script=sci_arttext&pid=S1213-19982121111511114",
            "/scielo.php?script=sci_issues&pid=0001-3035",
            "/scielo.php?script=sci_issues&pid=0001-3765",
            "/scielo.php?script=sci_issues&pid=0203-1998",
            "/scielo.php?script=sci_issues&pid=1213-1998",
            "/scielo.php?script=sci_issuetoc&pid=0001-303520200005",
            "/scielo.php?script=sci_issuetoc&pid=0001-376520200005",
            "/scielo.php?script=sci_issuetoc&pid=0203-199820200005",
            "/scielo.php?script=sci_issuetoc&pid=1213-199821211115",
            "/scielo.php?script=sci_pdf&pid=S0001-30352020000501101",
            "/scielo.php?script=sci_pdf&pid=S0001-37652020000501101",
            "/scielo.php?script=sci_pdf&pid=S0203-19982020000501101",
            "/scielo.php?script=sci_pdf&pid=S1213-19982121111511111",
            "/scielo.php?script=sci_pdf&pid=S1213-19982121111511112",
            "/scielo.php?script=sci_pdf&pid=S1213-19982121111511113",
            "/scielo.php?script=sci_pdf&pid=S1213-19982121111511114",
            "/scielo.php?script=sci_serial&pid=0001-3035",
            "/scielo.php?script=sci_serial&pid=0001-3765",
            "/scielo.php?script=sci_serial&pid=0203-1998",
            "/scielo.php?script=sci_serial&pid=1213-1998",
        ]
        self.kwargs["ti"].xcom_pull.return_value = uri_list
        result = get_uri_items_grouped_by_script_name(**self.kwargs)
        self.assertTrue(result)
        self.kwargs["ti"].xcom_pull.assert_called_once_with(
            task_ids="merge_uri_items_from_different_sources_id",
            key="uri_items"
        )
        self.assertIn(
            call("Total %i URIs", 26),
            mock_info.call_args_list
        )
        self.assertIn(
            call(
                'sci_arttext',
                [
                    "/scielo.php?script=sci_arttext&pid=S0001-30352020000501101",
                    "/scielo.php?script=sci_arttext&pid=S0001-37652020000501101",
                    "/scielo.php?script=sci_arttext&pid=S0203-19982020000501101",
                    "/scielo.php?script=sci_arttext&pid=S1213-19982121111511111",
                    "/scielo.php?script=sci_arttext&pid=S1213-19982121111511112",
                    "/scielo.php?script=sci_arttext&pid=S1213-19982121111511113",
                    "/scielo.php?script=sci_arttext&pid=S1213-19982121111511114",
                ]
            ),
            self.kwargs["ti"].xcom_push.call_args_list
        )
        self.assertIn(
            call(
                'sci_pdf',
                [
                    "/scielo.php?script=sci_pdf&pid=S0001-30352020000501101",
                    "/scielo.php?script=sci_pdf&pid=S0001-37652020000501101",
                    "/scielo.php?script=sci_pdf&pid=S0203-19982020000501101",
                    "/scielo.php?script=sci_pdf&pid=S1213-19982121111511111",
                    "/scielo.php?script=sci_pdf&pid=S1213-19982121111511112",
                    "/scielo.php?script=sci_pdf&pid=S1213-19982121111511113",
                    "/scielo.php?script=sci_pdf&pid=S1213-19982121111511114",
                ]
            ),
            self.kwargs["ti"].xcom_push.call_args_list
        )
        self.assertIn(
            call(
                'sci_issuetoc',
                [
                    "/scielo.php?script=sci_issuetoc&pid=0001-303520200005",
                    "/scielo.php?script=sci_issuetoc&pid=0001-376520200005",
                    "/scielo.php?script=sci_issuetoc&pid=0203-199820200005",
                    "/scielo.php?script=sci_issuetoc&pid=1213-199821211115",
                ]
            ),
            self.kwargs["ti"].xcom_push.call_args_list
        )
        self.assertIn(
            call(
                'sci_issues',
                [
                    "/scielo.php?script=sci_issues&pid=0001-3035",
                    "/scielo.php?script=sci_issues&pid=0001-3765",
                    "/scielo.php?script=sci_issues&pid=0203-1998",
                    "/scielo.php?script=sci_issues&pid=1213-1998",
                ]
            ),
            self.kwargs["ti"].xcom_push.call_args_list
        )
        self.assertIn(
            call(
                'sci_serial',
                [
                    "/scielo.php?script=sci_serial&pid=0001-3035",
                    "/scielo.php?script=sci_serial&pid=0001-3765",
                    "/scielo.php?script=sci_serial&pid=0203-1998",
                    "/scielo.php?script=sci_serial&pid=1213-1998",
                ]
            ),
            self.kwargs["ti"].xcom_push.call_args_list
        )

    @patch("check_website.Logger.info")
    def test_get_uri_items_grouped_by_script_name_returns_false(self, mock_info):
        bad_uri_list = [
            "/scielo.php?param1=sci_arttext&pid=S0001-30352020000501101",
        ]
        self.kwargs["ti"].xcom_pull.return_value = bad_uri_list
        result = get_uri_items_grouped_by_script_name(**self.kwargs)
        self.assertFalse(result)
        self.kwargs["ti"].xcom_push.assert_not_called()


class TestGetWebsiteURLlist(TestCase):
    @patch("check_website.Variable.get")
    def test_get_website_url_list_raises_value_error_if_variable_WEBSITE_URL_LIST_is_not_set(self, mock_get):
        mock_get.return_value = []
        with self.assertRaises(ValueError):
            get_website_url_list()


class TestCheckSciSerialUriItems(TestCase):
    def setUp(self):
        self.kwargs = {
            "ti": MagicMock(),
            "conf": None,
            "run_id": "test_run_id",
        }

    @patch("check_website.check_any_uri_items")
    def test_check_sci_serial_uri_items_assert_called_xcom_pull_with_sci_serial_value(self, mock_check_any):
        mock_check_any.return_value = 0
        check_sci_serial_uri_items(**self.kwargs)
        self.kwargs["ti"].xcom_pull.assert_called_once_with(
            task_ids="get_uri_items_grouped_by_script_name_id",
            key="sci_serial"
        )

    @patch("check_website.check_any_uri_items")
    def test_check_sci_serial_uri_items_assert_called_once_check_any_uri_list(
            self, mock_check_any_uri_items):
        self.kwargs["ti"].xcom_pull.return_value = [
            "/scielo.php?script=sci_serial&pid=0001-3035",
            "/scielo.php?script=sci_serial&pid=0001-3765",
        ]
        mock_check_any_uri_items.return_value = 2
        check_sci_serial_uri_items(**self.kwargs)
        mock_check_any_uri_items.assert_called_once_with(
            [
                "/scielo.php?script=sci_serial&pid=0001-3035",
                "/scielo.php?script=sci_serial&pid=0001-3765",
            ],
            "sci_serial",
            self.kwargs
        )


class TestCheckSciIssuesUriItems(TestCase):
    def setUp(self):
        self.kwargs = {
            "ti": MagicMock(),
            "conf": None,
            "run_id": "test_run_id",
        }

    @patch("check_website.check_any_uri_items")
    def test_check_sci_issues_uri_items_assert_called_xcom_pull_with_sci_issues_value(self, mock_check_any):
        mock_check_any.return_value = 0
        check_sci_issues_uri_items(**self.kwargs)
        self.kwargs["ti"].xcom_pull.assert_called_once_with(
            task_ids="get_uri_items_grouped_by_script_name_id",
            key="sci_issues"
        )

    @patch("check_website.check_any_uri_items")
    def test_check_sci_issues_uri_items_assert_called_once_check_any_uri_items(
            self, mock_check_any_uri_items):
        self.kwargs["ti"].xcom_pull.return_value = [
            "/scielo.php?script=sci_issues&pid=0001-3035",
            "/scielo.php?script=sci_issues&pid=0001-3765",
        ]
        mock_check_any_uri_items.return_value = 2
        check_sci_issues_uri_items(**self.kwargs)
        mock_check_any_uri_items.assert_called_once_with(
            [
                "/scielo.php?script=sci_issues&pid=0001-3035",
                "/scielo.php?script=sci_issues&pid=0001-3765",
            ],
            "sci_issues",
            self.kwargs
        )


class TestCheckSciIssuetocUriItems(TestCase):
    def setUp(self):
        self.kwargs = {
            "ti": MagicMock(),
            "conf": None,
            "run_id": "test_run_id",
        }

    @patch("check_website.check_any_uri_items")
    def test_check_sci_issuetoc_uri_items_assert_called_xcom_pull_with_sci_issuetoc_value(self, mock_check_any):
        mock_check_any.return_value = 0
        check_sci_issuetoc_uri_items(**self.kwargs)
        self.kwargs["ti"].xcom_pull.assert_called_once_with(
            task_ids="get_uri_items_grouped_by_script_name_id",
            key="sci_issuetoc"
        )

    @patch("check_website.check_any_uri_items")
    def test_check_sci_issuetoc_uri_items_assert_called_once_check_any_uri_list(
            self, mock_check_any_uri_list):
        self.kwargs["ti"].xcom_pull.return_value = [
            "/scielo.php?script=sci_issuetoc&pid=0001-303520200005",
            "/scielo.php?script=sci_issuetoc&pid=0001-376520200005",
        ]
        mock_check_any_uri_list.return_value = 2
        check_sci_issuetoc_uri_items(**self.kwargs)
        mock_check_any_uri_list.assert_called_once_with(
            [
                "/scielo.php?script=sci_issuetoc&pid=0001-303520200005",
                "/scielo.php?script=sci_issuetoc&pid=0001-376520200005",
            ],
            "sci_issuetoc",
            self.kwargs
        )


class TestCheckSciPdfUriItems(TestCase):
    def setUp(self):
        self.kwargs = {
            "ti": MagicMock(),
            "conf": None,
            "run_id": "test_run_id",
        }

    @patch("check_website.check_any_uri_items")
    def test_check_sci_pdf_uri_items_assert_called_xcom_pull_with_sci_pdf_value(self, mock_check_any):
        mock_check_any.return_value = 0
        check_sci_pdf_uri_items(**self.kwargs)

        self.kwargs["ti"].xcom_pull.assert_called_once_with(
            task_ids="get_uri_items_grouped_by_script_name_id",
            key="sci_pdf"
        )

    @patch("check_website.check_any_uri_items")
    def test_check_sci_pdf_uri_items_assert_called_once_check_any_uri_list(
            self, mock_check_any_uri_list):
        self.kwargs["ti"].xcom_pull.return_value = [
            "/scielo.php?script=sci_pdf&pid=0001-303520200005",
            "/scielo.php?script=sci_pdf&pid=0001-376520200005",
        ]
        mock_check_any_uri_list.return_value = 2
        check_sci_pdf_uri_items(**self.kwargs)
        mock_check_any_uri_list.assert_called_once_with(
            [
                "/scielo.php?script=sci_pdf&pid=0001-303520200005",
                "/scielo.php?script=sci_pdf&pid=0001-376520200005",
            ],
            "sci_pdf",
            self.kwargs
        )


class TestCheckSciArttextUriItems(TestCase):
    def setUp(self):
        self.kwargs = {
            "ti": MagicMock(),
            "conf": None,
            "run_id": "test_run_id",
        }

    @patch("check_website.check_any_uri_items")
    def test_check_sci_arttext_uri_items_assert_called_xcom_pull_with_sci_arttext_value(
            self, mock_check_any_uri_list):
        mock_check_any_uri_list.return_value = 0
        check_sci_arttext_uri_items(**self.kwargs)
        self.kwargs["ti"].xcom_pull.assert_called_once_with(
            task_ids="get_uri_items_grouped_by_script_name_id",
            key="sci_arttext"
        )

    @patch("check_website.check_any_uri_items")
    def test_check_sci_arttext_uri_items_assert_called_once_check_any_uri_list(
            self, mock_check_any_uri_list):
        self.kwargs["ti"].xcom_pull.return_value = [
            "/scielo.php?script=sci_arttext&pid=0001-303520200005",
            "/scielo.php?script=sci_arttext&pid=0001-376520200005",
        ]
        mock_check_any_uri_list.return_value = 2
        check_sci_arttext_uri_items(**self.kwargs)
        mock_check_any_uri_list.assert_called_once_with(
            [
                "/scielo.php?script=sci_arttext&pid=0001-303520200005",
                "/scielo.php?script=sci_arttext&pid=0001-376520200005",
            ],
            "sci_arttext",
            self.kwargs
        )


class TestCheckAnyUriItems(TestCase):
    def setUp(self):
        self.kwargs = {
            "ti": MagicMock(),
            "conf": None,
            "run_id": "test_run_id",
        }

    @patch("check_website.Variable.get")
    def test_check_any_uri_items_returns_0_because_flag_is_off(self, mock_get):
        uri_items = [
            "/scielo.php?script=sci_issues&pid=0001-3035",
            "/scielo.php?script=sci_issues&pid=0001-3765",
        ]
        mock_get.return_value = False
        result = check_any_uri_items(uri_items, "label", {"daginfo": ""})
        self.assertEqual(0, result)

    @patch("check_website.Variable.get")
    def test_check_any_uri_items_raises_value_error(self, mock_get):
        uri_items = [
            "/scielo.php?script=sci_issues&pid=0001-3035",
            "/scielo.php?script=sci_issues&pid=0001-3765",
        ]
        mock_get.return_value = None
        with self.assertRaises(ValueError):
            check_any_uri_items(uri_items, "label", {"daginfo": ""})

    @patch("check_website.Variable.get")
    def test_check_any_uri_items_returns_0_because_uri_items_is_None(self, mock_get):
        uri_items = None
        mock_get.return_value = ["https://www.scielo.br"]
        result = check_any_uri_items(uri_items, "label", {"daginfo": ""})
        self.assertEqual(0, result)

    @patch("check_website.check_website_operations.concat_website_url_and_uri_list_items")
    @patch("check_website.Variable.get")
    def test_check_any_uri_items_assert_called_once_concat_website_url_and_uri_list_items(
            self, mock_get, mock_concat_website_url_and_uri_list_items):
        uri_items = [
            "/scielo.php?script=sci_issues&pid=0001-3035",
            "/scielo.php?script=sci_issues&pid=0001-3765",
        ]
        mock_get.return_value = ["https://www.scielo.br"]
        mock_concat_website_url_and_uri_list_items.return_value = [
            "https://www.scielo.br/scielo.php?script=sci_issues&pid=0001-3035",
            "https://www.scielo.br/scielo.php?script=sci_issues&pid=0001-3765",
        ]
        result = check_any_uri_items(uri_items, "label", {"daginfo": ""})
        self.assertEqual(2, result)
        mock_concat_website_url_and_uri_list_items.assert_called_once_with(
            ["https://www.scielo.br"],
            [
                "/scielo.php?script=sci_issues&pid=0001-3035",
                "/scielo.php?script=sci_issues&pid=0001-3765",
            ]
        )

    @patch("check_website.check_website_operations.check_website_uri_list")
    @patch("check_website.Variable.get")
    def test_check_any_uri_items_assert_called_once_check_website_uri_list(
            self, mock_get, mock_check_website_uri_list):
        uri_items = [
            "/scielo.php?script=sci_issues&pid=0001-3035",
            "/scielo.php?script=sci_issues&pid=0001-3765",
        ]
        mock_get.return_value = ["https://www.scielo.br"]
        mock_check_website_uri_list.return_value = MagicMock(), MagicMock()
        result = check_any_uri_items(uri_items, "label", self.kwargs)
        self.assertEqual(2, result)
        mock_check_website_uri_list.assert_called_once_with(
            [
                "https://www.scielo.br/scielo.php?script=sci_issues&pid=0001-3035",
                "https://www.scielo.br/scielo.php?script=sci_issues&pid=0001-3765",
            ],
            "label"
        )

    @patch("check_website.check_website_operations.register_sci_pages_availability_report")
    @patch("check_website.check_website_operations.check_website_uri_list")
    @patch("check_website.Variable.get")
    def test_check_any_uri_items_assert_calls_register_sci_pages_availability_report(
            self, mock_get, mock_check_website_uri_list,
            mock_register_sci_pages_availability_report,
            ):
        uri_items = [
            "/scielo.php?script=sci_issues&pid=0001-3035",
            "/scielo.php?script=sci_issues&pid=0001-3765",
        ]
        mock_get.return_value = ["https://new.scielo.br", "https://www.scielo.br"]

        success = [
                {
                    "available": True,
                    "status code": 200,
                    "start time": 1,
                    "end time": 2,
                    "duration": 1,
                    "uri": "https://www.scielo.br/scielo.php?script=sci_issues&pid=0001-3035"
                },
                {
                    "available": True,
                    "status code": 200,
                    "start time": 1,
                    "end time": 2,
                    "duration": 1,
                    "uri": "https://www.scielo.br/scielo.php?script=sci_issues&pid=0001-3765"
                },
            ]
        failures = [
            {
                "available": False,
                "status code": 404,
                "start time": 1,
                "end time": 2,
                "duration": 1,
                "uri": "https://new.scielo.br/scielo.php?script=sci_issues&pid=0001-3035",
            },
            {
                "available": False,
                "status code": 404,
                "start time": 1,
                "end time": 2,
                "duration": 1,
                "uri": "https://new.scielo.br/scielo.php?script=sci_issues&pid=0001-3765"
            },
        ]
        mock_check_website_uri_list.return_value = (
            success,
            failures,
        )
        dag_info = {"dag": "daginfo"}
        result = check_any_uri_items(uri_items, "label", dag_info)
        self.assertEqual(4, result)

        calls = [
            call(failures, dag_info),
            call(success, dag_info),
        ]
        self.assertListEqual(
            calls, mock_register_sci_pages_availability_report.call_args_list
        )

    @patch("check_website.check_website_operations.add_execution_in_database")
    @patch("check_website.check_website_operations.check_website_uri_list")
    @patch("check_website.Variable.get")
    def test_check_any_uri_items_assert_calls_add_execution_in_database(
            self, mock_get, mock_check_website_uri_list,
            mock_add_execution_in_database,
            ):
        uri_items = [
            "/scielo.php?script=sci_issues&pid=0001-3035",
            "/scielo.php?script=sci_issues&pid=0001-3765",
        ]
        mock_get.return_value = ["https://new.scielo.br", "https://www.scielo.br"]

        success = [
                {
                    "available": True,
                    "status code": 200,
                    "start time": 1,
                    "end time": 2,
                    "duration": 1,
                    "uri": "https://www.scielo.br/scielo.php?script=sci_issues&pid=0001-3035"
                },
                {
                    "available": True,
                    "status code": 200,
                    "start time": 1,
                    "end time": 2,
                    "duration": 1,
                    "uri": "https://www.scielo.br/scielo.php?script=sci_issues&pid=0001-3765"
                },
            ]
        failures = [
            {
                "available": False,
                "status code": 404,
                "start time": 1,
                "end time": 2,
                "duration": 1,
                "uri": "https://new.scielo.br/scielo.php?script=sci_issues&pid=0001-3035",
            },
            {
                "available": False,
                "status code": 404,
                "start time": 1,
                "end time": 2,
                "duration": 1,
                "uri": "https://new.scielo.br/scielo.php?script=sci_issues&pid=0001-3765"
            },
        ]
        mock_check_website_uri_list.return_value = (
            success,
            failures,
        )
        dag_info = {}
        expected = [
            {
                "dag_run": None,
                "input_file_name": None,
                "uri": "https://new.scielo.br/scielo.php?script=sci_issues&pid=0001-3035",
                "failed": True,
                "detail": '{"available": false, "status code": 404, '
                          '"start time": 1, "end time": 2, "duration": 1, '
                          '"uri": "https://new.scielo.br/scielo.php?script='
                          'sci_issues&pid=0001-3035"}',
                "pid_v2_journal": "0001-3035",
                "pid_v2_issue": None,
                "pid_v2_doc": None,
            },
            {
                "dag_run": None,
                "input_file_name": None,
                "uri": "https://new.scielo.br/scielo.php?script=sci_issues&pid=0001-3765",
                "failed": True,
                "detail": '{"available": false, "status code": 404, '
                          '"start time": 1, "end time": 2, "duration": 1, '
                          '"uri": "https://new.scielo.br/scielo.php?script='
                          'sci_issues&pid=0001-3765"}',
                "pid_v2_journal": "0001-3765",
                "pid_v2_issue": None,
                "pid_v2_doc": None,
            },
            {
                "dag_run": None,
                "input_file_name": None,
                "uri": "https://www.scielo.br/scielo.php?script=sci_issues&pid=0001-3035",
                "failed": False,
                "detail": '{"available": true, "status code": 200, '
                          '"start time": 1, "end time": 2, "duration": 1, '
                          '"uri": "https://www.scielo.br/scielo.php?script='
                          'sci_issues&pid=0001-3035"}',
                "pid_v2_journal": "0001-3035",
                "pid_v2_issue": None,
                "pid_v2_doc": None,
            },
            {
                "dag_run": None,
                "input_file_name": None,
                "uri": "https://www.scielo.br/scielo.php?script=sci_issues&pid=0001-3765",
                "failed": False,
                "detail": '{"available": true, "status code": 200, '
                          '"start time": 1, "end time": 2, "duration": 1, '
                          '"uri": "https://www.scielo.br/scielo.php?script='
                          'sci_issues&pid=0001-3765"}',
                "pid_v2_journal": "0001-3765",
                "pid_v2_issue": None,
                "pid_v2_doc": None,
            },
        ]
        result = check_any_uri_items(uri_items, "label", dag_info)
        self.assertEqual(4, result)
        calls = [
            call("sci_pages_availability", expected[0]),
            call("sci_pages_availability", expected[1]),
            call("sci_pages_availability", expected[2]),
            call("sci_pages_availability", expected[3]),
        ]
        self.assertListEqual(
            calls, mock_add_execution_in_database.call_args_list
        )

    @patch("check_website.check_website_operations.add_execution_in_database")
    @patch("check_website.check_website_operations.check_website_uri_list")
    @patch("check_website.Variable.get")
    def test_check_any_uri_items_assert_calls_add_execution_in_database_with_dag_info(
            self, mock_get, mock_check_website_uri_list,
            mock_add_execution_in_database,
            ):
        uri_items = [
            "/scielo.php?script=sci_issues&pid=0001-3035",
            "/scielo.php?script=sci_issues&pid=0001-3765",
        ]
        mock_get.return_value = ["https://new.scielo.br", "https://www.scielo.br"]

        success = [
                {
                    "available": True,
                    "status code": 200,
                    "start time": 1,
                    "end time": 2,
                    "duration": 1,
                    "uri": "https://www.scielo.br/scielo.php?script=sci_issues&pid=0001-3035"
                },
                {
                    "available": True,
                    "status code": 200,
                    "start time": 1,
                    "end time": 2,
                    "duration": 1,
                    "uri": "https://www.scielo.br/scielo.php?script=sci_issues&pid=0001-3765"
                },
            ]
        failures = [
            {
                "available": False,
                "status code": 404,
                "start time": 1,
                "end time": 2,
                "duration": 1,
                "uri": "https://new.scielo.br/scielo.php?script=sci_issues&pid=0001-3035",
            },
            {
                "available": False,
                "status code": 404,
                "start time": 1,
                "end time": 2,
                "duration": 1,
                "uri": "https://new.scielo.br/scielo.php?script=sci_issues&pid=0001-3765"
            },
        ]
        mock_check_website_uri_list.return_value = (
            success,
            failures,
        )
        dag_info = {"run_id": "RUNID", "input_file_name": "a.csv", "k": "v"}
        expected = [
            {
                "dag_run": "RUNID",
                "input_file_name": "a.csv",
                "uri": "https://new.scielo.br/scielo.php?script=sci_issues&pid=0001-3035",
                "failed": True,
                "detail": '{"available": false, "status code": 404, '
                          '"start time": 1, "end time": 2, "duration": 1, '
                          '"uri": "https://new.scielo.br/scielo.php?script='
                          'sci_issues&pid=0001-3035"}',
                "pid_v2_journal": "0001-3035",
                "pid_v2_issue": None,
                "pid_v2_doc": None,
            },
            {
                "dag_run": "RUNID",
                "input_file_name": "a.csv",
                "uri": "https://new.scielo.br/scielo.php?script=sci_issues&pid=0001-3765",
                "failed": True,
                "detail": '{"available": false, "status code": 404, '
                          '"start time": 1, "end time": 2, "duration": 1, '
                          '"uri": "https://new.scielo.br/scielo.php?script='
                          'sci_issues&pid=0001-3765"}',
                "pid_v2_journal": "0001-3765",
                "pid_v2_issue": None,
                "pid_v2_doc": None,
            },
            {
                "dag_run": "RUNID",
                "input_file_name": "a.csv",
                "uri": "https://www.scielo.br/scielo.php?script=sci_issues&pid=0001-3035",
                "failed": False,
                "detail": '{"available": true, "status code": 200, '
                          '"start time": 1, "end time": 2, "duration": 1, '
                          '"uri": "https://www.scielo.br/scielo.php?script='
                          'sci_issues&pid=0001-3035"}',
                "pid_v2_journal": "0001-3035",
                "pid_v2_issue": None,
                "pid_v2_doc": None,
            },
            {
                "dag_run": "RUNID",
                "input_file_name": "a.csv",
                "uri": "https://www.scielo.br/scielo.php?script=sci_issues&pid=0001-3765",
                "failed": False,
                "detail": '{"available": true, "status code": 200, '
                          '"start time": 1, "end time": 2, "duration": 1, '
                          '"uri": "https://www.scielo.br/scielo.php?script='
                          'sci_issues&pid=0001-3765"}',
                "pid_v2_journal": "0001-3765",
                "pid_v2_issue": None,
                "pid_v2_doc": None,
            },
        ]
        result = check_any_uri_items(uri_items, "label", dag_info)
        self.assertEqual(4, result)
        calls = [
            call("sci_pages_availability", expected[0]),
            call("sci_pages_availability", expected[1]),
            call("sci_pages_availability", expected[2]),
            call("sci_pages_availability", expected[3]),
        ]
        self.assertListEqual(
            calls, mock_add_execution_in_database.call_args_list
        )

    @patch("check_website.check_website_operations.add_execution_in_database")
    @patch("check_website.check_website_operations.async_requests.parallel_requests")
    @patch("check_website.Variable.get")
    def test_check_any_uri_items_execute_all_except_uri_request_and_db_registration(
            self, mock_get,
            mock_request,
            mock_add_execution_in_database,
            ):
        uri_items = [
            "/scielo.php?script=sci_issues&pid=0001-3035",
            "/scielo.php?script=sci_issues&pid=0001-3765",
        ]
        mock_get.return_value = [
            "https://new.scielo.br", "https://www.scielo.br"]
        mock_request.return_value = [
            MockClientResponse(
                404,
                "https://new.scielo.br/scielo.php?script=sci_issues&pid=0001-3035",
            ),
            MockClientResponse(
                404,
                "https://new.scielo.br/scielo.php?script=sci_issues&pid=0001-3765",
            ),
            MockClientResponse(
                200,
                "https://www.scielo.br/scielo.php?script=sci_issues&pid=0001-3035",
            ),
            MockClientResponse(
                200,
                "https://www.scielo.br/scielo.php?script=sci_issues&pid=0001-3765",
            ),
        ]

        dag_info = {"run_id": "RUNID", "input_file_name": "a.csv", "k": "v"}
        dt0 = START_TIME.isoformat() + "Z"
        dt1 = END_TIME.isoformat() + "Z"
        expected = [
            {
                "dag_run": "RUNID",
                "input_file_name": "a.csv",
                "uri": "https://new.scielo.br/scielo.php?script=sci_issues&pid=0001-3035",
                "failed": True,
                "detail": '{"available": false, "status code": 404, '
                          '"start time": "%s", "end time": "%s", '
                          '"duration": 9e-06, '
                          '"uri": "https://new.scielo.br/scielo.php?script='
                          'sci_issues&pid=0001-3035"}'.replace(
                            '9e-06', str(DURATION)) % (dt0, dt1),
                "pid_v2_journal": "0001-3035",
                "pid_v2_issue": None,
                "pid_v2_doc": None,
            },
            {
                "dag_run": "RUNID",
                "input_file_name": "a.csv",
                "uri": "https://new.scielo.br/scielo.php?script=sci_issues&pid=0001-3765",
                "failed": True,
                "detail": '{"available": false, "status code": 404, '
                          '"start time": "%s", "end time": "%s", '
                          '"duration": 9e-06, '
                          '"uri": "https://new.scielo.br/scielo.php?script='
                          'sci_issues&pid=0001-3765"}'.replace(
                            '9e-06', str(DURATION)) % (dt0, dt1),
                "pid_v2_journal": "0001-3765",
                "pid_v2_issue": None,
                "pid_v2_doc": None,
            },
            {
                "dag_run": "RUNID",
                "input_file_name": "a.csv",
                "uri": "https://www.scielo.br/scielo.php?script=sci_issues&pid=0001-3035",
                "failed": False,
                "detail": '{"available": true, "status code": 200, '
                          '"start time": "%s", "end time": "%s", '
                          '"duration": 9e-06, '
                          '"uri": "https://www.scielo.br/scielo.php?script='
                          'sci_issues&pid=0001-3035"}'.replace(
                            '9e-06', str(DURATION)) % (dt0, dt1),
                "pid_v2_journal": "0001-3035",
                "pid_v2_issue": None,
                "pid_v2_doc": None,
            },
            {
                "dag_run": "RUNID",
                "input_file_name": "a.csv",
                "uri": "https://www.scielo.br/scielo.php?script=sci_issues&pid=0001-3765",
                "failed": False,
                "detail": '{"available": true, "status code": 200, '
                          '"start time": "%s", "end time": "%s", '
                          '"duration": 9e-06, '
                          '"uri": "https://www.scielo.br/scielo.php?script='
                          'sci_issues&pid=0001-3765"}'.replace(
                            '9e-06', str(DURATION)) % (dt0, dt1),
                "pid_v2_journal": "0001-3765",
                "pid_v2_issue": None,
                "pid_v2_doc": None,
            },
        ]
        result = check_any_uri_items(uri_items, "label", dag_info)
        self.assertEqual(4, result)
        calls = [
            call("sci_pages_availability", expected[0]),
            call("sci_pages_availability", expected[1]),
            call("sci_pages_availability", expected[2]),
            call("sci_pages_availability", expected[3]),
        ]
        self.assertListEqual(
            calls, mock_add_execution_in_database.call_args_list
        )


class TestGetPIDv3List(TestCase):
    def setUp(self):
        self.kwargs = {
            "ti": MagicMock(),
            "conf": None,
            "run_id": "test_run_id",
        }

    @patch("check_website.Variable.get")
    def test_get_pid_v3_list_raises_value_error_because_website_url_list_is_not_set(
            self, mock_variabel_get):
        mock_variabel_get.return_value = None

        with self.assertRaises(ValueError) as exc_info:
            get_pid_v3_list(**self.kwargs)
        self.assertIn(
            "`Variable[\"WEBSITE_URL_LIST\"]` is required",
            str(exc_info.exception)
        )

    @patch("check_website.check_website_operations.get_main_website_url")
    @patch("check_website.Variable.get")
    def test_get_pid_v3_list_raises_value_error_because_main_website_url_is_not_set(
            self, mock_variabel_get, mock_get_main_website_url):
        mock_variabel_get.return_value = ["https://www.scielo.br"]
        mock_get_main_website_url.return_value = None

        with self.assertRaises(ValueError) as exc_info:
            get_pid_v3_list(**self.kwargs)
        self.assertIn(
            "Unable to identify which one is the (new) SciELO website "
            "in this list: ['https://www.scielo.br']",
            str(exc_info.exception)
        )

    @patch("check_website.check_website_operations.get_main_website_url")
    @patch("check_website.Variable.get")
    def test_get_pid_v3_list_raises_value_error_because_xcom_pull_with_sci_arttext_is_none(
            self, mock_variabel_get, mock_get_main_website_url):
        mock_variabel_get.return_value = ["https://www.scielo.br"]
        mock_get_main_website_url.return_value = (
            "https://www.scielo.br"
        )
        self.kwargs["ti"].xcom_pull.return_value = None
        with self.assertRaises(ValueError) as exc_info:
            get_pid_v3_list(**self.kwargs)
        self.assertIn(
            "Missing URI items to get PID v3",
            str(exc_info.exception)
        )

    @patch("check_website.get_website_url_list")
    @patch("check_website.check_website_operations.get_main_website_url")
    @patch("check_website.check_website_operations.get_pid_v3_list")
    def test_get_pid_v3_list_assert_called_xcom_pull_with_sci_arttext_value(
            self, mock_get, mock_get_main_website_url,
            mock_get_website_url_list):
        mock_get.return_value = (
            ["DOCID1", "DOCID2"]
        )
        mock_get_main_website_url.return_value = (
            "https://www.scielo.br"
        )
        mock_get_website_url_list.return_value = [
            "https://www.scielo.br"
        ]
        self.kwargs["ti"].xcom_pull.return_value = [
            "/scielo.php?script=sci_arttext&pid=x",
            "/scielo.php?script=sci_arttext&pid=y",
        ]
        result = get_pid_v3_list(**self.kwargs)
        self.assertTrue(result)
        self.kwargs["ti"].xcom_pull.assert_called_once_with(
            task_ids="get_uri_items_grouped_by_script_name_id",
            key="sci_arttext"
        )

    @patch("check_website.get_website_url_list")
    @patch("check_website.check_website_operations.get_main_website_url")
    @patch("check_website.check_website_operations.get_pid_v3_list")
    def test_get_pid_v3_list_assert_called_xcom_push_with_pid_v3_list(
            self, mock_get, mock_get_main_website_url,
            mock_get_website_url_list):
        mock_get.return_value = (
            ["DOCID1", "DOCID2"]
        )
        mock_get_main_website_url.return_value = (
            "https://www.scielo.br"
        )
        mock_get_website_url_list.return_value = [
            "https://www.scielo.br"
        ]
        self.kwargs["ti"].xcom_pull.return_value = [
            "/scielo.php?script=sci_arttext&pid=x",
            "/scielo.php?script=sci_arttext&pid=y",
        ]
        result = get_pid_v3_list(**self.kwargs)
        self.assertTrue(result)
        self.assertListEqual(
            [
                call("pid_v3_list", ["DOCID1", "DOCID2"]),
                call("website_url", "https://www.scielo.br"),
            ],
            self.kwargs["ti"].xcom_push.call_args_list
        )

    @patch("check_website.get_website_url_list")
    @patch("check_website.check_website_operations.get_main_website_url")
    @patch("check_website.check_website_operations.get_pid_v3_list")
    def test_get_pid_v3_list_returns_false(
            self, mock_get, mock_get_main_website_url,
            mock_get_website_url_list):
        mock_get.return_value = None
        mock_get_main_website_url.return_value = (
            "https://www.scielo.br"
        )
        mock_get_website_url_list.return_value = [
            "https://www.scielo.br"
        ]
        self.kwargs["ti"].xcom_pull.return_value = [
            "/scielo.php?script=sci_arttext&pid=x",
            "/scielo.php?script=sci_arttext&pid=y",
        ]
        result = get_pid_v3_list(**self.kwargs)
        self.assertFalse(result)
        self.kwargs["ti"].xcom_push.assert_not_called()


class TestGroupUriItemsFromUriListsByScriptName(TestCase):

    def setUp(self):
        self.kwargs = {
            "ti": MagicMock(),
            "conf": None,
            "run_id": "test_run_id",
        }

    def test_group_uri_items_from_uri_lists_by_script_name_returns_true(self):
        self.kwargs["ti"].xcom_pull.return_value = [
            "/scielo.php?script=sci_arttext&pid=0001-303520200005",
            "/scielo.php?script=sci_arttext&pid=0001-376520200005",
            "/scielo.php?script=sci_pdf&pid=0001-303520200005",
            "/scielo.php?script=sci_pdf&pid=0001-376520200005",
        ]
        result = group_uri_items_from_uri_lists_by_script_name(**self.kwargs)
        self.kwargs["ti"].xcom_pull.assert_called_once_with(
            task_ids="get_uri_items_from_uri_list_files_id",
            key="uri_items"
        )
        self.assertIn(
            call(
                'sci_arttext',
                [
                    "/scielo.php?script=sci_arttext&pid=0001-303520200005",
                    "/scielo.php?script=sci_arttext&pid=0001-376520200005",
                ]
            ),
            self.kwargs["ti"].xcom_push.call_args_list
        )
        self.assertIn(
            call(
                'sci_pdf',
                [
                    "/scielo.php?script=sci_pdf&pid=0001-303520200005",
                    "/scielo.php?script=sci_pdf&pid=0001-376520200005",
                ]
            ),
            self.kwargs["ti"].xcom_push.call_args_list
        )
        self.assertTrue(result)

    @patch("check_website.check_website_operations.group_items_by_script_name")
    def test_group_uri_items_from_uri_lists_by_script_name_returns_false(self, mock_f):
        self.kwargs["ti"].xcom_pull.return_value = [
            "/scielo.php?script=sci_arttext&pid=0001-303520200005",
            "/scielo.php?script=sci_arttext&pid=0001-376520200005",
            "/scielo.php?script=sci_pdf&pid=0001-303520200005",
            "/scielo.php?script=sci_pdf&pid=0001-376520200005",
        ]
        mock_f.return_value = {}
        result = group_uri_items_from_uri_lists_by_script_name(**self.kwargs)
        self.assertFalse(result)
        self.kwargs["ti"].xcom_push.assert_not_called()

    def test_group_uri_items_from_uri_lists_by_script_name_returns_true_because_there_is_no_input(self):
        self.kwargs["ti"].xcom_pull.return_value = None
        result = group_uri_items_from_uri_lists_by_script_name(**self.kwargs)
        self.kwargs["ti"].xcom_pull.assert_called_once_with(
            task_ids="get_uri_items_from_uri_list_files_id",
            key="uri_items"
        )
        self.assertTrue(result)
        self.kwargs["ti"].xcom_push.assert_not_called()


class TestMergeUriItemsFromDifferentSources(TestCase):

    def setUp(self):
        self.kwargs = {
            "ti": MagicMock(),
            "conf": None,
            "run_id": "test_run_id",
        }

    def test_merge_uri_items_from_different_sources_removes_repetition(self):
        self.kwargs["ti"].xcom_pull.side_effect = [
            [
                "/scielo.php?script=sci_arttext&pid=0001-303520200005",
                "/scielo.php?script=sci_arttext&pid=0001-376520200005",
                "/scielo.php?script=sci_pdf&pid=0001-303520200005",
                "/scielo.php?script=sci_pdf&pid=0001-376520200005",
            ],
            [
                "/scielo.php?script=sci_arttext&pid=0001-303520200005",
            ],
        ]
        result = merge_uri_items_from_different_sources(**self.kwargs)
        self.assertTrue(result)
        self.assertListEqual([
            call(key="uri_items",
                 task_ids="get_uri_items_from_uri_list_files_id",),
            call(key="uri_items",
                 task_ids="get_uri_items_from_pid_list_csv_files_id",)
            ],
            self.kwargs["ti"].xcom_pull.call_args_list
        )
        self.kwargs["ti"].xcom_push.assert_called_once_with(
            "uri_items",
            [
                "/scielo.php?script=sci_arttext&pid=0001-303520200005",
                "/scielo.php?script=sci_arttext&pid=0001-376520200005",
                "/scielo.php?script=sci_pdf&pid=0001-303520200005",
                "/scielo.php?script=sci_pdf&pid=0001-376520200005",
            ]
        )

    def test_merge_uri_items_from_different_sources_pulls_three_uri_items_and_pushes_three_uri_items(self):
        self.kwargs["ti"].xcom_pull.side_effect = [
            [
                "/scielo.php?script=sci_pdf&pid=0001-303520200005",
                "/scielo.php?script=sci_pdf&pid=0001-376520200005",
            ],
            [
                "/scielo.php?script=sci_arttext&pid=0001-303520200005",
            ],
        ]
        result = merge_uri_items_from_different_sources(**self.kwargs)
        self.assertTrue(result)
        self.assertListEqual([
            call(key="uri_items",
                 task_ids="get_uri_items_from_uri_list_files_id",),
            call(key="uri_items",
                 task_ids="get_uri_items_from_pid_list_csv_files_id",)
            ],
            self.kwargs["ti"].xcom_pull.call_args_list
        )
        self.kwargs["ti"].xcom_push.assert_called_once_with(
            "uri_items",
            [
                "/scielo.php?script=sci_arttext&pid=0001-303520200005",
                "/scielo.php?script=sci_pdf&pid=0001-303520200005",
                "/scielo.php?script=sci_pdf&pid=0001-376520200005",
            ]
        )

    def test_merge_uri_items_from_different_sources_returns_true_only_from_csv(self):
        self.kwargs["ti"].xcom_pull.side_effect = [
            None,
            [
                "/scielo.php?script=sci_arttext&pid=0001-303520200005",
            ],
        ]
        result = merge_uri_items_from_different_sources(**self.kwargs)
        self.assertTrue(result)
        self.assertListEqual([
            call(key="uri_items",
                 task_ids="get_uri_items_from_uri_list_files_id",),
            call(key="uri_items",
                 task_ids="get_uri_items_from_pid_list_csv_files_id",)
            ],
            self.kwargs["ti"].xcom_pull.call_args_list
        )
        self.kwargs["ti"].xcom_push.assert_called_once_with(
            "uri_items",
            [
                "/scielo.php?script=sci_arttext&pid=0001-303520200005",
            ]
        )

    def test_merge_uri_items_from_different_sources_returns_true_only_from_lst(self):
        self.kwargs["ti"].xcom_pull.side_effect = [
            [
                "/scielo.php?script=sci_arttext&pid=0001-303520200005",
            ],
            None,
        ]
        result = merge_uri_items_from_different_sources(**self.kwargs)
        self.assertTrue(result)
        self.assertListEqual([
            call(key="uri_items",
                 task_ids="get_uri_items_from_uri_list_files_id",),
            call(key="uri_items",
                 task_ids="get_uri_items_from_pid_list_csv_files_id",)
            ],
            self.kwargs["ti"].xcom_pull.call_args_list
        )
        self.kwargs["ti"].xcom_push.assert_called_once_with(
            "uri_items",
            [
                "/scielo.php?script=sci_arttext&pid=0001-303520200005",
            ]
        )


class TestCheckInputVsProcessedPids(TestCase):

    def setUp(self):
        self.kwargs = {
            "ti": MagicMock(),
            "conf": None,
            "run_id": "test_run_id",
        }

    def test_check_input_vs_processed_pids_gets_merge_pids_from_different_tasks(self):
        self.kwargs["ti"].xcom_pull.side_effect = [
            [
                "0001-376520200005",
            ],
            [
                "0001-303520200005",
                "0001-376520200005",
            ],
            [
                "0001-303520200005",
                "0001-376520200005",
            ],
        ]
        result = check_input_vs_processed_pids(**self.kwargs)
        self.assertTrue(result)

        self.assertListEqual([
            call(key="sci_arttext",
                 task_ids="group_uri_items_from_uri_lists_by_script_name_id",),
            call(key="pid_items",
                 task_ids="get_uri_items_from_pid_list_csv_files_id",),
            call(key="processed_pid_v2_items",
                 task_ids="check_documents_deeply_id",),
            ],
            self.kwargs["ti"].xcom_pull.call_args_list
        )

    @patch("check_website.Logger")
    def test_check_input_vs_processed_pids_registers_success(
            self, mock_logger):
        self.kwargs["ti"].xcom_pull.side_effect = [
            [
                "0001-303520200005",
            ],
            [
                "0001-376520200005",
            ],
            [
                "0001-303520200005", "0001-376520200005",
            ]
        ]
        result = check_input_vs_processed_pids(**self.kwargs)
        self.assertTrue(result)

        self.assertListEqual([
                call(
                    "Check if all the PID items from `uri_list_*.lst` "
                    "and `*.csv` were processed at the end"
                ),
                call("Total %i PIDs v2 from uri_list", 1),
                call("Total %i PIDs v2 from csv", 1),
                call("Total %i input PIDs", 2),
                call("Total %i processed PIDs", 2),
                call("All the PIDs were processed"),
            ],
            mock_logger.info.call_args_list
        )

    @patch("check_website.Logger")
    def test_check_input_vs_processed_pids_registers_error(
            self, mock_logger):
        self.kwargs["ti"].xcom_pull.side_effect = [
            [
                "0001-303520200005",
            ],
            [
                "0001-376520200005",
            ],
            []
        ]
        result = check_input_vs_processed_pids(**self.kwargs)
        self.assertFalse(result)

        self.assertListEqual([
                call(
                    "Check if all the PID items from `uri_list_*.lst` "
                    "and `*.csv` were processed at the end"
                ),
                call("Total %i PIDs v2 from uri_list", 1),
                call("Total %i PIDs v2 from csv", 1),
                call("Total %i input PIDs", 2),
                call("Total %i processed PIDs", 0),
            ],
            mock_logger.info.call_args_list
        )
        self.assertListEqual([
                call(
                    "There are %i PIDs which are in input lists, "
                    "but were not processed:\n%s",
                    2, "0001-303520200005\n0001-376520200005"
                ),
            ],
            mock_logger.error.call_args_list
        )

    @patch("check_website.Logger")
    def test_check_input_vs_processed_pids_registers_warning(
            self, mock_logger):
        self.kwargs["ti"].xcom_pull.side_effect = [
            [
                "0001-303520200005",
            ],
            [
                "0001-376520200005",
            ],
            [
                "0001-303520200005",
                "0001-376520200005",
                "0001-30352020XXX5",
            ]
        ]
        result = check_input_vs_processed_pids(**self.kwargs)
        self.assertTrue(result)

        self.assertListEqual([
                call(
                    "Check if all the PID items from `uri_list_*.lst` "
                    "and `*.csv` were processed at the end"
                ),
                call("Total %i PIDs v2 from uri_list", 1),
                call("Total %i PIDs v2 from csv", 1),
                call("Total %i input PIDs", 2),
                call("Total %i processed PIDs", 3),
                call("All the PIDs were processed"),
            ],
            mock_logger.info.call_args_list
        )
        self.assertListEqual([
                call(
                    "There are %i processed PIDs which were not"
                    " in input lists:\n%s"
                    "(Probably because they are previous PID)",
                    1,
                    "0001-30352020XXX5"
                ),
            ],
            mock_logger.warning.call_args_list
        )


class TestCheckDocumentsDeeply(TestCase):

    def setUp(self):
        self.kwargs = {
            "ti": MagicMock(),
            "conf": None,
            "run_id": "test_run_id",
        }

    @patch("check_website.Logger")
    @patch("check_website.Variable.get")
    def test_check_documents_deeply_return_false_because_all_flag_were_set_as_false(
            self, mock_var_get, mock_logger):
        mock_var_get.side_effect = [
            False, False, False, False
        ]
        result = check_documents_deeply(**self.kwargs)
        self.assertFalse(result)
        mock_logger.warning.assert_called_once_with(
            "'check_documents_deeply_id' was NOT executed because "
            "all FLAGS are set to 'false'"
        )

    @patch("check_website.Logger")
    @patch("check_website.Variable.get")
    def test_check_documents_deeply_raises_value_error(
            self, mock_var_get, mock_logger):
        mock_var_get.side_effect = [
            True, False, False, False
        ]
        self.kwargs["ti"].xcom_pull.return_value = None
        with self.assertRaises(ValueError) as exc_info:
            result = check_documents_deeply(**self.kwargs)
        self.assertEqual(
            str(exc_info.exception),
            "Unable to execute this task because `website_url` is not set"
        )

    @patch("check_website.Logger")
    @patch("check_website.Variable.get")
    def test_check_documents_deeply_registers_no_pid_v3_to_checkup(
            self, mock_var_get, mock_logger):
        mock_var_get.side_effect = [
            True, False, False, False,
            "https://minio.scielo.br",
        ]
        pid_v3_items = None
        self.kwargs["ti"].xcom_pull.side_effect = [
            "https://www.scielo.br",
            pid_v3_items,
        ]
        result = check_documents_deeply(**self.kwargs)
        self.assertFalse(result)
        mock_logger.warning.assert_called_once_with(
            "There is no PID v3 to check"
        )

    @patch("check_website.check_website_operations.check_website_uri_list_deeply")
    @patch("check_website.Logger")
    @patch("check_website.Variable.get")
    def test_check_documents_deeply_registers_returns_true(
            self, mock_var_get, mock_logger, mock_check_website_uri_list):
        mock_var_get.side_effect = [
            True, False, False, False,
            "https://minio.scielo.br",
        ]
        pid_v3_items = ["DOCPIDV3"]
        self.kwargs["ti"].xcom_pull.side_effect = [
            "https://www.scielo.br",
            pid_v3_items,
        ]
        mock_check_website_uri_list.return_value = [
            "PID_V2_1",
            "PID_V2_2",
        ]
        result = check_documents_deeply(**self.kwargs)
        self.assertTrue(result)
        self.kwargs["ti"].xcom_push.assert_called_once_with(
            "processed_pid_v2_items",
            [
                "PID_V2_1",
                "PID_V2_2",
            ]
        )
        self.assertIn(
            call("Checked %i documents", 1),
            mock_logger.info.call_args_list
        )
        self.assertIn(
            call("Checked %i PID v2 items", 2),
            mock_logger.info.call_args_list
        )

    @patch("check_website.check_website_operations.check_website_uri_list_deeply")
    @patch("check_website.Logger")
    @patch("check_website.Variable.get")
    def test_check_documents_deeply_registers_returns_false(
            self, mock_var_get, mock_logger, mock_check_website_uri_list):
        mock_var_get.side_effect = [
            True, False, False, False,
            "https://minio.scielo.br",
        ]
        pid_v3_items = ["DOCPIDV3"]
        self.kwargs["ti"].xcom_pull.side_effect = [
            "https://www.scielo.br",
            pid_v3_items,
        ]
        mock_check_website_uri_list.return_value = None
        result = check_documents_deeply(**self.kwargs)
        self.assertFalse(result)
        self.kwargs["ti"].xcom_push.assert_not_called()

        self.assertIn(
            call("Checked %i documents", 1),
            mock_logger.info.call_args_list
        )
        self.assertIn(
            call("Checked %i PID v2 items", 0),
            mock_logger.info.call_args_list
        )
