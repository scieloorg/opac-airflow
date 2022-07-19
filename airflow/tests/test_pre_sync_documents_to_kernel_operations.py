import os
import tempfile
import shutil
import pathlib
import zipfile
from unittest import TestCase, main
from unittest.mock import patch, MagicMock, Mock, call

from airflow import DAG

from operations.pre_sync_documents_to_kernel_operations import get_sps_packages


def _create_scilista_and_filenames(scilista_file_path, scilista_lines=None):
    """
    Cria uma scilista e 3 nomes de arquivos para cada item da scilista
    """
    scilista_lines = scilista_lines or ["rba v53n1", "rba 2019nahead", "rsp v10n4s1"]
    source_filenames = []
    scilista_file_path = pathlib.Path(scilista_file_path)
    with scilista_file_path.open("w") as scilista_file:
        for line in scilista_lines:
            scilista_file.write(line + "\n")
            source_filenames += [
                "_".join([f"2020-01-01-00-0{i}-09-090901"] + line.split()) + ".zip"
                for i in range(1, 4)
            ]
    return source_filenames


def _create_zip(source_filenames, src_folder, zipfile_path):
    files = []
    for filename in source_filenames:
        zip_filename = pathlib.Path(src_folder) / filename
        files.append(str(zip_filename))
        with zipfile.ZipFile(zip_filename, "w") as zip_file:
            zip_file.write(zipfile_path)
    return files


@patch("operations.pre_sync_documents_to_kernel_operations.get_id_provider_functions")
class TestGetSPSPackagesWithPidManager(TestCase):
    def setUp(self):
        self.xc_dir_name = tempfile.mkdtemp()
        self.proc_dir_name = tempfile.mkdtemp()
        self.kwargs = {
            "scilista_file_path": str(pathlib.Path(self.xc_dir_name) / "scilista.lst"),
            "xc_dir_name": self.xc_dir_name,
            "proc_dir_name": self.proc_dir_name,
            "id_provider_db_uri": 'db_uri'
        }
        self.test_filepath = pathlib.Path(self.xc_dir_name) / "test.txt"
        with self.test_filepath.open("w") as test_file:
            test_file.write("Text test file")
        source_filenames = _create_scilista_and_filenames(self.kwargs["scilista_file_path"])
        self.zipfiles = _create_zip(source_filenames, self.xc_dir_name, self.test_filepath)

    def tearDown(self):
        shutil.rmtree(self.xc_dir_name)
        shutil.rmtree(self.proc_dir_name)

    @patch("operations.pre_sync_documents_to_kernel_operations.move_package_to_proc_dir")
    def test_get_sps_packages_calls_move_package_to_proc_dir(
            self,
            mk_move_package_to_proc_dir,
            mk_id_provider_functions,
            ):
        """
        Testa que move_package_to_proc_dir será executado porque as funções do
        ID provider são None, None no lugar de `request_document_id`, `connect`
        """
        mk_id_provider_functions.return_value = {
            "request_document_id": None,
            "connect": None
        }

        get_sps_packages(**self.kwargs)

        calls = mk_move_package_to_proc_dir.call_args_list
        self.assertEqual(len(calls), 9)
        for zipfile in self.zipfiles:
            with self.subTest(zipfile):
                self.assertIn(call(zipfile, self.proc_dir_name), calls)

    @patch("operations.pre_sync_documents_to_kernel_operations.move_package_to_proc_dir")
    def test_get_sps_packages_does_not_move_package_to_proc_dir_and_calls_connect_and_request_id(
            self,
            mk_move_package_to_proc_dir,
            mk_id_provider_functions,
            ):
        """
        Testa que move_package_to_proc_dir NÃO será executado porque
        as funções `request_document_id`, `connect` do ID provider foram
        carregadas
        """
        mk_request_id = MagicMock(name='request_id')
        mk_connect = Mock(name='connect')

        mk_id_provider_functions.return_value = {
            "request_document_id": mk_request_id,
            "connect": mk_connect
        }
        get_sps_packages(**self.kwargs)
        mk_connect.assert_called_once_with("db_uri")

        calls = mk_request_id.call_args_list
        self.assertEqual(len(calls), 9)
        for zipfile in self.zipfiles:
            with self.subTest(zipfile):
                _call = call(
                    zipfile,
                    os.path.join(self.proc_dir_name, os.path.basename(zipfile)),
                    "airflow",
                    "db_uri"
                )
                self.assertIn(_call, calls)
        mk_move_package_to_proc_dir.assert_not_called()

    @patch("operations.pre_sync_documents_to_kernel_operations.move_package_to_proc_dir")
    def test_get_sps_packages_calls_move_package_to_proc_dir_because_request_id_raises_exception(
            self,
            mk_move_package_to_proc_dir,
            mk_id_provider_functions,
            ):
        """
        Testa que move_package_to_proc_dir será executado porque
        as funções `request_document_id`, `connect` do ID provider foram
        carregadas, mas request_document_id levantou uma exceção
        """
        mk_request_id = MagicMock(name='request_id', side_effect=Exception)
        mk_connect = Mock(name='connect')

        mk_id_provider_functions.return_value = {
            "request_document_id": mk_request_id,
            "connect": mk_connect
        }
        get_sps_packages(**self.kwargs)
        mk_connect.assert_called_once_with("db_uri")

        calls = mk_request_id.call_args_list
        self.assertEqual(len(calls), 9)
        for zipfile in self.zipfiles:
            with self.subTest(zipfile):
                _call = call(
                    zipfile,
                    os.path.join(self.proc_dir_name, os.path.basename(zipfile)),
                    "airflow",
                    "db_uri"
                )
                self.assertIn(_call, calls)
        self.assertEqual(mk_move_package_to_proc_dir.call_count, 9)


class TestGetSPSPackages(TestCase):
    def setUp(self):
        self.xc_dir_name = tempfile.mkdtemp()
        self.proc_dir_name = tempfile.mkdtemp()
        self.kwargs = {
            "scilista_file_path": str(pathlib.Path(self.xc_dir_name) / "scilista.lst"),
            "xc_dir_name": self.xc_dir_name,
            "proc_dir_name": self.proc_dir_name,
        }
        self.test_filepath = pathlib.Path(self.xc_dir_name) / "test.txt"
        with self.test_filepath.open("w") as test_file:
            test_file.write("Text test file")

    def tearDown(self):
        shutil.rmtree(self.xc_dir_name)
        shutil.rmtree(self.proc_dir_name)

    @patch("operations.pre_sync_documents_to_kernel_operations.open")
    def test_get_sps_packages_raises_error_if_scilista_open_error(self, mk_open):
        mk_open.side_effect = FileNotFoundError
        self.assertRaises(FileNotFoundError, get_sps_packages, *self.kwargs)

    def test_get_sps_packages_moves_from_xc_dir_to_proc_dir(self):
        scilista_lines = ["rba v53n1", "rba 2019nahead", "rsp v10n4s1"]
        source_filenames = []
        scilista_file_path = pathlib.Path(self.kwargs["scilista_file_path"])
        with scilista_file_path.open("w") as scilista_file:
            for line in scilista_lines:
                scilista_file.write(line + "\n")
                source_filenames += [
                    "_".join([f"2020-01-01-00-0{i}-09-090901"] + line.split()) + ".zip"
                    for i in range(1, 4)
                ]
        for filename in source_filenames:
            zip_filename = pathlib.Path(self.xc_dir_name) / filename
            with zipfile.ZipFile(zip_filename, "w") as zip_file:
                zip_file.write(self.test_filepath)

        sps_packages = get_sps_packages(**self.kwargs)
        for filename in source_filenames:
            with self.subTest(filename=filename):
                self.assertTrue(
                    pathlib.Path(self.kwargs["proc_dir_name"])
                    .joinpath(filename)
                    .exists()
                )
        self.assertEqual(
            sps_packages,
            [
                str(pathlib.Path(self.proc_dir_name).joinpath(filename))
                for filename in source_filenames
            ],
        )

    def test_get_sps_packages_creates_sps_packages_list_file(self):
        scilista_lines = ["rba v53n1", "rba 2019nahead", "rsp v10n4s1"]
        source_filenames = []
        scilista_file_path = pathlib.Path(self.kwargs["scilista_file_path"])
        with scilista_file_path.open("w") as scilista_file:
            for line in scilista_lines:
                scilista_file.write(line + "\n")
                source_filenames += [
                    "_".join([f"2020-01-01-00-0{i}-09-090901"] + line.split()) + ".zip"
                    for i in range(1, 4)
                ]
        for filename in source_filenames:
            zip_filename = pathlib.Path(self.xc_dir_name) / filename
            with zipfile.ZipFile(zip_filename, "w") as zip_file:
                zip_file.write(self.test_filepath)

        sps_packages = get_sps_packages(**self.kwargs)
        package_paths_list = pathlib.Path(self.proc_dir_name) / "sps_packages.lst"
        self.assertTrue(package_paths_list.is_file())
        sps_packages_list = package_paths_list.read_text().split("\n")
        self.assertEqual(
            sps_packages_list,
            [
                str(pathlib.Path(self.proc_dir_name).joinpath(filename))
                for filename in source_filenames
            ],
        )


    def test_get_sps_packages_ignores_del_command_in_scilista(self):
        scilista_lines = ["rba v53n1", "rba 2019nahead", "rsp v10n4s1 del", "rsp v10n4s1", "csp v35nspe del"]
        source_filenames = []
        scilista_file_path = pathlib.Path(self.kwargs["scilista_file_path"])
        with scilista_file_path.open("w") as scilista_file:
            for line in scilista_lines:
                scilista_file.write(line + "\n")
                if not line.endswith("del"):
                    source_filenames += [
                        "_".join([f"2020-01-01-00-0{i}-09-090901"] + line.split()) + ".zip"
                        for i in range(1, 4)
                    ]
        for filename in source_filenames:
            zip_filename = pathlib.Path(self.xc_dir_name) / filename
            with zipfile.ZipFile(zip_filename, "w") as zip_file:
                zip_file.write(self.test_filepath)

        sps_packages = get_sps_packages(**self.kwargs)
        self.assertEqual(
            sps_packages,
            [
                str(pathlib.Path(self.proc_dir_name).joinpath(filename))
                for filename in source_filenames
            ],
        )

    def test_get_sps_packages_moves_nothing_if_no_source_file(self):
        scilista_file_path = pathlib.Path(self.kwargs["scilista_file_path"])
        package = "rba v53n2"
        scilista_file_path.write_text(package)
        source_filenames = [
            "2020-05-22-10-00-34-480190_rba_v53n1", "2020-05-22-10-00-34-480190_rba_2019nahead", "2020-05-22-10-00-34-480190_rsp_v10n4s1"
        ]
        for filename in source_filenames:
            zip_filename = pathlib.Path(self.xc_dir_name) / filename
            with zipfile.ZipFile(zip_filename, "w") as zip_file:
                zip_file.write(self.test_filepath)

        get_sps_packages(**self.kwargs)
        self.assertFalse(
            pathlib.Path(self.kwargs["proc_dir_name"])
            .joinpath("_".join(package.split()) + ".zip")
            .exists()
        )

    def test_get_sps_packages_must_not_move_brag_if_there_is_ag_in_scilista(self):
        scilista_file_path = pathlib.Path(self.kwargs["scilista_file_path"])
        package = "ag 2019nahead"
        scilista_file_path.write_text(package)
        source_filenames = ("2020-05-22-10-00-34-480190_brag_2019nahead.zip\n"
                            "2020-05-22-10-00-34-480190_ag_2019nahead.zip").splitlines()
        for filename in source_filenames:
            zip_filename = pathlib.Path(self.xc_dir_name) / filename
            with zipfile.ZipFile(zip_filename, "w") as zip_file:
                zip_file.write(self.test_filepath)

        result = get_sps_packages(**self.kwargs)
        self.assertEqual(
            [str(pathlib.Path(
                self.kwargs["proc_dir_name"]) / "2020-05-22-10-00-34-480190_ag_2019nahead.zip")],
            result
        )

    def test_get_sps_packages_must_return_packages_list_if_sps_packages_lst_exists(self):
        package_paths_list = [
            str(pathlib.Path(self.proc_dir_name) / f"package_0{seq}.zip")
            for seq in range(1, 4)
        ]
        package_paths_list_file = pathlib.Path(self.proc_dir_name) / "sps_packages.lst"
        package_paths_list_file.write_text("\n".join(package_paths_list))
        result = get_sps_packages(**self.kwargs)
        self.assertEqual(result, package_paths_list)


if __name__ == "__main__":
    main()
