import tempfile
import shutil
import pathlib
import zipfile
import json
from unittest import TestCase, main
from unittest.mock import patch, MagicMock

from airflow import DAG

from operations.pre_sync_documents_to_kernel_operations import get_sps_packages


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

    def _create_fake_sps_packages(self, scilista_lines):
        """
        Cria 3 pacotes fake para entrada para cada linha da scilista fake.
        Também cria a lista esperada ao final do teste, que conterá o caminho completo
        """
        scilista_file_path = pathlib.Path(self.kwargs["scilista_file_path"])
        fake_sps_packages = {}
        proc_dir_path = pathlib.Path(self.proc_dir_name)
        with scilista_file_path.open("w") as scilista_file:
            for line in scilista_lines:
                scilista_file.write(line + "\n")
                bundle_label = "_".join(line.split())
                source_filenames = []
                if not line.endswith("del"):
                    for i in range(1, 4):
                        filename = "_".join(
                            [f"2020-01-01-00-0{i}-09-090901"] + line.split()
                        ) + ".zip"
                        source_filenames.append(filename)
                        zip_filename = pathlib.Path(self.xc_dir_name) / filename
                        with zipfile.ZipFile(zip_filename, "w") as zip_file:
                            zip_file.write(self.test_filepath)
                    fake_sps_packages[bundle_label] = [
                        str(proc_dir_path.joinpath(filename))
                        for filename in source_filenames
                    ]
        return fake_sps_packages

    def test_get_sps_packages_moves_from_xc_dir_to_proc_dir(self):
        scilista_lines = ["rba v53n1", "rba 2019nahead", "rsp v10n4s1"]
        fake_sps_packages = self._create_fake_sps_packages(scilista_lines)

        sps_packages = get_sps_packages(**self.kwargs)

        self.assertEqual(sps_packages, fake_sps_packages)
        for fake_filenames in fake_sps_packages.values():
            for filename in fake_filenames:
                with self.subTest(filename=filename):
                    self.assertTrue(
                        pathlib.Path(self.kwargs["proc_dir_name"])
                        .joinpath(filename)
                        .exists()
                    )

    def test_get_sps_packages_creates_sps_packages_list_file(self):
        scilista_lines = ["rba v53n1", "rba 2019nahead", "rsp v10n4s1"]
        fake_sps_packages = self._create_fake_sps_packages(scilista_lines)

        sps_packages = get_sps_packages(**self.kwargs)

        package_paths_list = pathlib.Path(self.proc_dir_name) / "sps_packages.lst"
        self.assertTrue(package_paths_list.is_file())
        with package_paths_list.open() as sps_list_file:
            sps_packages_list = json.load(sps_list_file)
        self.assertEqual(sps_packages_list, fake_sps_packages)


    def test_get_sps_packages_ignores_del_command_in_scilista(self):
        scilista_lines = [
            "rba v53n1",
            "rba 2019nahead",
            "rsp v10n4s1 del",
            "rsp v10n4s1",
            "csp v35nspe del",
        ]
        fake_sps_packages = self._create_fake_sps_packages(scilista_lines)

        sps_packages = get_sps_packages(**self.kwargs)
        self.assertEqual(sps_packages, fake_sps_packages)

    def test_get_sps_packages_moves_nothing_if_no_source_file(self):
        scilista_file_path = pathlib.Path(self.kwargs["scilista_file_path"])
        package = "rba v53n2"
        scilista_file_path.write_text(package)
        source_filenames = [
            "2020-05-22-10-00-34-480190_rba_v53n1",
            "2020-05-22-10-00-34-480190_rba_2019nahead",
            "2020-05-22-10-00-34-480190_rsp_v10n4s1",
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
        ag_package_filename = "2020-05-22-10-00-34-480190_ag_2019nahead.zip"
        source_filenames = ("2020-05-22-10-00-34-480190_brag_2019nahead.zip\n"
                            + ag_package_filename).splitlines()
        for filename in source_filenames:
            zip_filename = pathlib.Path(self.xc_dir_name) / filename
            with zipfile.ZipFile(zip_filename, "w") as zip_file:
                zip_file.write(self.test_filepath)

        result = get_sps_packages(**self.kwargs)

        expected = {
            "_".join(package.split()): [
                str(
                    pathlib.Path(self.kwargs["proc_dir_name"]) / ag_package_filename
                )
            ]
        }
        self.assertEqual(result, expected)

    def test_get_sps_packages_must_return_packages_list_if_sps_packages_lst_exists(self):
        package_paths_list = [
            str(pathlib.Path(self.proc_dir_name) / f"package_0{seq}.zip")
            for seq in range(1, 4)
        ]
        package_paths_list_file = pathlib.Path(self.proc_dir_name) / "sps_packages.lst"
        with package_paths_list_file.open("w") as sps_list_file:
            json.dump(package_paths_list, sps_list_file)
        result = get_sps_packages(**self.kwargs)
        self.assertEqual(result, package_paths_list)


if __name__ == "__main__":
    main()
