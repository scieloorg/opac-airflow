import os
import logging
import shutil
import json
from pathlib import Path

Logger = logging.getLogger(__name__)

PREFIX_PATTERN = "[0-9]" * 4
PREFIX_PATTERN += ("-" + ("[0-9]" * 2)) * 5
PREFIX_PATTERN += "-" + ("[0-9]" * 6)


def get_scilista_file_path(
    xc_sps_packages_dir: Path, proc_sps_packages_dir: Path, gerapadrao_id: str
) -> str:
    """Garante que a scilista usada será a do diretório de PROC. Quando for a primeira
    execução da DAG, a lista será copiada para o diretório de PROC. Caso contrário, a 
    mesma será mantida.
    """
    proc_dir_scilista_list = list(proc_sps_packages_dir.glob(f"scilista-*.lst"))
    if proc_dir_scilista_list:
        _proc_scilista_file_path = proc_dir_scilista_list[0]
        Logger.info('Proc scilista "%s" already exists', _proc_scilista_file_path)
    else:
        _scilista_filename = f"scilista-{gerapadrao_id}.lst"
        _origin_scilista_file_path = xc_sps_packages_dir / _scilista_filename
        if not _origin_scilista_file_path.is_file():
            raise FileNotFoundError(_origin_scilista_file_path)

        _proc_scilista_file_path = proc_sps_packages_dir / _scilista_filename
        Logger.info(
            'Copying original scilista "%s" to proc "%s"',
            _origin_scilista_file_path,
            _proc_scilista_file_path,
        )
        shutil.copy(_origin_scilista_file_path, _proc_scilista_file_path)
    return str(_proc_scilista_file_path)


def get_sps_packages(scilista_file_path, xc_dir_name, proc_dir_name):
    """
    Obtém Pacotes SPS através da Scilista, movendo os pacotes para o diretório de 
    processamento do Airflow e gera lista dos paths dos pacotes SPS no diretório de 
    processamento.

    list scilista: lista com as linhas do arquivo scilista
        rsp v10n4
        rsp 2018nahead
        csp v4n2-3
    list sps_packages: lista com os paths dos pacotes SPS no diretório de
    processamento
    """
    Logger.debug("get_sps_packages IN")

    xc_dir_path = Path(xc_dir_name)
    proc_dir_path = Path(proc_dir_name)
    sps_packages_list = []

    package_paths_list = proc_dir_path / "sps_packages.lst"
    if package_paths_list.is_file():
        Logger.info(
            'Pre-sync already done. Returning packages from "%s" file',
            package_paths_list,
        )
        return package_paths_list.read_text().split("\n")

    with open(scilista_file_path) as scilista:
        for scilista_item in scilista.readlines():
            acron_issue = scilista_item.strip().split()
            if len(acron_issue) != 2:
                continue
            filename_pattern = "{}_{}.zip".format(
                PREFIX_PATTERN, "_".join(acron_issue))
            Logger.info("Reading ZIP files pattern: %s", filename_pattern)

            # verifica na origem
            for item in xc_dir_path.glob(filename_pattern):
                dest_file_path = str(proc_dir_path / item.name)
                if os.path.isfile(dest_file_path):
                    Logger.info("Skip %s", str(item))
                    continue
                Logger.info("Moving %s to %s", str(item), str(proc_dir_path))
                shutil.move(str(item), str(proc_dir_path))

            # verifica no destino
            acron_issue_list = []
            for item in sorted(proc_dir_path.glob(filename_pattern)):
                Logger.info("Found %s", str(proc_dir_path / item.name))
                acron_issue_list.append(str(proc_dir_path / item.name))

            if acron_issue_list:
                sps_packages_list.extend(acron_issue_list)
            else:
                Logger.exception(
                    "Missing files which pattern is '%s' in %s and in %s",
                    filename_pattern, str(xc_dir_path), str(proc_dir_path))

    if sps_packages_list:
        Logger.info('Saving SPS packages list in "%s" file', package_paths_list)
        package_paths_list.write_text("\n".join(sps_packages_list))

    Logger.debug("get_sps_packages OUT")
    return sps_packages_list


def get_sps_packages_on_scilista(scilista_file_path, xc_dir_name, proc_dir_name):
    """
    Obtém Pacotes SPS através da Scilista, movendo os pacotes para o diretório de 
    processamento do Airflow e gera lista dos paths dos pacotes SPS no diretório de 
    processamento.

    Retorna sps_packages_list: dict com as listas de paths dos pacotes SPS no diretório de
    processamento agrupadas por bundle
    """
    Logger.debug("get_sps_packages IN")

    xc_dir_path = Path(xc_dir_name)
    proc_dir_path = Path(proc_dir_name)
    sps_packages_list = {}

    package_paths_list = proc_dir_path / "sps_packages.lst"
    if package_paths_list.is_file():
        Logger.info(
            'Pre-sync already done. Returning packages from "%s" file',
            package_paths_list,
        )
        with package_paths_list.open() as sps_list_file:
            return json.load(sps_list_file)

    with open(scilista_file_path) as scilista:
        for scilista_item in scilista.readlines():
            acron_issue = scilista_item.strip().split()
            if len(acron_issue) != 2:
                continue
            bundle_label = "_".join(acron_issue)
            filename_pattern = "{}_{}.zip".format(
                PREFIX_PATTERN, bundle_label)
            Logger.info("Reading ZIP files pattern: %s", filename_pattern)

            # verifica na origem
            for item in xc_dir_path.glob(filename_pattern):
                dest_file_path = str(proc_dir_path / item.name)
                if os.path.isfile(dest_file_path):
                    Logger.info("Skip %s", str(item))
                    continue
                Logger.info("Moving %s to %s", str(item), str(proc_dir_path))
                shutil.move(str(item), str(proc_dir_path))

            # verifica no destino
            acron_issue_list = []
            for item in sorted(proc_dir_path.glob(filename_pattern)):
                Logger.info("Found %s", str(proc_dir_path / item.name))
                acron_issue_list.append(str(proc_dir_path / item.name))

            if acron_issue_list:
                sps_packages_list[bundle_label] = acron_issue_list
            else:
                Logger.exception(
                    "Missing files which pattern is '%s' in %s and in %s",
                    filename_pattern, str(xc_dir_path), str(proc_dir_path))

    if sps_packages_list:
        Logger.info('Saving SPS packages list in "%s" file', package_paths_list)
        with package_paths_list.open('w') as sps_list_file:
            json.dump(sps_packages_list, sps_list_file)

    Logger.debug("get_sps_packages OUT")
    return sps_packages_list
