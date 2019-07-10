# conding: utf-8
import logging
import os
import shutil
from datetime import timedelta
from pathlib import Path
from zipfile import ZipFile

from lxml import etree
from airflow import DAG
from airflow.models import Variable
from airflow import utils as airflow_utils
from airflow.operators.python_operator import PythonOperator


"""
    DAG responsável adicionar/atualizar os pacotes SPS no Kernel.

    Passos:
        a. Obtém Pacotes SPS através da Scilista
        b. Ler Pacotes SPS de acordo com a Scilista
            - Diretório configurável, alimentado pelo XC
            - Arquivos de computador no formato .zip, cada um representando um número
            (fascículo), com todos os arquivos XML, e respectivos arquivos PDF e
            outros ativos digitais.

            Para cada um dos XMLs
                1. Verificar XMLs para deleção
                   (/article-meta/article-id/@specific-use="delete")
                   I. DELETE documentos no Kernel
                2. Obter SciELO ID no XML
                    I. PUT pacotes SPS no Minio
                    II. PUT/PATCH XML no Kernel
                    III. PUT PDF no Kernel

        c. Não conseguiu ler pacotes
            1. Envio de Email sobre pacote inválido
            2. Pensar em outras forma de verificar
        d. Deleta fascículos de acordo com a Scilista
            1. Deletar o bundle no Kernel
"""
default_args = {
    "owner": "airflow",
    "start_date": airflow_utils.dates.days_ago(2),
}


dag = DAG(
    dag_id="kernel_documents",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
)


def get_sps_packages(**kwargs):
    """
    Obtém Pacotes SPS através da Scilista, movendo os pacotes para o diretório de
    processamento do Airflow e gera lista dos paths dos pacotes SPS no diretório de
    processamento.

    list scilista: lista com as linhas do arquivo scilista.lst
        rsp v10n4
        rsp 2018nahead
        csp v4n2-3
    list sps_packages: lista com os paths dos pacotes SPS no diretório de
    processamento
    """
    logging.debug("get_sps_packages IN")

    scilista_file_path = Variable.get("SCILISTA_FILE_PATH")
    xc_dir_name = Variable.get("XC_SPS_PACKAGES_DIR")
    proc_dir_name = Variable.get("PROC_SPS_PACKAGES_DIR")

    xc_dir_path = Path(xc_dir_name)
    proc_dir_path = Path(proc_dir_name)
    sps_packages_list = []

    with open(scilista_file_path) as scilista:
        for acron_issue in scilista.readlines():
            zip_filename = "{}.zip".format('_'.join(acron_issue.split()))
            source = xc_dir_path / zip_filename
            destination = proc_dir_path / zip_filename
            logging.info("Reading dir: %s" % str(source))
            if os.path.exists(str(source)):
                logging.info("Moving to dir: %s" % str(destination))
                shutil.move(str(source), str(destination))
                sps_packages_list.append(str(destination))

    kwargs["ti"].xcom_push(key="sps_packages", value=sorted(sps_packages_list))

    logging.debug("get_sps_packages OUT")


def list_documents(**kwargs):
    """
    Lista todos os XMLs dos SPS Packages da lista obtida do diretório do XC.

    list sps_packages: lista com os paths dos pacotes SPS no diretório de processamento
    dict sps_packages_xmls: dict com os paths dos pacotes SPS e os respectivos nomes dos
        arquivos XML.
    """
    logging.debug("list_documents IN")
    sps_packages_list = kwargs["ti"].xcom_pull(
        key="sps_packages",
        task_ids="get_sps_packages_id"
    )
    sps_packages_xmls = {}
    for sps_package in sps_packages_list or []:
        logging.info("Reading sps_package: %s" % sps_package)
        with ZipFile(sps_package) as zf:
            xmls_filenames = [
                xml_filename
                for xml_filename in zf.namelist()
                if os.path.splitext(xml_filename)[-1] == '.xml'
            ]
            if xmls_filenames:
                sps_packages_xmls[sps_package] = xmls_filenames
    if sps_packages_xmls:
        kwargs["ti"].xcom_push(
            key="sps_packages_xmls",
            value=sps_packages_xmls
        )
    logging.debug("list_documents OUT")


def read_xmls(**kwargs):
    """
    Lê XMLs para tratar documentos (Deletar, Registrar ou Atualizar) e gera listas

    dict sps_packages_xmls: dict com os paths dos pacotes SPS e os respectivos nomes dos
        arquivos XML.
    list docs_to_delete: lista de XMLs para deletar do Kernel
    list docs_to_preserve: lista de XMLs para manter no Kernel (Registrar ou atualizar)
    """
    logging.debug("read_xmls IN")
    sps_packages_xmls = kwargs["ti"].xcom_pull(key="sps_packages_xmls", task_id="list_documents_id")

    docs_to_delete = []
    docs_to_preserve = []
    for sps_package, sps_xml_files in (sps_packages_xmls or {}).items():
        logging.info("Reading sps_package: %s" % sps_package)
        with ZipFile(sps_package) as zf:
            for sps_xml_file in sps_xml_files:
                xml_content = zf.read(sps_xml_file)
                if len(xml_content) > 0:
                    xml_file = etree.XML(xml_content)
                    scielo_id = xml_file.find(".//article-id[@specific-use='scielo']")
                    if scielo_id is not None:
                        delete_tag = scielo_id.getparent().find(
                            "./article-id[@specific-use='delete']"
                        )
                        if delete_tag is not None:
                            docs_to_delete.append(scielo_id.text)
                        else:
                            docs_to_preserve.append(scielo_id.text)

    if docs_to_delete:
        kwargs["ti"].xcom_push(
            key="docs_to_delete",
            value=docs_to_delete
        )
    if docs_to_preserve:
        kwargs["ti"].xcom_push(
            key="docs_to_preserve",
            value=docs_to_preserve
        )
    logging.debug("read_xmls OUT")


def delete_documents(**kwargs):
    print("delete_documents IN")
    print("Deleta documentos informados do Kernel")
    print("delete_documents OUT")


def register_documents(**kwargs):
    print("register_documents IN")
    print("Registra documentos informados no Kernel")
    print("register_documents OUT")


def update_documents(**kwargs):
    print("update_documents IN")
    print("Atualiza documentos informados no Kernel")
    print("update_documents OUT")


get_sps_packages_task = PythonOperator(
    task_id="get_sps_packages_id",
    provide_context=True,
    python_callable=get_sps_packages,
    dag=dag,
)


list_documents_task = PythonOperator(
    task_id="list_documents_id",
    provide_context=True,
    python_callable=list_documents,
    dag=dag,
)


read_xmls_task = PythonOperator(
    task_id="read_xmls_id",
    python_callable=read_xmls,
    dag=dag,
)


delete_documents_task = PythonOperator(
    task_id="delete_documents_id",
    provide_context=True,
    python_callable=delete_documents,
    dag=dag,
)


register_documents_task = PythonOperator(
    task_id="register_documents_id",
    provide_context=True,
    python_callable=register_documents,
    dag=dag,
)


update_documents_task = PythonOperator(
    task_id="update_documents_id",
    provide_context=True,
    python_callable=update_documents,
    dag=dag,
)


get_sps_packages_task >> list_documents_task
list_documents_task >> read_xmls_task
read_xmls_task >> [delete_documents_task, register_documents_task, update_documents_task]
