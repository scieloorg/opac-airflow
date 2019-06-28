"""DAG responsável por ler dados de bases ISIS e replica-los
em uma REST-API que implementa a específicação do SciELO Kernel"""

import os
import requests
import json
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from xylose.scielodocument import Journal, Issue
from datetime import datetime, timedelta
from deepdiff import DeepDiff

"""
Para o devido entendimento desta DAG pode-se ter como base a seguinte explicação.

Esta DAG possui tarefas que são iniciadas a partir de um TRIGGER externo. As fases
de execução são às seguintes:

1) Ler a base TITLE em formato MST
1.1) Armazena output do isis2json na área de trabalho xcom

2) Ler a base ISSUE em formato MST
2.2) Armazena output do isis2json na área de trabalho xcom

3) Envia os dados da base TITLE para a API do Kernel
3.1) Itera entre os periódicos lidos da base TITLE
3.2) Converte o periódico para o formato JSON aceito pelo Kernel
3.3) Verifica se o Journal já existe na API Kernel
3.3.1) Se o Journal já existir verifica o md5 entre o dado lido e o dado existente
3.3.2) Se o hash for igual passa-se para o próximo
3.3.3) Se o hash for diferente faz-ze um PATCH para atualizar o registro
3.4) Se o Journal não existir faz-se um PUT para criar o registro
3.5) Dispara o DAG subsequente.
"""

BASE_PATH = os.path.dirname(os.path.dirname(__file__))

JAVA_LIB_DIR = os.path.join(BASE_PATH, "utils/isis2json/lib/")

JAVA_LIBS_PATH = [
    os.path.join(JAVA_LIB_DIR, file)
    for file in os.listdir(JAVA_LIB_DIR)
    if file.endswith(".jar")
]

CLASSPATH = ":".join(JAVA_LIBS_PATH)

ISIS2JSON_PATH = os.path.join(BASE_PATH, "utils/isis2json/isis2json.py")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 6, 25),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG("kernel-gate", default_args=default_args, schedule_interval=None)


jython_command_template = """java -cp {{ params.classpath}} org.python.util.jython \
    {{ params.isis2json }} -t 3 -p 'v' --inline {{ params.file }}
"""

read_title_mst = BashOperator(
    task_id="read_title_mst",
    bash_command=jython_command_template,
    params={
        "file": "/usr/local/airflow/bases/scl/title/title.mst",
        "classpath": CLASSPATH,
        "isis2json": ISIS2JSON_PATH,
    },
    dag=dag,
    xcom_push=True,
)


read_issue_mst = BashOperator(
    task_id="read_issue_mst",
    bash_command=jython_command_template,
    params={
        "file": "/usr/local/airflow/bases/scl/issue/issue.mst",
        "classpath": CLASSPATH,
        "isis2json": ISIS2JSON_PATH,
    },
    dag=dag,
    xcom_push=True,
)


def get_journal_kernel_payload(journal: dict) -> dict:
    """Gera um dicionário com a estrutura esperada pela API do Kernel a
    partir da estrutura gerada pelo isis2json"""

    journal = Journal(journal)

    _payload = {}
    _id = journal.any_issn()

    if not _id:
        _id = journal.scielo_issn

    _payload["_id"] = _id

    if journal.mission:

        _payload["mission"] = [
            {"language": lang, "value": value}
            for lang, value in journal.mission.items()
        ]

    if journal.title:
        _payload["title"] = journal.title

    if journal.abbreviated_iso_title:
        _payload["title_iso"] = journal.abbreviated_iso_title

    if journal.abbreviated_title:
        _payload["short_title"] = journal.abbreviated_title

    _payload["acronym"] = journal.acronym

    if journal.scielo_issn:
        _payload["scielo_issn"] = journal.scielo_issn

    if journal.print_issn:
        _payload["print_issn"] = journal.print_issn

    if journal.electronic_issn:
        _payload["print_issn"] = journal.electronic_issn

    if journal.status_history:
        _payload["status"] = {}
        _status = journal.status_history[-1]
        _payload["status"]["status"] = _status[1]

        if _status[2]:
            _payload["status"]["reason"] = _status[2]

    if journal.subject_areas:
        _payload["subject_areas"] = []

        for subject_area in journal.subject_areas:
            # TODO: Algumas áreas estão em caixa baixa, o que devemos fazer?
            _payload["subject_areas"].append(subject_area.upper())

    if journal.sponsors:
        _sponsors = [{"name": sponsor} for sponsor in journal.sponsors]
        _payload["sponsors"] = _sponsors

    if journal.wos_subject_areas:
        _payload["subject_categories"] = journal.wos_subject_areas

    if journal.submission_url:
        _payload["online_submission_url"] = journal.submission_url

    if journal.next_title:
        _next_journal = {"name": journal.next_title}
        _payload["next_journal"] = _next_journal

    if journal.previous_title:
        _previous_journal = {"name": journal.previous_title}
        _payload["previous_journal"] = _previous_journal

    _contact = {}

    if journal.editor_email:
        _contact["email"] = journal.editor_email

    if journal.editor_address:
        _contact["address"] = journal.editor_address

    if _contact:
        _payload["contact"] = _contact

    return _payload


# Este DNS é referente a um container docker que foi iniciado
# para realizarmos os testes de cadastro/update no Kernel
KERNEL_API_JOURNAL_URL = "http://document-store_webapp_1:6543/journals/"


def register_or_update(_id: str, payload: dict, entity_url: str):
    """Cadastra ou atualiza uma entidade no Kernel a partir de um payload"""
    response = requests.get("{}{}".format(entity_url, _id))

    if response.status_code == 404:
        response = requests.put(
            "{}{}".format(entity_url, _id), data=json.dumps(payload)
        )
    elif response.status_code == 200:
        _metadata = response.json()["metadata"]

        if DeepDiff(_metadata, payload, ignore_order=True):
            response = requests.patch(
                "{}{}".format(entity_url, _id), data=json.dumps(payload)
            )

    return response


def work_on_mst_output(**context):
    """Função responsável por ler o output do isis2json, transformar o dado
    em um payload no formato Kernel e submeter o seu envio para a API"""
    title_data = context["ti"].xcom_pull(task_ids="read_title_mst")

    for journal_data in json.loads(title_data):
        payload = get_journal_kernel_payload(journal_data)
        _id = payload.pop("_id")
        response = register_or_update(_id, payload, KERNEL_API_JOURNAL_URL)
        
        if response.status_code == 201:
            print("Created {}{}".format(KERNEL_API_JOURNAL_URL, _id))
        elif response.status_code == 204:
            print("Updated {}{}".format(KERNEL_API_JOURNAL_URL, _id))
        elif response.status_code == 400:
            # TODO: Em caso de erro nós deveriamos parar a task?
            print("Error on {}{}".format(KERNEL_API_JOURNAL_URL, _id))


work_on_journals = PythonOperator(
    task_id="work_on_journals",
    python_callable=work_on_mst_output,
    dag=dag,
    provide_context=True,
)

read_title_mst >> read_issue_mst
read_issue_mst >> work_on_journals
