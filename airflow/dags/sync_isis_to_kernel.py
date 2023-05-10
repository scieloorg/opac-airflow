"""DAG responsável por ler dados de bases ISIS e replica-los
em uma REST-API que implementa a específicação do SciELO Kernel"""

import os
import shutil
import logging
import requests
import json
import http.client
from typing import List
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.hooks.http_hook import HttpHook
from airflow.exceptions import AirflowException
from xylose.scielodocument import Journal, Issue
from datetime import datetime, timedelta
from deepdiff import DeepDiff

from common import hooks
from operations.docs_utils import get_bundle_id
from operations.sync_isis_to_kernel import parse_date_to_iso_format

"""
Para o devido entendimento desta DAG pode-se ter como base a seguinte explicação.

Esta DAG possui tarefas que são iniciadas a partir de um TRIGGER externo. As fases
de execução são às seguintes:

1) Cria as pastas temporárias de trabalho, sendo elas:
    a) /airflow_home/{{ dag_run }}/isis
    b) /airflow_home/{{ dag_run }}/json
2) Faz uma cópia das bases MST:
    a) A partir das variáveis `BASE_ISSUE_FOLDER_PATH` e `BASE_TITLE_FOLDER_PATH`
    b) Retorna XCOM com os paths exatos de onde os arquivos MST estarão
    c) Retorna XCOM com os paths exatos de onde os resultados da extração MST devem ser depositados
3) Ler a base TITLE em formato MST
    a) Armazena output do isis2json no arquivo `/airflow_home/{{ dag_run }}/json/title.json`
4) Ler a base ISSUE em formato MST
    a) Armazena output do isis2json no arquivo `/airflow_home/{{ dag_run }}/json/issue.json`
5) Envia os dados da base TITLE para a API do Kernel
    a) Itera entre os periódicos lidos da base TITLE
    b) Converte o periódico para o formato JSON aceito pelo Kernel
    c) Verifica se o Journal já existe na API Kernel
        I) Faz o diff do entre o payload gerado e os metadados vindos do Kernel
        II) Se houver diferenças faz-ze um PATCH para atualizar o registro
    d) Se o Journal não existir
        I) Remove as chaves nulas
        II) Faz-se um PUT para criar o registro
6) Dispara o DAG subsequente.
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

KERNEL_API_JOURNAL_ENDPOINT = "/journals/"
KERNEL_API_BUNDLES_ENDPOINT = "/bundles/"
KERNEL_API_JOURNAL_BUNDLES_ENDPOINT = "/journals/{journal_id}/issues"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 6, 25),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG("sync_isis_to_kernel", default_args=default_args, schedule_interval=None)


def journal_as_kernel(journal: Journal) -> dict:
    """Gera um dicionário com a estrutura esperada pela API do Kernel a
    partir da estrutura gerada pelo isis2json"""

    _payload = {}
    _payload["_id"] = journal.scielo_issn

    if journal.mission:
        _payload["mission"] = [
            {"language": lang, "value": value}
            for lang, value in journal.mission.items()
        ]
    else:
        _payload["mission"] = []

    _payload["title"] = journal.title or ""
    _payload["title_iso"] = journal.abbreviated_iso_title or ""
    _payload["short_title"] = journal.abbreviated_title or ""
    _payload["acronym"] = journal.acronym or ""
    _payload["scielo_issn"] = journal.scielo_issn or ""
    _payload["print_issn"] = journal.print_issn or ""
    _payload["electronic_issn"] = journal.electronic_issn or ""

    _payload["status_history"] = []

    for status in journal.status_history:
        _status = {"status": status[1], "date": parse_date_to_iso_format(status[0])}

        if status[2]:
            _status["reason"] = status[2]

        _payload["status_history"].append(_status)

    _payload["subject_areas"] = journal.subject_areas or []

    _payload["sponsors"] = []
    if journal.sponsors:
        _payload["sponsors"] = [{"name": sponsor} for sponsor in journal.sponsors]

    _payload["subject_categories"] = journal.wos_subject_areas or []
    _payload["online_submission_url"] = journal.submission_url or ""

    _payload["next_journal"] = {}
    if journal.next_title:
        _payload["next_journal"]["name"] = journal.next_title

    _payload["previous_journal"] = {}
    if journal.previous_title:
        _payload["previous_journal"]["name"] = journal.previous_title

    _payload["contact"] = {}
    if journal.editor_email:
        _payload["contact"]["email"] = journal.editor_email
    if journal.editor_address:
        _payload["contact"]["address"] = journal.editor_address

    institution_responsible_for = []
    if journal.publisher_name:
        for name in journal.publisher_name:
            item = {"name": name}
            if journal.publisher_city:
                item["city"] = journal.publisher_city
            if journal.publisher_state:
                item["state"] = journal.publisher_state
            if journal.publisher_country:
                country_code, country_name = journal.publisher_country
                item["country_code"] = country_code
                item["country"] = country_name
            institution_responsible_for.append(item)
    _payload["institution_responsible_for"] = institution_responsible_for

    return _payload


def issue_as_kernel(issue: dict) -> dict:
    def parse_date(date: str) -> str:
        """Traduz datas em formato simples ano-mes-dia, ano-mes para
        o formato iso utilizado durantr a persistência do Kernel"""

        _date = None
        try:
            _date = datetime.strptime(date, "%Y-%m-%d")
        except ValueError:
            try:
                _date = datetime.strptime(date, "%Y-%m")
            except ValueError:
                _date = datetime.strptime(date, "%Y")

        return _date

    def generate_scielo_issue_pid(issn_id, v36_field):
        year = v36_field[0:4]
        order = v36_field[4:].zfill(4)
        return issn_id + year + order

    _payload = {}
    _payload["volume"] = issue.volume or ""
    _payload["number"] = issue.number or ""

    suppl = issue.supplement_volume or issue.supplement_number
    if suppl or issue.type is "supplement":
        _payload["supplement"] = suppl or "0"

    if issue.titles:
        _titles = [
            {"language": lang, "value": value} for lang, value in issue.titles.items()
        ]
        _payload["titles"] = _titles
    else:
        _payload["titles"] = []

    _payload["publication_months"] = {}
    if issue.start_month and issue.end_month:
        _payload["publication_months"]["range"] = [
            int(issue.start_month), int(issue.end_month)]
    elif issue.start_month:
        _payload["publication_months"]["month"] = int(issue.start_month)

    issn_id = issue.data.get("issue").get("v35")[0]["_"]
    _creation_date = parse_date(issue.publication_date)

    _payload["_id"] = get_bundle_id(
        issn_id,
        str(_creation_date.year),
        issue.volume,
        issue.number,
        _payload.get("supplement"),
    )
    _payload["publication_year"] = str(_creation_date.year)
    _payload["pid"] = generate_scielo_issue_pid(
        issn_id, issue.data.get("issue").get("v36")[0]["_"]
    )

    return _payload


def issue_data_to_link(issue: dict) -> dict:
    _issue_data = issue_as_kernel(issue)
    _issue_info_to_link = {
        "id": _issue_data["_id"],
        "year": _issue_data["publication_year"],
    }
    for attr in ("volume", "number", "supplement"):
        if attr in _issue_data.keys() and _issue_data.get(attr):
            _issue_info_to_link[attr] = _issue_data.get(attr)
    return _issue_info_to_link


def register_or_update(_id: str, payload: dict, entity_url: str):
    """Cadastra ou atualiza uma entidade no Kernel a partir de um payload"""

    try:
        response = hooks.kernel_connect(
            endpoint="{}{}".format(entity_url, _id), method="GET"
        )
    except requests.exceptions.HTTPError as exc:
        logging.info("hooks.kernel_connect HTTPError: %d", exc.response.status_code)
        if exc.response.status_code == http.client.NOT_FOUND:
            payload = {k: v for k, v in payload.items() if v}
            response = hooks.kernel_connect(
                endpoint="{}{}".format(entity_url, _id),
                method="PUT",
                data=payload
            )
        else:
            raise exc
    else:
        _metadata = response.json()["metadata"]

        payload = {
            k: v
            for k, v in payload.items()
            if _metadata.get(k) or _metadata.get(k) == v or v
        }

        if DeepDiff(_metadata, payload, ignore_order=True):
            endpoint = "{}{}".format(entity_url, _id)
            if not _try_journal_patch(payload, endpoint):
                _retry_journal_patch(_metadata, payload, endpoint)

    return response


def _retry_journal_patch(_metadata, payload, endpoint):
    for k, v in payload.items():
        if _metadata.get(k) == v:
            continue
        _try_journal_patch({k: v}, endpoint)


def _try_journal_patch(payload, endpoint):
    try:
        response = hooks.kernel_connect(
            endpoint=endpoint,
            method="PATCH",
            data=payload
        )
        logging.info(
            "Sucesso ao realizar um PATCH no endpoint: %s, payload: %s" %
            (endpoint, payload)
        )
        return True
    except requests.exceptions.HTTPError as exc:
        logging.info(
            "Erro ao tentar realizar um PATCH no endpoint: %s, payload: %s" %
            (endpoint, payload)
        )
        return False
    except Exception as exc:
        logging.info(
            "Erro inesperado ao tentar realizar um PATCH no endpoint: %s, payload: %s, erro: %s" %
            (endpoint, payload, str(exc))
        )
        return False


def create_journal_issn_index(title_json_dirname, journals):
    _issn_index = {}
    for journal in journals:
        _issn_id = journal["scielo_issn"]
        _issn_index.update({_issn_id: _issn_id})
        _print_issn = journal.get("print_issn", "")
        if len(_print_issn) > 0 and _print_issn != _issn_id:
            _issn_index.update({_print_issn: _issn_id})
        _electronic_issn = journal.get("electronic_issn", "")
        if len(_electronic_issn) > 0 and _electronic_issn != _issn_id:
            _issn_index.update({_electronic_issn: _issn_id})

    _issn_index_json_path = os.path.join(title_json_dirname, "issn_index.json")
    logging.info("creating journal ISSN index file %s.", _issn_index_json_path)
    with open(_issn_index_json_path, "w") as index_file:
        index_file.write(json.dumps(_issn_index))
    return _issn_index_json_path


def process_journals(**context):
    """Processa uma lista de journals carregados a partir do resultado
    de leitura da base MST e gera um índice de ISSN de periódicos"""

    title_json_path = context["ti"].xcom_pull(
        task_ids="copy_mst_bases_to_work_folder_task", key="title_json_path"
    )

    with open(title_json_path, "r") as f:
        journals = f.read()
        logging.info("reading file from %s." % (title_json_path))

    journals = json.loads(journals)
    journals_as_kernel = [journal_as_kernel(Journal(journal)) for journal in journals]
    issn_index_json_path = create_journal_issn_index(
        os.path.dirname(title_json_path), journals_as_kernel
    )

    for journal in journals_as_kernel:
        _id = journal.pop("_id")
        register_or_update(_id, journal, KERNEL_API_JOURNAL_ENDPOINT)

    context["ti"].xcom_push("issn_index_json_path", issn_index_json_path)


def filter_issues(issues: List[Issue]) -> List[Issue]:
    """Filtra as issues em formato xylose sempre removendo
    os press releases e ahead of print"""

    filters = [
        lambda issue: not issue.type == "pressrelease",
        lambda issue: not issue.type == "ahead",
    ]

    for f in filters:
        issues = list(filter(f, issues))

    return issues


def process_issues(**context):
    """Processa uma lista de issues carregadas a partir do resultado
    de leitura da base MST"""

    issue_json_path = context["ti"].xcom_pull(
        task_ids="copy_mst_bases_to_work_folder_task", key="issue_json_path"
    )

    with open(issue_json_path, "r") as f:
        issues = f.read()
        logging.info("reading file from %s." % (issue_json_path))

    issues = json.loads(issues)
    issues = [Issue({"issue": data}) for data in issues]
    issues = filter_issues(issues)
    issues_as_kernel = _issues_as_kernel(issues)

    for issue in issues_as_kernel:
        _id = issue.pop("_id")
        register_or_update(_id, issue, KERNEL_API_BUNDLES_ENDPOINT)


def _issues_as_kernel(issues):
    """
    Replaces [issue_as_kernel(issue) for issue in issues]
    to avoid `issue_as_kernel` raises exception and break the processing
    """
    for issue in issues:
        try:
            yield issue_as_kernel(issue)
        except Exception as e:
            logging.error(f"Unable to get issue as kernel {e} {issue.data}")


def copy_mst_files_to_work_folder(**kwargs):
    """Copia as bases MST para a área de trabalho da execução corrente.

    O resultado desta função gera cópias das bases title e issue para paths correspondentes aos:
    title: /airflow_home/work_folder_path/{{ run_id }}/isis/title.*
    issue: /airflow_home/work_folder_path/{{ run_id }}/isis/issue.*
    """

    WORK_PATH = Variable.get("WORK_FOLDER_PATH")
    CURRENT_EXECUTION_FOLDER = os.path.join(WORK_PATH, kwargs["run_id"])
    WORK_ISIS_FILES = os.path.join(CURRENT_EXECUTION_FOLDER, "isis")
    WORK_JSON_FILES = os.path.join(CURRENT_EXECUTION_FOLDER, "json")

    BASE_TITLE_FOLDER_PATH = Variable.get("BASE_TITLE_FOLDER_PATH")
    BASE_ISSUE_FOLDER_PATH = Variable.get("BASE_ISSUE_FOLDER_PATH")

    copying_paths = []

    for path in [BASE_TITLE_FOLDER_PATH, BASE_ISSUE_FOLDER_PATH]:
        files = [
            f for f in os.listdir(path) if f.endswith(".xrf") or f.endswith(".mst")
        ]

        for file in files:
            origin_path = os.path.join(path, file)
            desatination_path = os.path.join(WORK_ISIS_FILES, file)
            copying_paths.append([origin_path, desatination_path])

    for origin, destination in copying_paths:
        logging.info("copying file from %s to %s." % (origin, destination))
        shutil.copy(origin, destination)

        if "title.mst" in destination:
            kwargs["ti"].xcom_push("title_mst_path", destination)
            kwargs["ti"].xcom_push(
                "title_json_path", os.path.join(WORK_JSON_FILES, "title.json")
            )

        if "issue.mst" in destination:
            kwargs["ti"].xcom_push("issue_mst_path", destination)
            kwargs["ti"].xcom_push(
                "issue_json_path", os.path.join(WORK_JSON_FILES, "issue.json")
            )


def mount_journals_issues_link(issues: List[dict]) -> dict:
    """Monta a relação entre os journals e suas issues.

    Monta um dicionário na estrutura {"journal_id": ["issue_id"]}. Issues do
    tipo ahead ou pressrelease não são consideradas. É utilizado o
    campo v35 (issue) para obter o `journal_id` ao qual a issue deve ser relacionada.

    :param issues: Lista contendo issues extraídas da base MST"""

    journal_issues = {}
    issues = [Issue({"issue": data}) for data in issues]
    issues = filter_issues(issues)

    for issue in issues:
        try:
            issue_to_link = issue_data_to_link(issue)
        except TypeError as e:
            # pass para o próximo issue
            logging.error(f"Unable to link issue {e} {issue.data}")
            continue

        issue_to_link["order"] = issue.data["issue"]["v36"][0]["_"]
        journal_id = issue.data.get("issue").get("v35")[0]["_"]
        journal_issues.setdefault(journal_id, [])

        if issue_to_link not in journal_issues[journal_id]:
            journal_issues[journal_id].append(issue_to_link)

    return journal_issues


def update_journals_and_issues_link(journal_issues: dict):
    """Atualiza o relacionamento entre Journal e Issues.

    Para cada Journal é verificado se há mudanças entre a lista de Issues
    obtida via API e a lista de issues recém montada durante o espelhamento. Caso
    alguma mudança seja detectada o Journal será atualizado com a nova lista de
    issues.

    :param journal_issues: Dicionário contendo journals e issues. As chaves do
    dicionário serão os identificadores dos journals e os valores serão listas contendo
    os indificadores das issues."""

    for journal_id, issues in journal_issues.items():
        try:
            api_hook = HttpHook(http_conn_id="kernel_conn", method="GET")
            response = api_hook.run(endpoint="{}{}".format(KERNEL_API_JOURNAL_ENDPOINT, journal_id))
            journal_items = response.json()["items"]

            if DeepDiff(journal_items, issues):
                BUNDLE_URL = KERNEL_API_JOURNAL_BUNDLES_ENDPOINT.format(
                    journal_id=journal_id
                )
                api_hook = HttpHook(http_conn_id="kernel_conn", method="PUT")
                response = api_hook.run(endpoint=BUNDLE_URL, data=json.dumps(issues))
                logging.info("updating bundles of journal %s" % journal_id)

        except (AirflowException):
            logging.warning("journal %s cannot be found" % journal_id)


def link_journals_and_issues(**kwargs):
    """Atualiza o relacionamento entre Journal e Issue."""

    issue_json_path = kwargs["ti"].xcom_pull(
        task_ids="copy_mst_bases_to_work_folder_task", key="issue_json_path"
    )

    with open(issue_json_path) as f:
        issues = json.load(f)
        logging.info("reading file from %s." % (issue_json_path))

    journal_issues = mount_journals_issues_link(issues)
    update_journals_and_issues_link(journal_issues)


def get_gerapadrao_id(**kwargs):
    # obtem o novo valor para `GERAPADRAO_ID` a partir de `dag_run.conf`
    gerapadrao_id = kwargs["dag_run"].conf.get("GERAPADRAO_ID")

    if gerapadrao_id:
        # atribui o valor novo para as variáveis
        Variable.set("GERAPADRAO_ID", gerapadrao_id)
        Variable.set("GERAPADRAO_ID_FOR_SCILISTA", gerapadrao_id)
        items = Variable.get("GERAPADRAO_ID_FOR_URI_LIST", default_var=[],
                             deserialize_json=True)
        items.append(gerapadrao_id)
        Variable.set("GERAPADRAO_ID_FOR_URI_LIST",
                     items, serialize_json=True)


CREATE_FOLDER_TEMPLATES = """
    mkdir -p '{{ var.value.WORK_FOLDER_PATH }}/{{ run_id }}/isis' && \
    mkdir -p '{{ var.value.WORK_FOLDER_PATH }}/{{ run_id }}/json'"""

EXCTRACT_MST_FILE_TEMPLATE = """
{% set input_path = task_instance.xcom_pull(task_ids='copy_mst_bases_to_work_folder_task', key=params.input_file_key) %}
{% set output_path = task_instance.xcom_pull(task_ids='copy_mst_bases_to_work_folder_task', key=params.output_file_key) %}

java -cp {{ params.classpath}} org.python.util.jython {{ params.isis2json }} -t 3 -p 'v' --inline {{ input_path }} -o {{ output_path }}"""


create_work_folders_task = BashOperator(
    task_id="create_work_folders_task", bash_command=CREATE_FOLDER_TEMPLATES, dag=dag
)

copy_mst_bases_to_work_folder_task = PythonOperator(
    task_id="copy_mst_bases_to_work_folder_task",
    python_callable=copy_mst_files_to_work_folder,
    dag=dag,
    provide_context=True,
)


extract_title_task = BashOperator(
    task_id="extract_title_task",
    bash_command=EXCTRACT_MST_FILE_TEMPLATE,
    params={
        "classpath": CLASSPATH,
        "isis2json": ISIS2JSON_PATH,
        "input_file_key": "title_mst_path",
        "output_file_key": "title_json_path",
    },
    dag=dag,
)


extract_issue_task = BashOperator(
    task_id="extract_issue_task",
    bash_command=EXCTRACT_MST_FILE_TEMPLATE,
    params={
        "classpath": CLASSPATH,
        "isis2json": ISIS2JSON_PATH,
        "input_file_key": "issue_mst_path",
        "output_file_key": "issue_json_path",
    },
    dag=dag,
)


process_journals_task = PythonOperator(
    task_id="process_journals_task",
    python_callable=process_journals,
    dag=dag,
    provide_context=True,
    params={"process_journals": True},
)

process_issues_task = PythonOperator(
    task_id="process_issues_task",
    python_callable=process_issues,
    dag=dag,
    provide_context=True,
)


link_journals_and_issues_task = PythonOperator(
    task_id="link_journals_and_issues_task",
    python_callable=link_journals_and_issues,
    dag=dag,
    provide_context=True,
)


get_gerapadrao_id_task = PythonOperator(
    task_id="get_gerapadrao_id_task",
    python_callable=get_gerapadrao_id,
    dag=dag,
    provide_context=True,
)

trigger_pre_sync_documents_to_kernel_dag_task = TriggerDagRunOperator(
    task_id="trigger_pre_sync_documents_to_kernel_dag_task",
    trigger_dag_id="pre_sync_documents_to_kernel",
    dag=dag,
)
get_gerapadrao_id_task >> create_work_folders_task
create_work_folders_task >> copy_mst_bases_to_work_folder_task >> extract_title_task
extract_title_task >> extract_issue_task >> process_journals_task >> process_issues_task
process_issues_task >> link_journals_and_issues_task >> trigger_pre_sync_documents_to_kernel_dag_task
