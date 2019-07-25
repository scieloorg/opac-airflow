"""
Dag responsável por cadastrar/atualizar os relacionamentos entre as
entidades Journal e DocumentsBundle, e DocumentsBundles e Documents.

Passos para o relacionamento Journal -> DocumentsBundle:
1. Recupera contexto de Issues passado pela Dag de Espelhamento (fetch_previous_context).
2. Monta a lista de Journals e seus respectivos Bundles com o formato (prepare_journals_links_data):
  `journals = {"journal-id": ["bundle-id", "bundle-id-2", "....", "n"]}`
3. Para cada Journal do passo 2:
  3.1 Inicia o processo de atualização dos relacionamentos do Journal
  3.2 Captura a lista de relacionamentos do Journal e armazena na variável _metadata_items
  3.3 Compara a lista de bundles com a lista montada no passo 2
  3.4 Em caso de diferença entre as listas é enviado um PUT para atualizar os relacionamentos
      do Journal
"""

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from xylose.scielodocument import Issue
from kernel_gate import issue_as_kernel


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 7, 24),
    "email": ["dev@scielo.org"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG("kernel_links", default_args=default_args, schedule_interval=None)


def fetch_previous_context(**kwargs) -> None:
    """Recupera o contexto de configuração utilizado para
    inicializar a Dag"""

    dag_run_object = kwargs["dag_run"]

    if not dag_run_object:
        return None

    config = dag_run_object.conf

    if not config:
        return None

    issues = config.get("issues", [])

    if issues:
        kwargs["ti"].xcom_push(key="issues", value=issues)
