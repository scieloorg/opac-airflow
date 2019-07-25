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


def prepare_journals_links_data(**kwargs) -> dict:
    """Prepara a estrutura de dados onde os journals possuem uma
    lista contendo os identificadores de bundles relacionados"""

    journals_bundles = {}
    return journals_bundles


def update_journals_items(**kwargs) -> None:
    """Atualiza a lista de `items` para cada journal_id recuperado
    imediatamente da task anterior"""

    journals_bundles = kwargs["ti"].xcom_pull(
        task_ids="prepare_journals_links_data_task"
    )

    for journal_id, bundles in journals_bundles.items():
        # GET /journals/:journal_id -> _metadata
        # Atribui _metadata_items = _metadata["items"]
        # Compara _metadata_items com bundles
        # SE _metadata_items != bundles
        # Envia HTTP PUT /journals/:journal_id/bundles -> bundles
        # SE não -> continue
        pass


fetch_previous_context_task = PythonOperator(
    task_id="fetch_previous_context_task",
    python_callable=fetch_previous_context,
    dag=dag,
    provide_context=True,
)

prepare_journals_links_data_task = PythonOperator(
    task_id="prepare_journals_links_data_task",
    python_callable=prepare_journals_links_data,
    dag=dag,
    provide_context=True,
)

update_journals_items_task = PythonOperator(
    task_id="update_journals_items_task",
    python_callable=update_journals_items,
    dag=dag,
    provide_context=True,
)


fetch_previous_context_task >> [
    prepare_journals_links_data_task >> update_journals_items_task
]
