import logging
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from common.hooks import mongo_connect
from operations.sync_exernal_content_to_website_operations import (
    try_fetch_and_register_feed,
)

Logger = logging.getLogger(__name__)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 2, 6),
}

dag = DAG(
    dag_id="sync_external_content_to_website",
    default_args=default_args,
    schedule_interval="@hourly",
    catchup=False
)

RSS_NEWS_FEEDS = {
    "pt_BR": {
        "display_name": "SciELO em Perspectiva",
        "url": "http://blog.scielo.org/feed/",
    },
    "es": {
        "display_name": "SciELO en Perspectiva",
        "url": "http://blog.scielo.org/es/feed/",
    },
    "en": {
        "display_name": "SciELO in Perspective",
        "url": "http://blog.scielo.org/en/feed/",
    },
}


def fetch_and_register_news_feed_callable(**kwargs):
    """Obtém e registra o feed de notícias do scielo em perspectiva.

    Esta task é responsável por obter o feed de notícias do scielo em perspectiva,
    transformar e salvar no banco de dados do website."""
    mongo_connect()

    rss_news_feeds = Variable.get(
        "RSS_NEWS_FEEDS", default_var=RSS_NEWS_FEEDS, deserialize_json=True
    )

    try_fetch_and_register_feed(rss_news_feeds)


fetch_feed_content_task = PythonOperator(
    task_id="fetch_feed_content_task",
    python_callable=fetch_and_register_news_feed_callable,
    dag=dag,
)

fetch_feed_content_task
