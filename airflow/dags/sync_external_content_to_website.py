import logging
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from common.hooks import mongo_connect
from operations.sync_external_content_to_website_operations import (
    try_fetch_and_register_news_feed, try_fetch_and_register_press_release_feed,
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

RSS_PRESS_RELEASES_FEEDS_BY_CATEGORY = {
    'pt_BR': {
        'display_name': 'SciELO em Perspectiva Press Releases',
        'url': 'http://pressreleases.scielo.org/blog/category/{1}/feed/'
    },
    'es': {
        'display_name': 'SciELO en Perspectiva Press Releases',
        'url': 'http://pressreleases.scielo.org/{0}/category/press-releases/{1}/feed/',
    },
    'en': {
        'display_name': 'SciELO in Perspective Press Releases',
        'url': 'http://pressreleases.scielo.org/{0}/category/press-releases/{1}/feed/',
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

    try_fetch_and_register_news_feed(rss_news_feeds)


fetch_news_feed_content_task = PythonOperator(
    task_id="fetch_news_feed_content_task",
    python_callable=fetch_and_register_news_feed_callable,
    dag=dag,
)


def fetch_and_register_press_release_feed_callable(**kwargs):
    """Recupera os registros de Press Releases.

    Armazena na base de dados do OPAC no modelo PressRelease.

    Para obter os registro do Press Release é necessário os acrônimos dos periódicos, exemplo: ['rae', 'tce', 'acta'....].

    Link do Blog do SciELO para obter os dados: https://pressreleases.scielo.org/.

    Em definição com a equipe que faz a entrada dos Press Releases no Blog, será identificado os períodicos pelo acrômino e este será uma categoria no blog.

    Exemplo de URL para obter o feed do periódico com acrônimo ``rae``: https://pressreleases.scielo.org/blog/category/rae/feed/

    Essa tarefa garante a criação e atualização dos registro, porém não realiza qualquer deleção, portanto, é possível que tenhamos registros no site que não existam na fonte.
    """
    mongo_connect()

    rss_press_release_feeds = Variable.get(
        "RSS_PRESS_RELEASES_FEEDS_BY_CATEGORY", default_var=RSS_PRESS_RELEASES_FEEDS_BY_CATEGORY, deserialize_json=True
    )

    try_fetch_and_register_press_release_feed(rss_press_release_feeds)

fetch_press_release_content_task = PythonOperator(
    task_id="fetch_press_release_feed_content_task",
    python_callable=fetch_and_register_press_release_feed_callable,
    dag=dag,
)


fetch_news_feed_content_task >> fetch_press_release_content_task
