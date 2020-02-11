import uuid
import logging
from datetime import datetime

import requests
import feedparser
from mongoengine.errors import ValidationError
from opac_schema.v1 import models
from tenacity import (
    retry,
    wait_exponential,
    stop_after_attempt,
    retry_if_exception_type,
    RetryError,
)


def NewsBuilder(entry: dict, language: str) -> models.News:
    """Recebe um dicionário contento informações sobre uma notícia e tranaforma
    em uma instância de opac_schema.v1.models.News.

    Por meio da url é verificado se uma notícia já está cadastrada na base do
    website, em caso de positivo, a notícia será atualizada."""

    url = entry.get("id")

    try:
        news = models.News.objects.get(url=url)
    except models.News.DoesNotExist:
        news = models.News()
        news._id = str(uuid.uuid4()).replace("-", "")

    news.url = entry.get("id")
    news.title = entry.get("title")
    news.description = entry.get("summary")
    news.image_url = entry.get("media_content", [{}])[-1].get("url")

    try:
        news.publication_date = datetime.strptime(
            entry["published"][5:25], "%d %b %Y %X"
        )
    except ValueError:
        news.publication_date = datetime.now()

    news.language = language

    return news


def PressReleaseBuilder(entry: dict, journal: models.Journal, language: str) -> models.PressRelease:
    """Recebe um dicionário contento informações sobre um press release e   transforma em uma instância de opac_schema.v1.models.PressRelease.
    """

    url = entry.get("id")

    try:
        press_release = models.PressRelease.objects.get(url=url)
    except models.PressRelease.DoesNotExist:
        press_release = models.PressRelease()
        press_release._id = str(uuid.uuid4()).replace("-", "")

    press_release.url = url
    press_release.title = entry.get("title")
    press_release.journal = journal
    press_release.language = language  # esperado: 'en' | 'es' | 'pt_BR'
    press_release.content = entry.get("summary")

    try:
        press_release.publication_date = datetime.strptime(
            entry["published"][5:25], "%d %b %Y %X"
        )
    except ValueError:
        press_release.publication_date = datetime.now()

    return press_release


@retry(
    wait=wait_exponential(),
    stop=stop_after_attempt(4),
    retry=retry_if_exception_type((requests.ConnectionError, requests.Timeout)),
)
def fetch_rss(url):
    return requests.get(url, timeout=10)


def try_fetch_and_register_news_feed(rss_news_feeds: dict) -> None:
    """Obtém as notícias de um feed, realiza o parser do XML para dicionário
    Python e registra na base de dados do Website."""

    for language, feed in rss_news_feeds.items():

        try:
            response = fetch_rss(feed["url"])
        except RetryError as exc:
            logging.error(
                "Could not fetch feed from '%s'.", feed["url"],
            )
            continue
        else:
            content = feedparser.parse(response.content)

        if content.bozo == 1:
            logging.error(
                "Could not parse feed content from '%s'. During processing this error '%s' was thrown.",
                feed["url"],
                content.bozo_exception,
            )

        for entry in content.get("entries", []):
            try:
                news = NewsBuilder(entry, language)
                news.save()
            except ValidationError as exc:
                logging.error(
                    "Could not save entry '%s', Please verify '%s'", entry, exc
                )
            else:
                logging.info("News '%s', saved successfully.", news.title)


def try_fetch_and_register_press_release_feed(rss_press_release: dict) -> None:
    """Obtém os periódicos públicos e correntes na base de dados do OPAC para coleta dos Press Release no Blog.
    """
    for journal in models.Journal.objects.filter(is_public=True, current_status='current'):
        for lang, feed in rss_press_release.items():
            feed_url_by_lang = feed['url'].format(lang, journal.acronym)

            try:
                response = fetch_rss(feed_url_by_lang)
            except RetryError as exc:
                logging.error(
                    "Could not fetch feed from '%s'.", feed_url_by_lang,
                )
                continue
            else:

                content = feedparser.parse(response.content)

            if content.bozo == 1:
                logging.error(
                    "Could not parse feed content from '%s'. During processing this error '%s' was thrown.",
                    feed_url_by_lang,
                    content.bozo_exception,
                )

            for entry in content.get("entries", []):
                try:
                    press_release = PressReleaseBuilder(entry, journal, lang)
                    press_release.save()
                except ValidationError as exc:
                    logging.error(
                        "Could not save entry '%s', Please verify '%s'", entry, exc
                    )
                else:
                    logging.info("Press Release '%s', saved successfully.", press_release.title)
