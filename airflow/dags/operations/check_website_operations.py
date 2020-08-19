import logging
import requests
import time

from urllib3.exceptions import MaxRetryError, NewConnectionError

from bs4 import BeautifulSoup


Logger = logging.getLogger(__name__)


def get_webpage_content(uri):
    response = access_uri(uri)
    return response.text


def get_webpage_href_and_src(content):
    href_items = {}

    soup = BeautifulSoup(content)

    href_items["href"] = [
        link.get('href')
        for link in soup.find_all('a')
        if link.get('href')
    ]
    href_items["src"] = [
        link.get('src')
        for link in soup.find_all(attrs={"src": True})
        if link.get('src')
    ]
    return href_items


def check_website_uri_list(uri_list_file_path, website_url_list):
    """
    Verifica o acesso de cada item da `uri_list_file_path`
    Exemplo de seu conteúdo:
        /scielo.php?script=sci_serial&pid=0001-3765
        /scielo.php?script=sci_issues&pid=0001-3765
        /scielo.php?script=sci_issuetoc&pid=0001-376520200005
        /scielo.php?script=sci_arttext&pid=S0001-37652020000501101
    """
    Logger.debug("check_website_uri_list IN")

    if not website_url_list:
        raise ValueError(
            "Unable to check the Web site resources are available "
            "because no Website URL was informed")

    uri_list_items = read_file(uri_list_file_path)

    uri_list_items = concat_website_url_and_uri_list_items(
        website_url_list, uri_list_items)

    total = len(uri_list_items)
    Logger.info("Quantidade de URI: %i", total)
    unavailable_uri_items = check_uri_list(uri_list_items)

    if unavailable_uri_items:
        Logger.info(
            "Não encontrados (%i/%i):\n%s",
            len(unavailable_uri_items), total,
            "\n".join(unavailable_uri_items))
    else:
        Logger.info("Encontrados: %i/%i", total, total)

    Logger.debug("check_website_uri_list OUT")


def read_file(uri_list_file_path):
    with open(uri_list_file_path) as fp:
        uri_list_items = fp.read().splitlines()
    return uri_list_items


def concat_website_url_and_uri_list_items(website_url_list, uri_list_items):
    if not website_url_list or not uri_list_items:
        return []
    items = []
    for website_url in website_url_list:
        for uri in uri_list_items:
            if uri:
                items.append(str(website_url) + str(uri))
    return items


def check_uri_list(uri_list_items):
    """Acessa uma lista de URI e retorna as que falharam"""
    failures = []
    for uri in uri_list_items:
        if not access_uri(uri):
            failures.append(uri)
    return failures


def requests_get(uri):
    try:
        response = requests.get(uri, timeout=10)
    except (requests.exceptions.ConnectionError,
            MaxRetryError,
            NewConnectionError) as e:
        Logger.error(
            "The URL '%s': %s",
            uri,
            e,
        )
        return False
    else:
        return response


def access_uri(uri):
    """Acessa uma URI e reporta o seu status de resposta"""

    response = requests_get(uri)
    if not response:
        return False

    if response.status_code in (200, 301, 302):
        return response

    if response.status_code in (429, 500, 502, 503, 504):
        return wait_and_retry_to_access_uri(uri)

    Logger.error(
        "The URL '%s' returned the status code '%s'.",
        uri,
        response.status_code,
    )
    return False


def retry_after():
    return (5, 10, 20, 40, 80, 160, 320, 640, )


def wait_and_retry_to_access_uri(uri):
    """
    Aguarda `t` segundos e tenta novamente até que status_code nao seja
    um destes (429, 500, 502, 503, 504)
    """
    available = False
    total_secs = 0
    for t in retry_after():
        Logger.info("Retry to access '%s' after %is", uri, t)
        total_secs += t
        time.sleep(t)

        response = requests_get(uri)

        if not response:
            available = False
            break

        if response.status_code in (429, 500, 502, 503, 504):
            continue

        if response.status_code in (200, 301, 302):
            available = response

    Logger.info(
        "The URL '%s' returned the status code '%s' after %is",
        uri,
        response.status_code,
        total_secs
    )
    return available
