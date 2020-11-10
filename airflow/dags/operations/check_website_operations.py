import csv
import time
import json
import logging
import requests
from urllib3.exceptions import MaxRetryError, NewConnectionError
from urllib.parse import urlparse, parse_qs
from datetime import datetime, date

from bs4 import BeautifulSoup

from common import async_requests
from common.hooks import add_execution_in_database
from operations.docs_utils import (
    is_pid_v2,
    get_document_manifest,
    get_document_data_to_generate_uri,
    get_document_assets_data,
    get_document_renditions_data,
)

Logger = logging.getLogger(__name__)
DO_REQ_TIMEOUT = 10


def time_diff(t1, t2):
    dt = t2 - t1
    return dt.total_seconds()


def fixes_for_json(o):
    if isinstance(o, (date, datetime)):
        return o.isoformat() + "Z"
    return o


def get_pid_list_from_csv(csv_file_path):
    """
    Le um arquivo csv, cuja primeira coluna é o pid v2
    Considerar que pode haver uma segunda coluna que contém o "previous" pid v2
    Returns:
        list: lista de pid v2
    """
    pids = []
    try:
        with open(csv_file_path, newline='') as f:
            for row in csv.reader(f):
                if len(row) > 0:
                    pids.append(row[0])
                if len(row) > 1 and len(row[1]) == 23:
                    pids.append(row[1])
    except (csv.Error, OSError) as e:
        Logger.exception("Unable to get PIDs from %s (%s)", csv_file_path, e)
    return pids


def get_uri_list_from_pid_dict(grouped_pids):
    uri_list = []
    for journal_pid, issue_pids in grouped_pids.items():
        data = {"pid_v2": journal_pid, "script": "sci_serial"}
        uri_list.append(get_classic_document_webpage_uri(data))
        data = {"pid_v2": journal_pid, "script": "sci_issues"}
        uri_list.append(get_classic_document_webpage_uri(data))
        for issue_pid, doc_pids in issue_pids.items():
            data = {"pid_v2": issue_pid, "script": "sci_issuetoc"}
            uri_list.append(get_classic_document_webpage_uri(data))
            for doc_pid in doc_pids:
                data = {"pid_v2": doc_pid, "script": "sci_arttext"}
                uri_list.append(get_classic_document_webpage_uri(data))
                data = {"pid_v2": doc_pid, "script": "sci_pdf"}
                uri_list.append(get_classic_document_webpage_uri(data))
    return uri_list


class InvalidResponse:
    def __init__(self, uri=None):
        self.status_code = None
        self.start_time = None
        self.end_time = None
        self.uri = uri


def do_request(uri, function=None, secs_sequence=None, timeout=DO_REQ_TIMEOUT):
    """
    Executa requisições (`requests.head` ou `requests.get`) com tentativas
    em tempos espaçados enquanto status_code retornado é um destes valores
    (500, 502, 503, 504)

    Args:
        uri (str): URI
        function (callable): `requests.head` ou `requests.get`
    Returns:
        HTTPResponse or False
    """
    function = function or requests.head
    secs_sequence = secs_sequence or retry_after()
    total_secs = 0
    times = 0
    start_time = datetime.utcnow()
    for t in secs_sequence:
        times += 1
        Logger.info("Attempt %i: wait %.2fs before access '%s'", times, t, uri)

        total_secs += t
        time.sleep(t)

        response = requests_get(uri, function, timeout=timeout)
        try:
            if response.status_code in (500, 502, 503, 504):
                continue
        except AttributeError:
            response = InvalidResponse()
        # break para `InvalidResponse` e
        # para `response.status_code` not in (500, 502, 503, 504)
        break

    response.end_time = datetime.utcnow()
    response.start_time = start_time

    Logger.info(
        "The URL '%s' returned the status code '%s' after %is",
        uri,
        response.status_code,
        total_secs
    )
    return response


def is_valid_response(response):
    try:
        return response.status_code in (200, 301, 302)
    except AttributeError:
        return False


def eval_response(response):
    if response is None:
        return {}
    try:
        duration = time_diff(response.start_time, response.end_time)
    except (AttributeError, TypeError, ValueError):
        duration = None
    return {
        "available": response.status_code in (200, 301, 302),
        "status code": response.status_code,
        "start time": response.start_time,
        "end time": response.end_time,
        "duration": duration,
    }


def get_kernel_document_id_from_classic_document_uri(classic_website_document_uri, timeout=DO_REQ_TIMEOUT):
    """
    >>> resp = requests.head("https://new.scielo.br/scielo.php?script=sci_arttext&pid=S0100-40422020000700987")
    >>> parsed = urlparse(resp.headers.get('Location'))
    >>> parsed
    ParseResult(scheme='https', netloc='new.scielo.br', path='/j/qn/a/RsJ6CyVbQP3q9cMWqBGyHjp/', params='', query='', fragment='')
    """
    resp = do_request(classic_website_document_uri, requests.head, timeout=DO_REQ_TIMEOUT)
    status_code = redirected_location = None
    try:
        status_code = resp.status_code
        redirected_location = resp.headers.get('Location')
        if not redirected_location:
            raise ValueError("Not found redirected location")
    except (AttributeError, ValueError) as e:
        Logger.error(
            "%s (%s): %s", classic_website_document_uri, status_code, e)
    else:
        Logger.info(
            "%s: %s", classic_website_document_uri, redirected_location)
        parsed = urlparse(redirected_location)
        splitted = [item for item in parsed.path.split("/") if item]
        #  path='/j/qn/a/RsJ6CyVbQP3q9cMWqBGyHjp/'
        if len(splitted[-1]) == 23:
            return splitted[-1]


def check_doc_webpage_uri_items_expected_in_webpage(existing_uri_items_in_html,
                                other_webpages_data):
    """
    Verifica os recursos de um documento, comparando os recursos registrados
    no Kernel com os recursos existentes na página do documento no site público

    Args:
        existing_uri_items_in_html (list): Lista de recursos que
            foram encontrados dentro da página do documento
        assets_data (list of dict, retorno de `get_document_assets_data`):
        other_webpages_data (list of dict,
            mesmo formato `retornado de get_document_webpages_data`):
            Dados de outras _webpages_ do documento,
            ou seja, outro idioma e outro formato,
            com uri(s) que se espera encontrar no HTML

    Returns:
        tuple:
            list of dict: resultado da verficação de cada recurso avaliado,
                mais dados do recurso, cujas chaves são:
                type, id, present_in_html, absent_in_html
            dict: chaves formatos html ou pdf, e valores de dicionário cujas
                chaves são "total" e "missing"
    """
    results = []
    formats = {}
    for other_version_uri_data in other_webpages_data:
        # {"doc_id": "", "lang": "", "format": "", "uri": ""},
        fmt = other_version_uri_data["format"]
        uri_result = {}
        uri_result["type"] = fmt
        uri_result["id"] = other_version_uri_data["lang"]
        uri_result["present_in_html"] = []
        alternatives = other_version_uri_data["uri_alternatives"]
        for uri in alternatives:
            if uri in existing_uri_items_in_html:
                uri_result["present_in_html"] = [uri]
                break
        if not uri_result["present_in_html"]:
            uri_result["absent_in_html"] = alternatives

        results.append(uri_result)

        formats[fmt] = formats.get(fmt) or []
        formats[fmt].append(bool(uri_result["present_in_html"]))

    summarized = calculate_missing_and_total(formats)
    return results, summarized


def calculate_missing_and_total(items):
    """
    Recebe um dicionário `items`, cujo valor é uma lista de elementos `bool`
    retorna um dicionário com mesmas chaves, mas os valores são um dicionário
    cujas chaves são `total` e `missing` e os respectivos valores são
    total de elementos da lista e total de False
    """
    result = {}
    for k, values in items.items():
        result[k] = {
            "total": len(values),
            "missing": values.count(False),
        }
    return result


def check_asset_uri_items_expected_in_webpage(existing_uri_items_in_html,
                                 assets_data):
    """
    Verifica os recursos de um documento, comparando os recursos registrados
    no Kernel com os recursos existentes na página do documento no site público

    Args:
        existing_uri_items_in_html (list): Lista de recursos que
            foram encontrados dentro da página do documento
        assets_data (list of dict, retorno de `get_document_assets_data`):
            Dados de ativos digitais com uri(s) que se espera encontrar no HTML
    Returns:
        tuple:
            list of dict: resultado da verficação de cada recurso avaliado,
                mais dados do recurso, cujas chaves são:
                type, id, present_in_html, absent_in_html
            dict: cujas chaves são totais: `total expected`, `total missing`,
                `total alternatives`, `total alternatives present in html`
    """
    results = []
    summary = {
        "total expected": len(assets_data),
        "total missing": 0,
        "total alternatives": sum(
            [len(a["uri_alternatives"])
             for a in assets_data]),
        "total alternatives present in html": 0,
    }
    incorrects = []
    for asset_data in assets_data:
        # {"prefix": prefix, "uri_alternatives": [],}
        uri_result = {}
        uri_result["type"] = "asset"
        uri_result["id"] = asset_data["prefix"]
        uri_result["present_in_html"] = []
        uri_result["absent_in_html"] = []
        for uri in asset_data["uri_alternatives"]:
            if uri in existing_uri_items_in_html:
                uri_result["present_in_html"].append(uri)
            else:
                uri_result["absent_in_html"].append(uri)
        if not uri_result["present_in_html"] and not uri_result["absent_in_html"]:
            incorrects += [asset_data["prefix"]]
            uri_result["incorrect"] = True
        results.append(uri_result)
        if not uri_result["present_in_html"]:
            summary["total missing"] += 1
        summary["total alternatives present in html"] += len(
            uri_result["present_in_html"])
    if incorrects:
        summary["total incorrect assets"] = len(incorrects)
    return results, summary


def get_document_webpage_uri_altenatives(data):
    """
    Retorna as variações de uri do documento no padrão:
        /j/:acron/a/:id_doc?format=pdf&lang=es
    """
    items = [
        get_document_webpage_uri(data, ("format", "lang")),
        get_document_webpage_uri(data, ("lang", "format")),
        get_document_webpage_uri(data, ("format",)),
    ]
    if data.get("format") == "html":
        items.append(get_document_webpage_uri(data, ("lang", )))
    for item in list(items):
        items.append(item.replace("?", "/?"))
    return items


def get_classic_document_webpage_uri(data):
    """
    Recebe data
    retorna uri no padrao
    /scielo.php?script=sci_arttext&pid=S0001-37652020000501101&tlng=lang
    /scielo.php?script=sci_pdf&pid=S0001-37652020000501101&tlng=lang
    """
    if data.get("format") == "pdf":
        script = "sci_pdf"
    else:
        script = data.get("script") or "sci_arttext"
    uri = "/scielo.php?script={}&pid={}".format(script, data['pid_v2'])
    if data.get("lang"):
        uri += "&tlng={}".format(data.get("lang"))
    return uri


def group_items_by_script_name(uri_items):
    """
    Recebe uri_items:
    /scielo.php?script=sci_arttext&pid=S0001-37652020000501101&tlng=lang
    /scielo.php?script=sci_pdf&pid=S0001-37652020000501101&tlng=lang
    e agrupa pelo valor de `script`.

    Args:
        uri_items (str list): no padrão
        /scielo.php?script=sci_arttext&pid=S0001-37652020000501101&tlng=lang
        /scielo.php?script=sci_pdf&pid=S0001-37652020000501101&tlng=lang

    Returns:
        dict (key: `script`, value: list of uri_items)

    """
    items = {}
    Logger.info("Total %i URIs", len(uri_items))
    for uri in uri_items:
        parsed = urlparse(uri)
        query_items = parse_qs(parsed.query)
        values = query_items.get('script')
        if values:
            script_name = values[0]
            items[script_name] = items.get(script_name) or []
            items[script_name].append(uri)
    return items


def get_journal_issue_doc_pids(uri):
    """
    A partir de um URI neste padrão:
        /scielo.php?script=sci_arttext&pid=S0001-37652020000501101&tlng=lang
        /scielo.php?script=sci_pdf&pid=S0001-37652020000501101&tlng=lang
    extrair os respectivos PIDs: journal, issue, doc
    """
    parsed = urlparse(uri)
    query_items = parse_qs(parsed.query)
    values = query_items.get('pid')
    pid_d = None
    pid_i = None
    pid_j = None
    if values:
        pid = values[0]
        if len(pid) == 9:
            pid_j = pid
        elif len(pid) == 17:
            pid_i = pid
            pid_j = pid[:9]
        elif len(pid) == 23:
            pid_d = pid
            pid_i = pid[1:18]
            pid_j = pid[1:10]
    return pid_j, pid_i, pid_d


def get_document_webpage_uri(data, query_param_names=None):
    """
    Recebe data
    retorna uri no padrao /j/:acron/a/:id_doc?format=pdf&lang=es
    """
    uri = "/j/{}/a/{}".format(data['acron'], data['doc_id'])
    query_param_names = query_param_names or ("format", "lang")
    query_items = [
        "{}={}".format(name, data.get(name))
        for name in query_param_names
        if data.get(name)
    ]
    if len(query_items):
        uri += "?" + "&".join(query_items)
    return uri


def get_document_webpages_data(doc_id, doc_data_list, doc_webpage_uri_function=None):
    """
    Gera `uri` para as várias _webpages_ do documento

    Args:
        doc_id (str): ID do documento (Kernel)
        doc_data_list (list of dict): dicionário contém dados do documento
            suficientes para sua identificação e formação de URI
        doc_webpage_uri_function (callable): função que forma a URI

    Returns:
        dict: mesmo conteúdo da lista `doc_data_list`, sendo que cada elemento
            terá as chaves novas, "uri" e "doc_id".
            `uri` é do site novo ou antigo, preferencialmente novo, mas o valor
            gerado dependerá dos dados presentes em `doc_data_list`
    """
    doc_data = doc_data_list[0]
    acron = doc_data.get("acron")
    pid_v2 = doc_data.get("pid_v2")

    if doc_webpage_uri_function is None:
        if acron:
            doc_webpage_uri_function = get_document_webpage_uri
        elif pid_v2:
            doc_webpage_uri_function = get_classic_document_webpage_uri

    if doc_webpage_uri_function is None:
        raise ValueError(
            "get_document_webpages_data requires `acron` or `pid_v2`")

    if doc_webpage_uri_function == get_document_webpage_uri and not acron:
        raise ValueError("get_document_webpages_data requires `acron`")
    if doc_webpage_uri_function == get_classic_document_webpage_uri and not is_pid_v2(pid_v2):
        raise ValueError("get_document_webpages_data requires `pid v2`")

    uri_items = []
    for doc_data in doc_data_list:
        data = {
            "doc_id": doc_id,
        }
        doc_data.update(data)
        doc_data["uri"] = doc_webpage_uri_function(doc_data)
        doc_data["uri_alternatives"] = get_document_webpage_uri_altenatives(
            doc_data)
        uri_items.append(doc_data)
    return uri_items


def get_webpage_content(uri):
    response = do_request(uri, requests.get)
    if is_valid_response(response):
        return response.text


def find_uri_items(content):
    soup = BeautifulSoup(content, features="lxml")

    uri_items = [
        link.get('href')
        for link in soup.find_all('a')
        if link.get('href')
    ]
    uri_items.extend(
        [
            link.get('src')
            for link in soup.find_all(attrs={"src": True})
            if link.get('src')
        ]
    )
    return sorted(list(set(uri_items)))


def filter_uri_list(uri_items, expected_netlocs):
    """
    Retorna uri list filtrada por `expected_netlocs`, ou seja, apenas as URI cujo domínio
    está informado na lista `expected_netlocs`
    No entanto, se expected_netlocs ausente ou é uma lista vazia, retorna `uri_items`
    sem filtrar

    Args:
        uri_items (list): lista de URI
        expected_netlocs (list): lista de domínios

    Returns:
        list: a mesma lista de entrada, se expected_netlocs é vazio ou None, ou
            lista filtrada, apenas URI cujo domínio está presente na lista
            `expected_netlocs`

    >>> u = urlparse('https://www.cwi.nl:80/%7Eguido/Python.html')
    >>> u
        ParseResult(
            scheme='https', netloc='www.cwi.nl:80',
            path='/%7Eguido/Python.html', params='', query='', fragment='')
    """
    if not expected_netlocs:
        return uri_items
    items = []
    for uri in uri_items:
        parsed = urlparse(uri)
        if parsed.netloc in expected_netlocs:
            if parsed.netloc == "":
                if parsed.path.startswith("/") and len(parsed.path) > 1:
                    items.append(uri)
            else:
                items.append(uri)
    return items


def check_document_html(uri, assets_data, other_webpages_data, object_store_url):
    """
    Verifica se, no documento HTML, os ativos digitais e outras
    _webpages_ do documento (HTML e PDF) estão mencionados,
    ou seja, em `img/@src` e/ou `*[@href]`

    Args:
        uri (str): URL do documento HTML no site público
        assets_data (list of dict): dicionário contém dados do ativo digital
        other_webpages_data (list of dict): dicionário contém metadados do
            documento suficientes para formar URI e também identificar
            o documento.
            Um documento pode ter várias URI devido à variação de formatos e
            idiomas
        object_store_url (str): URL do _Object Store_, usada para obter as
            URI encontradas no HTML que sejam apenas do _domínio_ do SPF
    Returns:
        report (dict):
            `available` (bool),
            `components` (list of dict): validação de cada ativos digital, ou
                menção às demais _webpages_ do documento (HTML/PDF/idiomas)

    """
    response = do_request(uri, requests.get)
    result = eval_response(response)
    expected_components_qty = len(assets_data) + len(other_webpages_data)
    if result.get("available") is False:
        result["total expected components"] = expected_components_qty
        result["total missing components"] = expected_components_qty
        return result

    content = response.text

    # lista de uri encontradas dentro da página
    filtered_by = []
    if object_store_url:
        parsed = urlparse(object_store_url)
        filtered_by = [parsed.netloc, ""]
    existing_uri_items_in_html = filter_uri_list(
        find_uri_items(content), filtered_by)

    # verifica se as uri esperadas estão present_in_html e no html da página
    # do documento, dados os dados dos ativos digitais e das
    # demais _webpages_ (formato e idioma) do documento
    check_doc_webpages_result, summarized_doc_webpages_result = check_doc_webpage_uri_items_expected_in_webpage(
        existing_uri_items_in_html, other_webpages_data
    )
    check_assets_result, summarized_check_assets_result = check_asset_uri_items_expected_in_webpage(
        existing_uri_items_in_html, assets_data
    )
    missing_doc_webpages = sum(
        [r["missing"]
         for r in summarized_doc_webpages_result.values()])
    missing_assets = summarized_check_assets_result["total missing"]
    result.update({
        "components": check_assets_result + check_doc_webpages_result,
        "total expected components": expected_components_qty,
        "total missing components": missing_doc_webpages + missing_assets,
    })
    result.update(summarized_doc_webpages_result)
    result.update({"assets": summarized_check_assets_result})

    if missing_doc_webpages + missing_assets:
        result.update(
            {"existing_uri_items_in_html": sorted(existing_uri_items_in_html)})
    return result


def check_document_html_content(content, assets_data, other_webpages_data, object_store_url):
    """
    Verifica se, no documento HTML, os ativos digitais e outras
    _webpages_ do documento (HTML e PDF) estão mencionados,
    ou seja, em `img/@src` e/ou `*[@href]`

    Args:
        html_content (str): conteúdo do html
        assets_data (list of dict): dicionário contém dados do ativo digital
        other_webpages_data (list of dict): dicionário contém metadados do
            documento suficientes para formar URI e também identificar
            o documento.
            Um documento pode ter várias URI devido à variação de formatos e
            idiomas
        object_store_url (str): URL do _Object Store_, usada para obter as
            URI encontradas no HTML que sejam apenas do _domínio_ do SPF
    Returns:
        report (dict):
            `available` (bool),
            `components` (list of dict): validação de cada ativos digital, ou
                menção às demais _webpages_ do documento (HTML/PDF/idiomas)

    """
    result = {}
    expected_components_qty = len(assets_data) + len(other_webpages_data)
    if content is None:
        result["total expected components"] = expected_components_qty
        result["total missing components"] = expected_components_qty
        return result

    # lista de uri encontradas dentro da página
    filtered_by = []
    if object_store_url:
        parsed = urlparse(object_store_url)
        filtered_by = [parsed.netloc, ""]
    existing_uri_items_in_html = filter_uri_list(
        find_uri_items(content), filtered_by)

    # verifica se as uri esperadas estão present_in_html e no html da página
    # do documento, dados os dados dos ativos digitais e das
    # demais _webpages_ (formato e idioma) do documento
    check_doc_webpages_result, summarized_doc_webpages_result = check_doc_webpage_uri_items_expected_in_webpage(
        existing_uri_items_in_html, other_webpages_data
    )
    check_assets_result, summarized_check_assets_result = check_asset_uri_items_expected_in_webpage(
        existing_uri_items_in_html, assets_data
    )
    missing_doc_webpages = sum(
        [r["missing"]
         for r in summarized_doc_webpages_result.values()])
    missing_assets = summarized_check_assets_result["total missing"]
    result.update({
        "components": check_assets_result + check_doc_webpages_result,
        "total expected components": expected_components_qty,
        "total missing components": missing_doc_webpages + missing_assets,
    })
    result.update(summarized_doc_webpages_result)
    result.update({"assets": summarized_check_assets_result})

    if missing_doc_webpages + missing_assets:
        result.update(
            {"existing_uri_items_in_html": sorted(existing_uri_items_in_html)})
    return result


def check_document_webpages_availability(website_url, doc_data_list, assets_data, object_store_url):
    """
    Verifica a disponibilidade do documento nos respectivos formatos e idiomas.
    No caso, do HTML, inclui a verificação se os ativos digitais e outras
    _webpages_ do documento (HTML e PDF) estão mencionadas dentro do HTML,
    ou seja, em `img/@src` e/ou `*[@href]`

    Args:
        website_url (str): URL do site público
        doc_data_list (list of dict): dicionário contém metadados do documento
            suficientes para formar URI e também identificar o documento.
            Um documento pode ter várias URI devido à variação de formatos e
            idiomas
        assets_data (list of dict): dicionário contém dados do ativo digital
            como URI e identificação
        object_store_url (str): URL do _Object Store_, usada para obter as
            URI encontradas no HTML que sejam apenas do _domínio_ do SPF

    Returns:
        tuple (list of dict, dict):
            list of dict: mesma lista `doc_data_list`, sendo que cada
                elemento, recebe novas chaves e valores:
                    `uri` (formada com os dados),
                    `available` (bool),
                    `components` (list of dict):
                        validação de cada ativos digital, ou menção às demais
                        _webpages_ do documento (HTML/PDF/idiomas)
            dict: {
                "web html": {
                    "total": 1, "total unavailable": 0, "total incomplete": 0},
                "web pdf": {
                    "total": 1, "total unavailable": 0},
                },

    """
    report = []
    summary = {}

    for doc_data in doc_data_list:
        key = "web {}".format(doc_data["format"])
        summary[key] = summary.get(key) or {"total": 0, "total unavailable": 0}
        summary[key]["total"] += 1
        doc_uri = website_url + doc_data.get("uri")
        result = doc_data.copy()
        if "uri_alternatives" in result.keys():
            del result["uri_alternatives"]
        result.update(
            {
                "uri": doc_uri,
            }
        )
        Logger.info("Verificando página do documento: %s", doc_uri)
        if doc_data.get("format") == "html":
            summary[key]["total incomplete"] = summary[key].get(
                    "total incomplete") or 0
            # lista de uri para outro idioma e/ou formato
            other_webpages_data = list(doc_data_list)
            other_webpages_data.remove(doc_data)
            components_result = check_document_html(
                                        doc_uri,
                                        assets_data,
                                        other_webpages_data,
                                        object_store_url)
            result.update(components_result)
            if result.get("available") is True and result["total missing components"] > 0:
                summary[key]["total incomplete"] += 1
            report.append(result)
        else:
            result.update(
                eval_response(do_request(doc_uri))
            )
            report.append(result)
        if result.get("available") is False:
            summary[key]["total unavailable"] += 1

    return report, summary


def add_responses(doc_data_list, website_url=None, request=True, timeout=None):
    """
    Dada uma lista de dicionários que contém uma chave `uri`,
    se aplicável, concatena valor de `uri` com valor de `website_url`,
    faz as requisições de todos os URI presentes nesta lista,
    e atualiza cada elemento da lista (dicionário) com a chave `response` e o
    respectivo resultado da requisição
    Args:
        doc_data_list (list of dict): lista de dicionários, obrigatoriamente
        tem a chave `uri`.
    Returns:
        None
    """
    body = False
    if website_url:
        for doc_data in doc_data_list:
            doc_data["uri"] = website_url + doc_data["uri"]
            body = doc_data.get("format") == "html"
    uri_items = (doc_data["uri"] for doc_data in doc_data_list if doc_data["uri"])

    responses = {}
    if request:
        responses = async_requests.parallel_requests(uri_items, body=body, timeout=timeout)
        responses = {resp.uri: resp for resp in responses}

    for doc_data in doc_data_list:
        doc_data["response"] = responses.get(doc_data["uri"])


def check_html_webpages_availability(html_data_items, assets_data, webpages_data, object_store_url):
    """
    Verifica também se os ativos digitais e outras
    _webpages_ do documento (HTML e PDF) estão mencionadas dentro do HTML,
    ou seja, em `img/@src` e/ou `*[@href]`

    Args:
        html_data_items (list of dict): dicionário contém metadados do documento
            suficientes para formar URI e também identificar o documento.
            Um documento pode ter várias URI devido à variação de formatos e
            idiomas
        assets_data (list of dict): dicionário contém dados do ativo digital
            como URI e identificação
        webpages_data (list of dict): dicionário contém metadados do documento
            suficientes para formar URI e também identificar o documento.
            Um documento pode ter várias URI devido à variação de formatos e
            idiomas
        object_store_url (str): URL do _Object Store_, usada para obter as
            URI encontradas no HTML que sejam apenas do _domínio_ do SPF

    Returns:
        tuple (list of dict, dict):
            list of dict: mesma lista `html_data_items`, sendo que cada
                elemento, recebe novas chaves e valores:
                    `uri` (formada com os dados),
                    `available` (bool),
                    `components` (list of dict):
                        validação de cada ativos digital, ou menção às demais
                        _webpages_ do documento (HTML/PDF/idiomas)
            dict:
                {"total": 1, "total unavailable": 0, "total incomplete": 0},

    """
    report = []
    unavailable = 0
    incomplete = 0

    for doc_data in html_data_items:
        doc_uri = doc_data["uri"]

        response = doc_data["response"]
        del doc_data["response"]

        result = doc_data.copy()
        if "uri_alternatives" in result.keys():
            del result["uri_alternatives"]

        Logger.info("Verificando página do documento: %s", doc_uri)

        # lista de uri para outro idioma e/ou formato
        other_webpages_data = webpages_data.copy()
        other_webpages_data.remove(doc_data)

        try:
            content = response.text
        except (AttributeError):
            content = None
        result.update(eval_response(response))
        components_result = check_document_html_content(
                                    content,
                                    assets_data,
                                    other_webpages_data,
                                    object_store_url)
        result.update(components_result)
        report.append(result)
        if result.get("available") is True and result["total missing components"] > 0:
            incomplete += 1
        if result.get("available") is False:
            unavailable += 1
    summary = {
        "total": len(html_data_items),
        "total unavailable": unavailable,
        "total incomplete": incomplete,
    }
    return report, summary


def check_responses(uri_data_items):
    """
    Verifica os dados de `response`

    Args:
        uri_data_items (list of dict): lista de _renditions_ ou de _assets_
    Returns:
        list of dict, int:
            mesma lista de dicionários da entrada + dados da avaliação de
                `response`
            quantidade de ítens indisponíveis
    """
    report = []
    unavailable = 0
    for item in uri_data_items:
        result = item.copy()
        result.update(eval_response(item["response"]))

        del result["response"]
        report.append(result)
        if result.get("available") is False:
            unavailable += 1
    return report, unavailable


def check_pdf_webpages_availability(doc_data_list):
    """
    Verifica a disponibilidade do documento nos respectivos idiomas.
    Args:
        doc_data_list (list of dict): dicionário contém metadados do documento
            suficientes para formar URI e também identificar o documento.
            Um documento pode ter várias URI devido à variação de formatos e
            idiomas

    Returns:
        tuple (list of dict, dict):
            list of dict: mesma lista `doc_data_list`, sendo que cada
                elemento, recebe novas chaves e valores:
                    `uri` (formada com os dados),
                    `available` (bool),
            dict: {"total": 1, "total unavailable": 0}
    """
    report = []
    summary = {}
    unavailable = 0

    for doc_data in doc_data_list:
        doc_uri = doc_data["uri"]

        response = doc_data["response"]
        del doc_data["response"]

        result = doc_data.copy()
        if "uri_alternatives" in result.keys():
            del result["uri_alternatives"]

        Logger.info("Verificando página do documento: %s", doc_uri)

        # lista de uri para outro idioma e/ou formato
        result.update(eval_response(response))
        report.append(result)

        if result.get("available") is False:
            unavailable += 1

    summary = {
        "total": len(doc_data_list),
        "total unavailable": unavailable,
    }
    return report, summary


def check_website_uri_list(uri_list_items, label="", timeout=None):
    """
    Verifica a disponibilidade dos URI de `uri_list_items`
    Args:
        uri_list_items (str list): lista de uri para verificar a
            disponibilidade
    Returns:
        tuple: (success (dict list), failures (dict list))
        ```
        {
            "available": True,
            "status code": 200,
            "start time": START_TIME,
            "end time": END_TIME,
            "duration": DURATION,
            "uri": uri_list[3]
        },
        ```
    """
    Logger.debug("check_website_uri_list IN")

    total = len(uri_list_items)
    Logger.info("Total %s URIs: %i", label, total)
    success, failures = check_uri_items(uri_list_items, timeout)

    if failures:
        Logger.info(
            "Unavailable %s URIs (%i/%i):\n%s",
            label,
            len(failures), total,
            "\n".join(sorted([item["uri"] for item in failures])))
    else:
        Logger.info("Total available %s URIs: %i/%i", label, total, total)

    Logger.debug("check_website_uri_list OUT")
    return success, failures


def register_sci_pages_availability_report(result_items, dag_info):
    """
    Registra os resultados da verificação da disponibilidade na base de dados.
    Args:
        result_items (list of dict): resultado de `check_uri_items`
            {
                "available": True,
                "status code": 200,
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
                "uri": uri_list[3]
            }
        dag_info (dict): dados da DAG
    """
    Logger.info("Register uri items availability report")
    total = len(result_items)
    for i, row in enumerate(result_items):
        Logger.info("Registering %i/%i", i, total)
        add_execution_in_database(
            "sci_pages_availability",
            format_sci_page_availability_result_to_register(row, dag_info))
    Logger.info("Finished: register uri items availability report")


def format_sci_page_availability_result_to_register(sci_page_availability_result, dag_info):
    """
    Formata os dados para registrar na tabela `sci_pages_availability`
    Colunas da tabela:
      id SERIAL PRIMARY KEY,
      dag_run varchar(255),                             -- Identificador de execução da dag `check_website`
      input_file_name varchar(255) NULL,                -- Nome do arquivo de entrada: csv com PIDs v2 ou uri_list
      failed bool DEFAULT false,                        -- Falha == true
      detail json,                                      -- Detalhes da verificação da disponibilidade
      pid_v2_journal varchar(9),                        -- ISSN ID do periódico
      pid_v2_issue varchar(17) NULL,                    -- PID do fascículo
      pid_v2_doc varchar(23) NULL,                      -- PID do documento
      created_at timestamptz DEFAULT now()
    Args:
        sci_page_availability_result (dict)
            {
                "available": True,
                "status code": 200,
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
                "uri": uri_list[3]
            }
        dag_info (dict): dados da DAG

    Returns:
        dict: dados da coluna
    """
    parsed_pids = get_journal_issue_doc_pids(
        sci_page_availability_result["uri"])
    data = {}
    data["dag_run"] = dag_info.get("run_id")
    data["input_file_name"] = dag_info.get("input_file_name")
    data["uri"] = sci_page_availability_result["uri"]
    data["failed"] = sci_page_availability_result.get("available") is False
    data["detail"] = json.dumps(
        sci_page_availability_result, default=fixes_for_json)
    data["pid_v2_journal"] = parsed_pids[0]
    data["pid_v2_issue"] = parsed_pids[1]
    data["pid_v2_doc"] = parsed_pids[2]
    return data


def read_file(uri_list_file_path):
    with open(uri_list_file_path, "r") as fp:
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


def check_uri_items(uri_list_items, timeout=None):
    """Acessa uma lista de URI e retorna o resultado da verificação"""

    if not uri_list_items:
        return [], []

    success = []
    failures = []
    responses = async_requests.parallel_requests(uri_list_items, timeout=timeout)
    for resp in responses:
        result = eval_response(resp)
        result.update({"uri": resp.uri})
        if result.get("available") is True:
            success.append(result)
        else:
            failures.append(result)
    return success, failures


def requests_get(uri, function=None, timeout=DO_REQ_TIMEOUT):
    try:
        function = function or requests.head
        response = function(uri, timeout=timeout or DO_REQ_TIMEOUT)
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


def format_document_webpage_availability_to_register(
        document_webpage_availability, extra_data={}):
    """
    Formata os dados da avaliação da disponibilidade de uma _webpage_
    do documento para linhas em uma tabela

    Args:
        document_webpage_availability (dict): dados do documento para
            identificação e para formar URI
            {
                "lang": sps_package.original_language,
                "format": "html",
                "pid_v2": sps_package.scielo_pid_v2,
                "acron": sps_package.acron,
                "doc_id_for_human": sps_package.package_name,
                "doc_id": "",
                "uri": "",
                "available": bool,
                "components": [
                    {"type": "", "id": "",
                     "present_in_html": bool, "uri": "" or []
                ]
            }
    """
    rows = []
    doc_data = extra_data.copy()
    doc_data["annotation"] = ""
    for name in ("doc_id", "pid_v2", "doc_id_for_human"):
        doc_data[name] = document_webpage_availability[name]
    doc_data_result = doc_data.copy()
    doc_data_result["type"] = document_webpage_availability["format"]
    doc_data_result["id"] = document_webpage_availability["lang"]
    doc_data_result["uri"] = document_webpage_availability["uri"]
    doc_data_result["status"] = ("available"
                                 if document_webpage_availability["available"]
                                 else "not available")

    # linha sobre cada _webpage_ do documento
    rows.append(doc_data_result)

    for component in document_webpage_availability.get("components") or []:
        component_data = doc_data.copy()
        component_data["type"] = component["type"]
        component_data["id"] = component["id"]
        row = component_data.copy()
        if not component["present_in_html"]:
            row["uri"] = str(row["absent_in_html"])
            row["annotation"] = "Existing in HTML:\n{}".format(
                    "\n".join(
                        document_webpage_availability["existing_uri_items_in_html"])
                    )
        row["status"] = ("present in HTML"
                         if component["present_in_html"]
                         else "absent in HTML")
        if row.get("present_in_html"):
            del row["present_in_html"]
        rows.append(row)
    return rows


def format_document_items_availability_to_register(document_data,
        document_items_availability, extra_data={}):
    """
    Formata os dados da avaliação da disponibilidade de items (ativos digitais
    e renditions) de um documento para linhas em uma tabela

    Args:
        document_data (dict): dados do documento
        document_items_availability (list of dict): dados do documento para
            identificação e para formar URI
            {
                "type": "tipo",
                "id": "identificação do item",
                "uri": "",
                "available": bool,
            }
    Returns:
        list
    """
    rows = []
    doc_data = extra_data.copy()
    doc_data["annotation"] = ""
    for name in ("doc_id", "pid_v2", "doc_id_for_human"):
        doc_data[name] = document_data[name]

    for doc_item_availability in document_items_availability or []:
        row = doc_data.copy()
        row.update(doc_item_availability)
        row["status"] = ("available"
                         if doc_item_availability["available"]
                         else "not available")
        del row["available"]
        rows.append(row)
    return rows


def group_doc_data_by_webpage_type(document_webpages_data):
    """
    Cria um dicionário em que:
    as chaves: "web " + o valor de `document_webpages_data[index]['format']`,
        ou seja, _web pdf_ ou _web html_
    os valores: os ítens

    Args:
        document_webpages_data (list of dict): retorno de
            `get_document_webpages_data`
    Returns:
        dict: mesmos dados mas orgnizados em dicionário em que os valores das
        chaves são "web pdf" e "web html"
    """
    d = {}
    for doc_data in document_webpages_data:
        key = "web {}".format(doc_data["format"])
        d[key] = d.get(key, [])
        d[key].append(doc_data)
    return d


def check_document_availability(doc_id, website_url, object_store_url, flags={}, timeout=None):
    """
    Verifica a disponibilidade do documento `doc_id`, verificando a
    disponibilidade de todas as _webpages_ (HTML/PDF/idiomas) e de todos os ativos
    digitais. Também verifica se os ativos digitais e as demais _webpages_ estão
    mencionadas (`@href`/`@src`) no conteúdo do HTML
    """
    t1 = datetime.utcnow()
    LAST_VERSION = -1
    document_manifest = get_document_manifest(doc_id)
    current_version = document_manifest["versions"][LAST_VERSION]
    doc_data = get_document_data_to_generate_uri(current_version)

    document_webpages_data = get_document_webpages_data(doc_id, doc_data)
    doc_data_grouped_by_webpage_type = group_doc_data_by_webpage_type(
        document_webpages_data)

    add_responses(doc_data_grouped_by_webpage_type["web html"], website_url,
                  flags.get("CHECK_WEB_HTML_PAGES", True),
                  timeout=timeout)

    add_responses(doc_data_grouped_by_webpage_type["web pdf"], website_url,
                  flags.get("CHECK_WEB_PDF_PAGES", True),
                  timeout=timeout)

    assets_data, assets_data_grouped_by_id = get_document_assets_data(current_version)
    renditions_data = get_document_renditions_data(current_version)

    add_responses(renditions_data, request=flags.get("CHECK_RENDITIONS", True),
                  timeout=timeout)
    add_responses(assets_data, request=flags.get("CHECK_ASSETS", True),
                  timeout=timeout)

    web_html_availability, web_html_numbers = check_html_webpages_availability(
                doc_data_grouped_by_webpage_type["web html"],
                assets_data_grouped_by_id,
                document_webpages_data,
                object_store_url
            )
    web_pdf_availability, web_pdf_numbers = check_pdf_webpages_availability(
            doc_data_grouped_by_webpage_type["web pdf"])

    renditions_availability, q_unavailable_renditions = check_responses(
                renditions_data
            )
    assets_availability, q_unavailable_assets = check_responses(assets_data)
    summarized = {}
    for name, numbers in (("web html", web_html_numbers), ("web pdf", web_pdf_numbers)):
        if numbers:
            summarized.update({name: numbers})
    summarized.update({
        "renditions": {
            "total": len(renditions_data),
            "total unavailable": q_unavailable_renditions},
        "assets": {
            "total": len(assets_availability),
            "total unavailable": q_unavailable_assets},
        }
    )
    for name, flag_name in (("web html", "CHECK_WEB_HTML_PAGES"),
                       ("web pdf", "CHECK_WEB_PDF_PAGES"),
                       ("renditions", "CHECK_RENDITIONS"),
                       ("assets", "CHECK_DIGITAL_ASSETS"),):
        summarized[name]["requested"] = flags.get(flag_name, True)

    t2 = datetime.utcnow()
    summarized["processing"] = {
        "start": t1, "end": t2, "duration": time_diff(t1, t2)
    }
    return {
        "summary": summarized,
        "detail":
            {
                "web html": web_html_availability,
                "web pdf": web_pdf_availability,
                "renditions": renditions_availability,
                "assets": assets_availability,
            },
    }


def get_status(summary):
    """
    {
      "web html": {
         "total": 1,
         "total unavailable": 0,
         "total incomplete": 1
      },
      "web pdf": {
         "total": 1,
         "total unavailable": 0
      },
      "renditions": {
         "total": 1,
         "total unavailable": 0
      },
      "assets": {
         "total": 6,
         "total unavailable": 0
      },
      "processing": {
         "start": "t0",
         "end": "t3",
         "duration": 5
      }
    }
    """
    _summary = {k: v
                for k, v in summary.items()
                if k in ("web html", "web pdf", "renditions", "assets")
                }
    total = sum([item.get("total", 0) for item in _summary.values()])
    total_u = sum([item.get("total unavailable", 0)
                   for item in _summary.values()])
    total_i = _summary.get("web html", {}).get("total incomplete", 0)
    if total == 0 or total == total_u:
        return "missing"

    if total_u == total_i == 0 and total > 0:
        return "complete"

    return "partial"


def get_pid_v3_list(uri_items, website_url, timeout=DO_REQ_TIMEOUT):
    """
    Retorna o PID v3 de cada um dos itens de `uri_items`,
    acessando o link do padrão:
    `/scielo.php?script=sci_arttext&pid=S0001-37652020000501101&tlng=lang`
    e parseando o URI do redirecionamento, padrão:
    `/j/:acron/a/:id_doc`

    Args:
        uri_items (str list): lista de URI no padrão
            /scielo.php?script=sci_arttext&pid=S0001-37652020000501101&tlng=lang
        website_url (str): URL de site
    Returns:
        str list: lista de PID v3
    """
    pid_v3_list = []
    for uri in uri_items:
        doc_uri = "{}{}".format(website_url, uri)
        doc_id = get_kernel_document_id_from_classic_document_uri(
            doc_uri, timeout=timeout)
        if doc_id:
            pid_v3_list.append(doc_id)
        else:
            Logger.error("Unable to find PID v3 from %s", doc_uri)
    return pid_v3_list


def get_main_website_url(website_url_list, timeout=DO_REQ_TIMEOUT):
    """
    Dentre a lista de URL de website SciELO para testar, verifica qual é a
    do site novo
    """
    for url in website_url_list:
        resp = do_request("{}/about".format(url), timeout=timeout)
        Logger.info(resp)
        Logger.info(eval_response(resp))
        if is_valid_response(resp):
            return url


def check_website_uri_list_deeply(doc_id_list, website_url, object_store_url, dag_info, timeout=None):
    """
    Executa a verificação da disponibilidade profundamente e
    faz o registro do resultado

    Args:
        doc_id_list (str list): lista de PID v3
        website_url (str): URL do site público
        object_store_url (str): URL do _object store_
        dag_info (dict): dados da DAG

    """
    total = len(doc_id_list)
    Logger.info("Check availability of %i documents", total)

    checked_pids = []

    for i, doc_id in enumerate(doc_id_list):
        Logger.info(
            "Check document availability of %s (%i/%i)", doc_id, i, total)
        report = check_document_availability(
            doc_id, website_url, object_store_url, dag_info, timeout=timeout)

        Logger.info("Format table row data")
        row = format_document_availability_result_to_register(
            doc_id, report, dag_info)

        # identifica os PIDs processados
        for name in ("pid_v2_doc", "previous_pid_v2_doc"):
            if row.get(name):
                checked_pids.append(row[name])

        Logger.info("Register document availability checking result")
        add_execution_in_database("doc_deep_checkup", row)
    return checked_pids


def format_document_availability_result_to_register(
        doc_id, doc_checkup_result, dag_info):
    """
    Formata os dados para registrar na tabela `doc_deep_checkup`
    Colunas da tabela:
      id SERIAL PRIMARY KEY,
      dag_run varchar(255),                             -- Identificador de execução da dag `check_website`
      input_file_name varchar(255) NULL,                -- Nome do arquivo de entrada: csv com PIDs v2 ou uri_list
      pid_v3 varchar(23),                               -- scielo-pid-v3 presente no xml
      pid_v2_journal varchar(9),                        -- ISSN ID do periódico
      pid_v2_issue varchar(17),                         -- PID do fascículo
      pid_v2_doc varchar(23),                           -- PID do documento
      previous_pid_v2_doc varchar(23) NULL,             -- previous PID do documento
      status varchar(8),                                -- "complete" or "partial" or "missing"
      detail json,                                      -- Detalhes da verificação profunda
      created_at timestamptz default now()
    Args:
        doc_checkup_result (dict): resultado da verificação
        dag_info (dict): dados da DAG

    Returns:
        dict: dados da coluna
    """
    doc_data = (doc_checkup_result["detail"].get("web html") or
                doc_checkup_result["detail"].get("web pdf"))[0]

    pid_v2 = doc_data.get("pid_v2", "").strip()
    previous_pid_v2 = doc_data.get("previous_pid_v2").strip() if doc_data.get("previous_pid_v2") else None

    data = {}
    data["dag_run"] = dag_info.get("run_id")
    data["input_file_name"] = dag_info.get("input_file_name")
    data["pid_v3"] = doc_id
    data["pid_v2_journal"] = pid_v2[1:10]
    data["pid_v2_issue"] = pid_v2[1:18]
    data["pid_v2_doc"] = pid_v2
    data["previous_pid_v2_doc"] = previous_pid_v2
    data["status"] = get_status(
        doc_checkup_result.get("summary", {})) or "unidentified"
    data["detail"] = json.dumps(doc_checkup_result, default=fixes_for_json)
    return data


def retry_after():
    return (0, 5, 10, 20, 40, 80, 160, 320, 640, )


def format_document_availability_data_to_register(
            webpages_availability,
            assets_availability,
            renditions_availability,
            extra_data,
        ):
    for version in webpages_availability:
        for row in format_document_webpage_availability_to_register(
                version, extra_data):
            add_execution_in_database("availability", row)

    for asset_availability in assets_availability:
        for row in format_document_items_availability_to_register(
                webpages_availability[0],
                asset_availability, extra_data):
            add_execution_in_database("availability", row)

    for rendition_availability in renditions_availability:
        for row in format_document_items_availability_to_register(
                webpages_availability[0],
                rendition_availability, extra_data):
            add_execution_in_database("availability", row)


def group_documents_by_issue_pid_v2(uri_items):
    groups = {}
    for uri in uri_items:
        pid_j, pid_i, pid_d = get_journal_issue_doc_pids(uri)
        groups[pid_i] = groups.get(pid_i, [])
        groups[pid_i].append(uri)
    return groups
