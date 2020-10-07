import asyncio
from datetime import datetime
import logging

import aiohttp
import requests
import certifi


TIMEOUT_FOR_PAR_REQS = 60*5
TIMEOUT_FOR_SEQ_REQS = 60
Logger = logging.getLogger(__name__)


class InvalidClientResponse:
    def __init__(self):
        self.status = None


async def fetch(uri, session, body=False):
    """
    asynchronous `get` or `head`
    """
    start = datetime.utcnow()
    do_request = session.head if body is False else session.get

    try:
        async with do_request(uri) as response:
            text = await response.text()
    except (
            aiohttp.ClientResponseError,
            aiohttp.ClientError,
            AttributeError) as e:
        response = InvalidClientResponse()
        Logger.exception(e)
        text = None
    finally:
        # acrescenta novos atributos para o objeto ClientResponse
        response.uri = uri
        response.end_time = datetime.utcnow()
        response.start_time = start
        response.status_code = response.status
        response.text = text

        Logger.info("Requested %s: %s", uri, response.status_code)
        return response


async def fetch_many(uri_items, body=False, timeout=TIMEOUT_FOR_PAR_REQS):
    # https://docs.aiohttp.org/en/stable/client_quickstart.html#timeouts
    client_timeout = aiohttp.ClientTimeout(total=timeout or TIMEOUT_FOR_PAR_REQS)
    async with aiohttp.ClientSession(timeout=client_timeout) as session:
        responses = await asyncio.gather(*[
            fetch(url, session, body)
            for url in uri_items
        ])
        return responses


def parallel_requests(uri_items, body=False, timeout=TIMEOUT_FOR_PAR_REQS):
    """
    performs parallel requests
    """
    return asyncio.run(fetch_many(uri_items, body, timeout=timeout))


def seq_requests(uri_items, body=False, timeout=TIMEOUT_FOR_SEQ_REQS):
    """
    performs sequential requests
    """
    do_request = requests.head if body is False else requests.get
    resps = []
    for u in uri_items:
        resp = do_request(u, timeout=timeout)
        resps.append(resp)
    return resps


def compare(lista, body=True):
    print("")
    print("Body: {}".format(body))
    print("Requests: ")
    print("\n".join(lista))

    resps = {}
    for name, func in (("Sequential", seq_requests), ("Parallel", parallel_requests)):
        print(name)
        t1 = datetime.utcnow()
        items = func(lista, body)
        t2 = datetime.utcnow()
        resps[name] = {
            "duration (ms)": (t2 - t1).microseconds,
            "status": [r.status_code for r in items],
            "len": [len(r.text) for r in items],
        }
    print(resps)
    print(resps["Parallel"]["duration (ms)"] / resps["Sequential"]["duration (ms)"])


def main():
    lista3 = [
        'https://www.scielo.br/scielo.php?script=sci_arttext&pid=S0102-67202020000200304&lng=en&nrm=iso&tlng=en',
        'https://www.scielo.br/scielo.php?script=sci_arttext&pid=S0102-67202020000200305&lng=en&nrm=iso',
        'https://www.scielo.br/scielo.php?pid=S0100-39842020000200001&script=sci_arttext&tlng=pt',
    ]

    compare(lista3, True)
    compare(lista3, False)
    compare([lista3[0]], True)
    compare([lista3[0]], False)


if __name__ == "__main__":
    main()
