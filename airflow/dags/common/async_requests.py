import asyncio
import aiohttp
from datetime import datetime
import requests


async def fetch(uri, session, head=False):
    """
    asynchronous `get` or `head`
    """
    start = datetime.utcnow()
    do_request = session.head if head else session.get
    async with do_request(uri) as response:
        await response.text()
        response.end_time = datetime.utcnow()
        response.start_time = start
        response.status_code = response.status
        return response


async def fetch_many(loop, uri_items, head=False):
    """
    many asynchronous get requests, gathered
    """
    async with aiohttp.ClientSession() as session:
        tasks = [loop.create_task(fetch(uri, session, head))
                 for uri in uri_items]
        return await asyncio.gather(*tasks)


def parallel_requests(uri_items, head=False):
    """
    performs parallel requests
    """
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(fetch_many(loop, uri_items, head))


def seq_requests(uri_items, head=False):
    """
    performs sequential requests
    """
    do_request = requests.head if head else requests.get
    resps = []
    for u in uri_items:
        resp = do_request(u)
        resps.append(resp)
    return resps


if __name__ == "__main__":
    lista = [
        'https://www.scielo.br/scielo.php?script=sci_arttext&pid=S0102-67202020000200304&lng=en&nrm=iso&tlng=en',
        'https://www.scielo.br/scielo.php?script=sci_arttext&pid=S0102-67202020000200305&lng=en&nrm=iso',
        'https://www.scielo.br/scielo.php?pid=S0100-39842020000200001&script=sci_arttext&tlng=pt',
    ]
    t1 = datetime.utcnow()
    seq_requests(lista, True)
    t2 = datetime.utcnow()

    t3 = datetime.utcnow()
    parallel_requests(lista, True)
    t4 = datetime.utcnow()

    print((t2 - t1).microseconds)
    print((t4 - t3).microseconds)
