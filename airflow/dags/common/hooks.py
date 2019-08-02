import requests
from tenacity import (
    retry,
    wait_exponential,
    stop_after_attempt,
    retry_if_exception_type,
)
from airflow.hooks.http_hook import HttpHook


@retry(
    wait=wait_exponential(),
    stop=stop_after_attempt(4),
    retry=retry_if_exception_type(
        (requests.ConnectionError,
        requests.Timeout),
    ),
)
def kernel_connect(endpoint, method, timeout=1):
    api_hook = HttpHook(http_conn_id="kernel_conn", method=method)
    response = api_hook.run(endpoint=endpoint, extra_options={"timeout": timeout})
    response.raise_for_status()
    return response
