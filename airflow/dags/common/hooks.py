import logging
import json
import requests
from tenacity import (
    retry,
    wait_exponential,
    stop_after_attempt,
    retry_if_exception_type,
)
from airflow.hooks.http_hook import HttpHook
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.base_hook import BaseHook

from mongoengine import connect


Logger = logging.getLogger(__name__)

DEFAULT_HEADER = {
    "Content-Type": "application/json"
}

KERNEL_HOOK_BASE = HttpHook(http_conn_id="kernel_conn", method="GET")

@retry(
    wait=wait_exponential(),
    stop=stop_after_attempt(4),
    retry=retry_if_exception_type((requests.ConnectionError, requests.Timeout)),
)
def http_hook_run(api_hook, method, endpoint, data=None, headers=DEFAULT_HEADER, timeout=1):
    response = api_hook.run(
        endpoint=endpoint,
        data=data,
        headers=headers,
        extra_options={"timeout": timeout, "check_response": False}
    )
    Logger.debug(
        "%s %s - Payload: %s - status_code: %s",
        method, endpoint, json.dumps((data or ""), indent=2), response.status_code
    )
    return response


def kernel_connect(endpoint, method, data=None, headers=DEFAULT_HEADER, timeout=1):
    api_hook = HttpHook(http_conn_id="kernel_conn", method=method)
    response = http_hook_run(
        api_hook=api_hook,
        method=method,
        endpoint=endpoint,
        data=json.dumps(data) if data is not None else None,
        headers=headers,
        timeout=timeout
    )
    response.raise_for_status()
    return response


@retry(
    wait=wait_exponential(),
    stop=stop_after_attempt(4),
    retry=retry_if_exception_type((requests.ConnectionError, requests.Timeout)),
)
def object_store_connect(bytes_data, filepath, bucket_name):
    s3_hook = S3Hook(aws_conn_id="aws_default")
    s3_hook.load_bytes(bytes_data, key=filepath, bucket_name=bucket_name, replace=True)
    s3_host = s3_hook.get_connection("aws_default").extra_dejson.get("host")
    return "{}/{}/{}".format(s3_host, bucket_name, filepath)


@retry(wait=wait_exponential(), stop=stop_after_attempt(10))
def mongo_connect():
    # TODO: Necessário adicionar um commando para adicionar previamente uma conexão, ver: https://github.com/puckel/docker-airflow/issues/75
    conn = BaseHook.get_connection("opac_conn")

    uri = "mongodb://{creds}{host}{port}/{database}".format(
        creds="{}:{}@".format(conn.login, conn.password) if conn.login else "",
        host=conn.host,
        port="" if conn.port is None else ":{}".format(conn.port),
        database=conn.schema,
    )

    connect(host=uri, **conn.extra_dejson)
