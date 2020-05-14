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
from airflow.hooks.postgres_hook import PostgresHook
from airflow.exceptions import AirflowException
from airflow.models import Variable
from psycopg2 import ProgrammingError

from mongoengine import connect


Logger = logging.getLogger(__name__)

DEFAULT_HEADER = {
    "Content-Type": "application/json"
}

KERNEL_HOOK_BASE = HttpHook(http_conn_id="kernel_conn", method="GET")
# HTTP_HOOK_RUN_RETRIES = Variable.get("HTTP_HOOK_RUN_RETRIES", 5)
HTTP_HOOK_RUN_RETRIES = """
    {% if var.value.HTTP_HOOK_RUN_RETRIES is defined %}
        {{ var.value.HTTP_HOOK_RUN_RETRIES }}
    {% else %}
        5
    {% endif %}
"""

@retry(
    wait=wait_exponential(),
    stop=stop_after_attempt(HTTP_HOOK_RUN_RETRIES),
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


def kernel_connect(endpoint, method, data=None, headers=DEFAULT_HEADER, timeout=10):
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


@retry(wait=wait_exponential(), stop=stop_after_attempt(4))
def update_metadata_in_object_store(filepath, metadata, bucket_name):
    s3_hook = S3Hook(aws_conn_id="aws_default")
    s3_object = s3_hook.get_key(key=filepath, bucket_name=bucket_name)
    s3_object.metadata.update(metadata)
    s3_object.copy_from(
        CopySource={'Bucket': bucket_name, 'Key': filepath},
        Metadata=s3_object.metadata,
        MetadataDirective='REPLACE'
    )


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


def add_execution_in_database(
    table, data={}, connection_id="postgres_report_connection"
):
    """Registra informações em um banco PostgreSQL de forma dinâmica."""

    data = dict(data)

    if data is None or len(data.keys()) == 0:
        logging.info(
            "Cannot insert `empty data` into the database. Please verify your data attributes."
        )
        return

    hook = PostgresHook(postgres_conn_id=connection_id)

    try:
        hook.get_conn()
    except AirflowException:
        logging.info("Cannot insert data. Connection '%s' is not configured.", connection_id)
        return

    if data.get("payload"):
        data["payload"] = json.dumps(data["payload"])

    columns = list(data.keys())
    values = list(data.values())

    try:
        hook.insert_rows(table, [values], target_fields=columns)
    except (AirflowException, ProgrammingError) as exc:
        logging.error(exc)
    else:
        logging.info("Registering `%s` into '%s' table.", data, table)
