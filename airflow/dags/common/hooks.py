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
from sqlalchemy.exc import OperationalError


Logger = logging.getLogger(__name__)

DEFAULT_HEADER = {
    "Content-Type": "application/json"
}

KERNEL_HOOK_BASE = HttpHook(http_conn_id="kernel_conn", method="GET")

try:
    HTTP_HOOK_RUN_RETRIES = int(Variable.get("HTTP_HOOK_RUN_RETRIES", 5))
except (OperationalError, ValueError):
    HTTP_HOOK_RUN_RETRIES = 5

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
    Logger.info(
        "%s %s - Payload: %s - status_code: %s",
        method, endpoint, json.dumps((data or ""), indent=2), response.status_code
    )
    return response


def kernel_connect(endpoint, method, data=None, headers=DEFAULT_HEADER, timeout=13):
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
    stop=stop_after_attempt(10),
)
def object_store_connect(bytes_data, filepath, bucket_name):
    s3_hook = S3Hook(aws_conn_id="aws_default")
    connection = s3_hook.get_connection("aws_default")
    object_store_bucket_name = get_object_store_upload_bucket(connection, bucket_name)
    object_store_filepath = get_object_store_upload_filepath(connection, filepath)
    s3_hook.load_bytes(
        bytes_data,
        key=object_store_filepath,
        bucket_name=object_store_bucket_name,
        replace=True,
    )
    object_store_public_url = get_object_store_public_url(connection)
    object_store_public_filepath = get_object_store_public_filepath(
        connection,
        bucket_name,
        filepath,
    )
    return "{}/{}".format(
        object_store_public_url.rstrip("/"),
        object_store_public_filepath,
    )


def join_object_store_path(*parts):
    return "/".join(
        str(part).strip("/")
        for part in parts
        if part is not None and str(part).strip("/")
    )


def get_object_store_upload_bucket(connection, bucket_name):
    return connection.extra_dejson.get("upload_bucket") or bucket_name


def get_object_store_upload_filepath(connection, filepath):
    upload_prefix = connection.extra_dejson.get("upload_prefix")
    return join_object_store_path(upload_prefix, filepath)


def get_object_store_public_filepath(connection, bucket_name, filepath):
    public_prefix = connection.extra_dejson.get("public_prefix", bucket_name)
    return join_object_store_path(public_prefix, filepath)


def get_object_store_public_url(connection):
    extra = connection.extra_dejson
    return (
        extra.get("public_url")
        or extra.get("public_host")
        or extra.get("host")
        or extra.get("endpoint_url")
    )


@retry(wait=wait_exponential(), stop=stop_after_attempt(4))
def update_metadata_in_object_store(filepath, metadata, bucket_name):
    s3_hook = S3Hook(aws_conn_id="aws_default")
    connection = s3_hook.get_connection("aws_default")
    object_store_bucket_name = get_object_store_upload_bucket(connection, bucket_name)
    object_store_filepath = get_object_store_upload_filepath(connection, filepath)
    s3_object = s3_hook.get_key(
        key=object_store_filepath,
        bucket_name=object_store_bucket_name,
    )
    s3_object.metadata.update(metadata)
    s3_object.copy_from(
        CopySource={'Bucket': object_store_bucket_name, 'Key': object_store_filepath},
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


@retry(wait=wait_exponential(), stop=stop_after_attempt(10))
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
