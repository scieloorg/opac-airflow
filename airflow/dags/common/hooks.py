import requests
from tenacity import (
    retry,
    wait_exponential,
    stop_after_attempt,
    retry_if_exception_type,
)
from airflow.hooks.http_hook import HttpHook
from airflow.hooks.S3_hook import S3Hook


@retry(
    wait=wait_exponential(),
    stop=stop_after_attempt(4),
    retry=retry_if_exception_type((requests.ConnectionError, requests.Timeout)),
)
def kernel_connect(endpoint, method, data=None, timeout=1):
    api_hook = HttpHook(http_conn_id="kernel_conn", method=method)
    response = api_hook.run(
        endpoint=endpoint, data=data, extra_options={"timeout": timeout}
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
