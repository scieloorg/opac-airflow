import os
import json
import logging

import requests
from lxml import etree as et
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from packtools.sps.formats.orcid.payload_builder import build_payload


logger = logging.getLogger(__name__)

ENABLE_ORCID_PUSH = os.environ.get("ENABLE_ORCID_PUSH", "false").lower() == "true"
ORCID_PUSH_URL = os.environ.get("ORCID_PUSH_URL", "")


class RetryableError(Exception):
    ...


class NonRetryableError(Exception):
    ...


@retry(
    retry=retry_if_exception_type(RetryableError),
    wait=wait_exponential(multiplier=1, min=1, max=5),
    stop=stop_after_attempt(5),
)
def post_data(url, data=None, headers=None, timeout=2, verify=True):
    """
    Post data with HTTP.

    Retry: Wait 2^x * 1 second between each retry starting with 1 second,
           then up to 5 seconds.
    """
    try:
        response = requests.post(
            url,
            data=data,
            headers=headers,
            timeout=timeout,
            verify=verify,
        )
    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
        logger.error(
            "Error posting data (timeout=%s): %s, retry..., error: %s",
            timeout, url, exc,
        )
        raise RetryableError(exc) from exc
    except (
        requests.exceptions.InvalidSchema,
        requests.exceptions.MissingSchema,
        requests.exceptions.InvalidURL,
    ) as exc:
        raise NonRetryableError(exc) from exc

    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        if 400 <= exc.response.status_code < 500:
            raise NonRetryableError(exc) from exc
        elif 500 <= exc.response.status_code < 600:
            logger.error(
                "Error posting data: %s, retry..., error: %s", url, exc,
            )
            raise RetryableError(exc) from exc
        else:
            raise

    return response


def register_orcid(document_id, document_xml):
    """Registra publicação no serviço de ORCID push.

    Utiliza o XML do documento para gerar os payloads de cada autor com ORCID
    e envia uma requisição POST para o serviço de ORCID push.

    Args:
        document_id (str): Identificador do documento.
        document_xml (bytes): Conteúdo XML do documento.
    """
    if not ENABLE_ORCID_PUSH:
        return

    if not ORCID_PUSH_URL:
        logger.warning(
            "ORCID push is enabled but ORCID_PUSH_URL is not configured. "
            "Skipping ORCID registration for document '%s'.",
            document_id,
        )
        return

    try:
        xml_tree = et.XML(document_xml)
    except Exception as exc:
        logger.error(
            "Could not parse XML for ORCID registration of document '%s': %s",
            document_id, exc,
        )
        return

    url = ORCID_PUSH_URL.rstrip("/") + "/works"
    headers = {"Content-Type": "application/json"}

    payloads = build_payload(xml_tree)
    if payloads is None:
        logger.info(
            "No ORCID payloads generated for document '%s'. "
            "Missing required fields.",
            document_id,
        )
        return

    for payload in payloads:
        try:
            post_data(
                url=url,
                data=json.dumps(payload),
                headers=headers,
            )
            logger.info(
                "ORCID registration successful for document '%s', "
                "author ORCID '%s'.",
                document_id, payload.get("orcid_id"),
            )
        except (RetryableError, NonRetryableError) as exc:
            logger.error(
                "Could not register ORCID for document '%s', "
                "author ORCID '%s': %s",
                document_id, payload.get("orcid_id"), exc,
            )
        except Exception as exc:
            logger.error(
                "Unexpected error during ORCID registration for document '%s', "
                "author ORCID '%s': %s",
                document_id, payload.get("orcid_id"), exc,
            )
