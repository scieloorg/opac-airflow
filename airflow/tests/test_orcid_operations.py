import os
import json
import unittest
from unittest.mock import patch, MagicMock, call

from operations.orcid_operations import (
    post_data,
    register_orcid,
    RetryableError,
    NonRetryableError,
)


class PostDataTests(unittest.TestCase):

    @patch("operations.orcid_operations.requests.post")
    def test_post_data_success(self, mock_post):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        result = post_data(
            url="http://example.com/works",
            data='{"key": "value"}',
            headers={"Content-Type": "application/json"},
        )

        mock_post.assert_called_once()
        self.assertEqual(result, mock_response)

    @patch("operations.orcid_operations.requests.post")
    def test_post_data_connection_error_raises_retryable(self, mock_post):
        import requests as req
        mock_post.side_effect = req.exceptions.ConnectionError("Connection failed")

        with self.assertRaises(RetryableError):
            post_data.__wrapped__(
                url="http://example.com/works",
                data='{"key": "value"}',
            )

    @patch("operations.orcid_operations.requests.post")
    def test_post_data_timeout_raises_retryable(self, mock_post):
        import requests as req
        mock_post.side_effect = req.exceptions.Timeout("Timeout")

        with self.assertRaises(RetryableError):
            post_data.__wrapped__(
                url="http://example.com/works",
                data='{"key": "value"}',
            )

    @patch("operations.orcid_operations.requests.post")
    def test_post_data_invalid_url_raises_non_retryable(self, mock_post):
        import requests as req
        mock_post.side_effect = req.exceptions.InvalidURL("Invalid URL")

        with self.assertRaises(NonRetryableError):
            post_data.__wrapped__(
                url="invalid-url",
                data='{"key": "value"}',
            )

    @patch("operations.orcid_operations.requests.post")
    def test_post_data_4xx_raises_non_retryable(self, mock_post):
        import requests as req
        mock_response = MagicMock()
        mock_response.status_code = 400
        exc = req.HTTPError(response=mock_response)
        mock_response.raise_for_status.side_effect = exc
        mock_post.return_value = mock_response

        with self.assertRaises(NonRetryableError):
            post_data.__wrapped__(
                url="http://example.com/works",
                data='{"key": "value"}',
            )

    @patch("operations.orcid_operations.requests.post")
    def test_post_data_5xx_raises_retryable(self, mock_post):
        import requests as req
        mock_response = MagicMock()
        mock_response.status_code = 500
        exc = req.HTTPError(response=mock_response)
        mock_response.raise_for_status.side_effect = exc
        mock_post.return_value = mock_response

        with self.assertRaises(RetryableError):
            post_data.__wrapped__(
                url="http://example.com/works",
                data='{"key": "value"}',
            )


class RegisterOrcidTests(unittest.TestCase):

    SAMPLE_XML = b"""<?xml version="1.0" encoding="utf-8"?>
    <article article-type="research-article" xml:lang="en">
        <front>
            <journal-meta>
                <journal-title-group>
                    <journal-title>Test Journal</journal-title>
                </journal-title-group>
            </journal-meta>
            <article-meta>
                <article-id pub-id-type="doi">10.1000/test.123</article-id>
                <title-group>
                    <article-title>Test Article Title</article-title>
                </title-group>
                <contrib-group>
                    <contrib contrib-type="author">
                        <contrib-id contrib-id-type="orcid">0000-0001-1234-5678</contrib-id>
                        <name>
                            <surname>Smith</surname>
                            <given-names>John</given-names>
                        </name>
                        <xref ref-type="aff" rid="aff1"/>
                    </contrib>
                </contrib-group>
                <aff id="aff1">
                    <institution content-type="original">University</institution>
                    <email>john@example.com</email>
                </aff>
                <pub-date pub-type="epub">
                    <day>15</day>
                    <month>03</month>
                    <year>2025</year>
                </pub-date>
            </article-meta>
        </front>
    </article>"""

    @patch("operations.orcid_operations.ENABLE_ORCID_PUSH", False)
    @patch("operations.orcid_operations.post_data")
    def test_register_orcid_disabled(self, mock_post_data):
        register_orcid("doc-1", self.SAMPLE_XML)
        mock_post_data.assert_not_called()

    @patch("operations.orcid_operations.ENABLE_ORCID_PUSH", True)
    @patch("operations.orcid_operations.ORCID_PUSH_URL", "")
    @patch("operations.orcid_operations.post_data")
    def test_register_orcid_no_url(self, mock_post_data):
        register_orcid("doc-1", self.SAMPLE_XML)
        mock_post_data.assert_not_called()

    @patch("operations.orcid_operations.ENABLE_ORCID_PUSH", True)
    @patch("operations.orcid_operations.ORCID_PUSH_URL", "https://orcid-push.scielo.org")
    @patch("operations.orcid_operations.post_data")
    @patch("operations.orcid_operations.build_payload")
    def test_register_orcid_posts_payload(self, mock_build, mock_post_data):
        payload = {
            "orcid_id": "0000-0001-1234-5678",
            "author_name": "John Smith",
            "author_email": "john@example.com",
            "work_data": {"title": {"title": {"value": "Test"}}},
        }
        mock_build.return_value = iter([payload])

        register_orcid("doc-1", self.SAMPLE_XML)

        mock_post_data.assert_called_once_with(
            url="https://orcid-push.scielo.org/works",
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )

    @patch("operations.orcid_operations.ENABLE_ORCID_PUSH", True)
    @patch("operations.orcid_operations.ORCID_PUSH_URL", "https://orcid-push.scielo.org/")
    @patch("operations.orcid_operations.post_data")
    @patch("operations.orcid_operations.build_payload")
    def test_register_orcid_url_trailing_slash(self, mock_build, mock_post_data):
        payload = {"orcid_id": "0000-0001-1234-5678"}
        mock_build.return_value = iter([payload])

        register_orcid("doc-1", self.SAMPLE_XML)

        mock_post_data.assert_called_once_with(
            url="https://orcid-push.scielo.org/works",
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )

    @patch("operations.orcid_operations.ENABLE_ORCID_PUSH", True)
    @patch("operations.orcid_operations.ORCID_PUSH_URL", "https://orcid-push.scielo.org")
    @patch("operations.orcid_operations.post_data")
    @patch("operations.orcid_operations.build_payload")
    def test_register_orcid_multiple_authors(self, mock_build, mock_post_data):
        payloads = [
            {"orcid_id": "0000-0001-1111-1111", "author_name": "Author One"},
            {"orcid_id": "0000-0001-2222-2222", "author_name": "Author Two"},
        ]
        mock_build.return_value = iter(payloads)

        register_orcid("doc-1", self.SAMPLE_XML)

        self.assertEqual(mock_post_data.call_count, 2)

    @patch("operations.orcid_operations.ENABLE_ORCID_PUSH", True)
    @patch("operations.orcid_operations.ORCID_PUSH_URL", "https://orcid-push.scielo.org")
    @patch("operations.orcid_operations.post_data")
    @patch("operations.orcid_operations.build_payload")
    def test_register_orcid_no_payloads(self, mock_build, mock_post_data):
        mock_build.return_value = None

        register_orcid("doc-1", self.SAMPLE_XML)

        mock_post_data.assert_not_called()

    @patch("operations.orcid_operations.ENABLE_ORCID_PUSH", True)
    @patch("operations.orcid_operations.ORCID_PUSH_URL", "https://orcid-push.scielo.org")
    @patch("operations.orcid_operations.post_data")
    @patch("operations.orcid_operations.build_payload")
    def test_register_orcid_post_failure_does_not_raise(self, mock_build, mock_post_data):
        payload = {"orcid_id": "0000-0001-1234-5678"}
        mock_build.return_value = iter([payload])
        mock_post_data.side_effect = RetryableError("Connection failed")

        # Should not raise - errors are logged
        register_orcid("doc-1", self.SAMPLE_XML)

    @patch("operations.orcid_operations.ENABLE_ORCID_PUSH", True)
    @patch("operations.orcid_operations.ORCID_PUSH_URL", "https://orcid-push.scielo.org")
    @patch("operations.orcid_operations.post_data")
    def test_register_orcid_invalid_xml(self, mock_post_data):
        register_orcid("doc-1", b"<invalid xml")
        mock_post_data.assert_not_called()
