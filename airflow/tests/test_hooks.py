from unittest import TestCase
from unittest.mock import Mock, patch

from common.hooks import get_object_store_public_url, object_store_connect


class TestObjectStoreConnect(TestCase):
    @patch("common.hooks.S3Hook")
    def test_object_store_connect_returns_public_url_when_configured(
        self, MockS3Hook
    ):
        connection = Mock()
        connection.extra_dejson = {
            "host": "https://ny-s3.storage.bunnycdn.com",
            "public_url": "https://minio.scielo.br",
        }
        s3_hook = MockS3Hook.return_value
        s3_hook.get_connection.return_value = connection

        result = object_store_connect(
            b"<xml/>",
            "journal/scielo-id/hash.xml",
            "documentstore",
        )

        s3_hook.load_bytes.assert_called_once_with(
            b"<xml/>",
            key="journal/scielo-id/hash.xml",
            bucket_name="documentstore",
            replace=True,
        )
        self.assertEqual(
            "https://minio.scielo.br/documentstore/journal/scielo-id/hash.xml",
            result,
        )

    @patch("common.hooks.S3Hook")
    def test_object_store_connect_returns_public_host_when_configured(
        self, MockS3Hook
    ):
        connection = Mock()
        connection.extra_dejson = {
            "host": "https://ny-s3.storage.bunnycdn.com",
            "public_host": "https://minio.scielo.br/",
        }
        s3_hook = MockS3Hook.return_value
        s3_hook.get_connection.return_value = connection

        result = object_store_connect(
            b"<xml/>",
            "/journal/scielo-id/hash.xml",
            "documentstore",
        )

        self.assertEqual(
            "https://minio.scielo.br/documentstore/journal/scielo-id/hash.xml",
            result,
        )


class TestGetObjectStorePublicUrl(TestCase):
    def test_get_object_store_public_url_prefers_public_url(self):
        connection = Mock()
        connection.extra_dejson = {
            "endpoint_url": "https://ny-s3.storage.bunnycdn.com",
            "host": "https://ny-s3.storage.bunnycdn.com",
            "public_host": "https://public-host.example.org",
            "public_url": "https://minio.scielo.br",
        }

        self.assertEqual(
            "https://minio.scielo.br",
            get_object_store_public_url(connection),
        )

    def test_get_object_store_public_url_keeps_host_as_legacy_fallback(self):
        connection = Mock()
        connection.extra_dejson = {"host": "https://minio.scielo.br"}

        self.assertEqual(
            "https://minio.scielo.br",
            get_object_store_public_url(connection),
        )

    def test_get_object_store_public_url_accepts_endpoint_url_as_last_fallback(self):
        connection = Mock()
        connection.extra_dejson = {
            "endpoint_url": "https://ny-s3.storage.bunnycdn.com"
        }

        self.assertEqual(
            "https://ny-s3.storage.bunnycdn.com",
            get_object_store_public_url(connection),
        )
