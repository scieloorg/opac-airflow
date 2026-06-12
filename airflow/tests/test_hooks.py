from unittest import TestCase
from unittest.mock import Mock, patch

from common.hooks import (
    get_object_store_public_url,
    object_store_connect,
    update_metadata_in_object_store,
)


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

    @patch("common.hooks.S3Hook")
    def test_object_store_connect_accepts_separate_upload_bucket_and_prefix(
        self, MockS3Hook
    ):
        connection = Mock()
        connection.extra_dejson = {
            "host": "https://ny-s3.storage.bunnycdn.com",
            "upload_bucket": "minio",
            "upload_prefix": "documentstore",
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
            key="documentstore/journal/scielo-id/hash.xml",
            bucket_name="minio",
            replace=True,
        )
        self.assertEqual(
            "https://minio.scielo.br/documentstore/journal/scielo-id/hash.xml",
            result,
        )


class TestUpdateMetadataInObjectStore(TestCase):
    @patch("common.hooks.S3Hook")
    def test_update_metadata_uses_separate_upload_bucket_and_prefix(
        self, MockS3Hook
    ):
        connection = Mock()
        connection.extra_dejson = {
            "upload_bucket": "minio",
            "upload_prefix": "documentstore",
        }
        s3_object = Mock()
        s3_object.metadata = {"filename": "previous.xml"}
        s3_hook = MockS3Hook.return_value
        s3_hook.get_connection.return_value = connection
        s3_hook.get_key.return_value = s3_object

        update_metadata_in_object_store(
            "journal/scielo-id/hash.xml",
            {"mimetype": "application/xml"},
            "documentstore",
        )

        s3_hook.get_key.assert_called_once_with(
            key="documentstore/journal/scielo-id/hash.xml",
            bucket_name="minio",
        )
        s3_object.copy_from.assert_called_once_with(
            CopySource={
                'Bucket': "minio",
                'Key': "documentstore/journal/scielo-id/hash.xml",
            },
            Metadata={
                "filename": "previous.xml",
                "mimetype": "application/xml",
            },
            MetadataDirective='REPLACE',
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
