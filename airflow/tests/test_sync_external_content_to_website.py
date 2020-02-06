import os
import json
import unittest
from unittest.mock import Mock, patch

from opac_schema.v1 import models

from .test_sync_kernel_to_website import load_json_fixture
from dags.operations.sync_external_content_to_website_operations import NewsBuilder

FIXTURES_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures")


class TestNewsBuilder(unittest.TestCase):
    def setUp(self):
        self.news = load_json_fixture("rss-news-feed.json")
        self.news_objects = patch(
            "operations.sync_external_content_to_website_operations.models.News.objects"
        )

        NewsObjectsMock = self.news_objects.start()
        NewsObjectsMock.get.side_effect = models.News.DoesNotExist

        self.news_instance = NewsBuilder(self.news[0], "en")

    def tearDown(self):
        self.news_objects.stop()

    def test_news_instance_has_id(self):
        self.assertIsNotNone(self.news_instance._id)

    def test_news_instance_id_has_32_characters(self):
        self.assertEqual(32, len(self.news_instance._id))

    def test_news_instance_has_title(self):
        self.assertEqual("Random title", self.news_instance.title)

    def test_news_instance_has_description(self):
        self.assertEqual("Summary..", self.news_instance.description)

    def test_news_instance_has_image_url(self):
        self.assertEqual(
            "https://blog.scielo.org/wp-content/image.jpg",
            self.news_instance.image_url,
        )

    def test_news_instance_has_language(self):
        self.assertEqual("en", self.news_instance.language)


