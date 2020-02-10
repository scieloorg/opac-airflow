import os
import unittest
from unittest.mock import patch

from opac_schema.v1 import models

from .test_sync_kernel_to_website import load_json_fixture
from dags.operations.sync_external_content_to_website_operations import NewsBuilder, PressReleaseBuilder

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


class TestPressReleaseBuilder(unittest.TestCase):
    def setUp(self):
        self.press_release = load_json_fixture("rss-press-release-feed.json")
        self.press_release_objects = patch(
            "operations.sync_external_content_to_website_operations.models.PressRelease.objects"
        )

        self.new_journal = patch("operations.sync_external_content_to_website_operations.models.Journal")

        PressReleaseObjectsMock = self.press_release_objects.start()
        PressReleaseObjectsMock.get.side_effect = models.PressRelease.DoesNotExist

        JournalObjectsMock = self.new_journal.start()
        JournalObjectsMock._id = "J00001"
        self.news_instance = PressReleaseBuilder(self.press_release[0], JournalObjectsMock, "pt_BR")

    def tearDown(self):
        self.press_release_objects.stop()

    def test_press_release_instance_has_id(self):
        self.assertIsNotNone(self.news_instance._id)

    def test_press_release_instance_id_has_32_characters(self):
        self.assertEqual(32, len(self.news_instance._id))

    def test_press_release_instance_has_title(self):
        self.assertEqual("Como os memes da internet conectam diferentes mundos?", self.news_instance.title)

    def test_press_release_instance_has_description(self):
        self.assertEqual("Que se destacou na internet e outras....", self.news_instance.content)

    def test_press_release_instance_has_language(self):
        self.assertEqual("pt_BR", self.news_instance.language)
