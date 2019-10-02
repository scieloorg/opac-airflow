import os
import unittest
from unittest.mock import patch, MagicMock
import json

from airflow import DAG

from kernel_changes import JournalFactory
from operations.kernel_changes_operations import (
    ArticleFactory,
    try_register_documents,
    ArticleRenditionFactory,
    try_register_documents_renditions,
)
from opac_schema.v1 import models


FIXTURES_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures")


def load_json_fixture(filename):
    with open(os.path.join(FIXTURES_PATH, filename)) as f:
        return json.load(f)


class JournalFactoryTests(unittest.TestCase):
    def setUp(self):
        self.journal_data = load_json_fixture("kernel-journals-1678-4464.json")
        self.journal = JournalFactory(self.journal_data)

    def test_has_method_save(self):
        self.assertTrue(hasattr(self.journal, "save"))

    def test_attribute_mongodb_id(self):
        self.assertEqual(self.journal._id, "1678-4464")

    def test_attribute_jid(self):
        self.assertEqual(self.journal.jid, "1678-4464")

    def test_attribute_title(self):
        self.assertEqual(self.journal.title, "Cadernos de Saúde Pública")

    def test_attribute_title_iso(self):
        self.assertEqual(self.journal.title_iso, "Cad. saúde pública")

    def test_attribute_short_title(self):
        self.assertEqual(self.journal.short_title, "Cad. Saúde Pública")

    def test_attribute_acronym(self):
        self.assertEqual(self.journal.acronym, "csp")

    def test_attribute_scielo_issn(self):
        self.assertEqual(self.journal.scielo_issn, "0102-311X")

    def test_attribute_print_issn(self):
        self.assertEqual(self.journal.print_issn, "0102-311X")

    def test_attribute_eletronic_issn(self):
        self.assertEqual(self.journal.eletronic_issn, "1678-4464")

    def test_attribute_subject_categories(self):
        self.assertEqual(self.journal.subject_categories, ["Health Policy & Services"])

    @unittest.skip("not implemented")
    def test_attribute_metrics(self):
        pass

    def test_attribute_issue_count(self):
        self.assertEqual(self.journal.issue_count, 300)

    @unittest.skip("not implemented")
    def test_attribute_mission(self):
        pass

    def test_attribute_study_areas(self):
        self.assertEqual(self.journal.study_areas, ["HEALTH SCIENCES"])

    def test_attribute_sponsors(self):
        self.assertEqual(
            self.journal.sponsors,
            ["CNPq - Conselho Nacional de Desenvolvimento Científico e Tecnológico "],
        )

    def test_attribute_editor_email(self):
        self.assertEqual(self.journal.editor_email, "cadernos@ensp.fiocruz.br")

    def test_attribute_online_submission_url(self):
        self.assertEqual(
            self.journal.online_submission_url,
            "http://cadernos.ensp.fiocruz.br/csp/index.php",
        )

    def test_attribute_logo_url(self):
        self.assertEqual(
            self.journal.logo_url, "http://cadernos.ensp.fiocruz.br/csp/logo.jpeg"
        )

    def test_attribute_current_status(self):
        self.assertEqual(self.journal.current_status, "current")

    def test_attribute_created(self):
        self.assertEqual(self.journal.created, "1999-07-02T00:00:00.000000Z")

    def test_attribute_updated(self):
        self.assertEqual(self.journal.updated, "2019-07-19T20:33:17.102106Z")


class ArticleFactoryTests(unittest.TestCase):
    def setUp(self):
        self.article_objects = patch(
            "operations.kernel_changes_operations.models.Article.objects"
        )
        self.issue_objects = patch(
            "operations.kernel_changes_operations.models.Issue.objects"
        )
        ArticleObjectsMock = self.article_objects.start()
        self.issue_objects.start()

        ArticleObjectsMock.get.side_effect = models.Article.DoesNotExist

        self.document_front = load_json_fixture(
            "kernel-document-front-s1518-8787.2019053000621.json"
        )
        self.document = ArticleFactory(
            "67TH7T7CyPPmgtVrGXhWXVs", self.document_front, "issue-1", "1", ""
        )

    def tearDown(self):
        self.article_objects.stop()
        self.issue_objects.stop()

    def test_has_method_save(self):
        self.assertTrue(hasattr(self.document, "save"))

    def test_has_title_attribute(self):
        self.assertTrue(hasattr(self.document, "title"))

    def test_has_section_attribute(self):
        self.assertTrue(hasattr(self.document, "section"))

    def test_has_abstract_attribute(self):
        self.assertTrue(hasattr(self.document, "abstract"))

    def test_has_identification_attributes(self):
        self.assertTrue(hasattr(self.document, "_id"))
        self.assertTrue(hasattr(self.document, "aid"))
        self.assertTrue(hasattr(self.document, "pid"))
        self.assertTrue(hasattr(self.document, "doi"))

        self.assertEqual("67TH7T7CyPPmgtVrGXhWXVs", self.document._id)
        self.assertEqual("67TH7T7CyPPmgtVrGXhWXVs", self.document.aid)
        self.assertEqual("10.11606/S1518-8787.2019053000621", self.document.doi)

    def test_has_authors_attribute(self):
        self.assertTrue(hasattr(self.document, "authors"))

    def test_has_translated_titles_attribute(self):
        self.assertTrue(hasattr(self.document, "translated_titles"))
        self.assertEqual(1, len(self.document.translated_titles))

    def test_has_trans_sections_attribute(self):
        self.assertTrue(hasattr(self.document, "trans_sections"))
        self.assertEqual(2, len(self.document.trans_sections))

    def test_has_abstracts_attribute(self):
        self.assertTrue(hasattr(self.document, "abstracts"))
        self.assertEqual(2, len(self.document.abstracts))

    def test_has_keywords_attribute(self):
        self.assertTrue(hasattr(self.document, "keywords"))
        self.assertEqual(2, len(self.document.keywords))

    def test_has_abstract_languages_attribute(self):
        self.assertTrue(hasattr(self.document, "abstract_languages"))
        self.assertEqual(2, len(self.document.abstract_languages))

    def test_has_original_language_attribute(self):
        self.assertTrue(hasattr(self.document, "original_language"))
        self.assertEqual("en", self.document.original_language)

    def test_has_publication_date_attribute(self):
        self.assertTrue(hasattr(self.document, "publication_date"))
        self.assertEqual("31 01 2019", self.document.publication_date)

    def test_has_type_attribute(self):
        self.assertTrue(hasattr(self.document, "type"))
        self.assertEqual("research-article", self.document.type)

    def test_has_elocation_attribute(self):
        self.assertTrue(hasattr(self.document, "elocation"))

    def test_has_fpage_attribute(self):
        self.assertTrue(hasattr(self.document, "fpage"))

    def test_has_lpage_attribute(self):
        self.assertTrue(hasattr(self.document, "lpage"))

    def test_has_issue_attribute(self):
        self.assertTrue(hasattr(self.document, "issue"))

    def test_has_journal_attribute(self):
        self.assertTrue(hasattr(self.document, "journal"))

    def test_has_order_attribute(self):
        self.assertTrue(hasattr(self.document, "order"))
        self.assertEqual(1, self.document.order)

    def test_has_xml_attribute(self):
        self.assertTrue(hasattr(self.document, "xml"))

    def test_has_htmls_attribute(self):
        self.assertTrue(hasattr(self.document, "htmls"))

    def test_htmls_attibutes_should_be_populated_with_documents_languages(self):
        self.assertEqual([{"lang": "en"}, {"lang": "pt"}], self.document.htmls)


class RegisterDocumentTests(unittest.TestCase):
    def setUp(self):
        self.documents = ["67TH7T7CyPPmgtVrGXhWXVs"]
        self.document_front = load_json_fixture(
            "kernel-document-front-s1518-8787.2019053000621.json"
        )

        mk_hooks = patch("operations.kernel_changes_operations.hooks")
        self.mk_hooks = mk_hooks.start()

    def tearDown(self):
        self.mk_hooks.stop()

    def test_try_register_documents_call_save_methods_from_article_instance(self):
        article_factory_mock = MagicMock()
        article_instance_mock = MagicMock()
        article_factory_mock.return_value = article_instance_mock

        try_register_documents(
            documents=self.documents,
            get_relation_data=lambda document_id: (
                "issue-1",
                {"id": "67TH7T7CyPPmgtVrGXhWXVs", "order": "01"},
            ),
            fetch_document_front=lambda document_id: self.document_front,
            article_factory=article_factory_mock,
        )

        article_instance_mock.save.assert_called_once()

    def test_try_register_documents_call_fetch_document_front_once(self):
        fetch_document_front_mock = MagicMock()
        article_factory_mock = MagicMock()

        try_register_documents(
            documents=self.documents,
            get_relation_data=lambda _: ("", {}),
            fetch_document_front=fetch_document_front_mock,
            article_factory=article_factory_mock,
        )

        fetch_document_front_mock.assert_called_once_with("67TH7T7CyPPmgtVrGXhWXVs")

    def test_try_register_documents_call_article_factory_once(self):
        article_factory_mock = MagicMock()
        self.mk_hooks.KERNEL_HOOK_BASE.run.side_effect = [
            MagicMock(url="http://kernel_url/")
        ]

        try_register_documents(
            documents=self.documents,
            get_relation_data=lambda _: (
                "issue-1",
                {"id": "67TH7T7CyPPmgtVrGXhWXVs", "order": "01"},
            ),
            fetch_document_front=lambda _: self.document_front,
            article_factory=article_factory_mock,
        )

        article_factory_mock.assert_called_once_with(
            "67TH7T7CyPPmgtVrGXhWXVs",
            self.document_front,
            "issue-1",
            "01",
            "http://kernel_url/documents/67TH7T7CyPPmgtVrGXhWXVs",
        )

    def test_try_register_document_should_be_orphan_when_issue_was_not_found(self):
        article_factory_mock = MagicMock()

        orphans = try_register_documents(
            documents=self.documents,
            get_relation_data=lambda document_id: (),
            fetch_document_front=lambda document_id: self.document_front,
            article_factory=article_factory_mock,
        )

        self.assertEqual(1, len(orphans))
        self.assertEqual(["67TH7T7CyPPmgtVrGXhWXVs"], orphans)


class ArticleRenditionFactoryTests(unittest.TestCase):
    def setUp(self):
        self.article_objects = patch(
            "operations.kernel_changes_operations.models.Article.objects"
        )

        ArticleObjectsMock = self.article_objects.start()
        ArticleObjectsMock.get.side_effect = MagicMock()

        self.document_front = load_json_fixture(
            "kernel-document-front-s1518-8787.2019053000621.json"
        )
        self.article = ArticleRenditionFactory(
            "67TH7T7CyPPmgtVrGXhWXVs",
            [
                {
                    "filename": "filename.pdf",
                    "url": "//object-storage/file.pdf",
                    "mimetype": "application/pdf",
                    "lang": "en",
                    "size_bytes": 1,
                }
            ],
        )

    def tearDown(self):
        self.article_objects.stop()

    def test_pdfs_attr_should_be_populated_with_rendition_pdf_data(self):
        self.assertEqual(
            [{"lang": "en", "url": "//object-storage/file.pdf", "type": "pdf"}],
            self.article.pdfs,
        )


class RegisterDocumentRenditionsTest(unittest.TestCase):
    def setUp(self):
        self.documents = ["67TH7T7CyPPmgtVrGXhWXVs"]
        self.document_front = load_json_fixture(
            "kernel-document-front-s1518-8787.2019053000621.json"
        )

        mk_hooks = patch("operations.kernel_changes_operations.hooks")
        self.mk_hooks = mk_hooks.start()

        self.renditions = [
            {
                "filename": "filename.pdf",
                "url": "//object-storage/file.pdf",
                "mimetype": "application/pdf",
                "lang": "en",
                "size_bytes": 1,
            }
        ]

    def tearDown(self):
        self.mk_hooks.stop()

    def test_try_register_documents_renditions_call_save_methods_from_article_instance(
        self
    ):
        article_rendition_factory_mock = MagicMock()
        article_instance_mock = MagicMock()
        article_rendition_factory_mock.return_value = article_instance_mock

        orphans = try_register_documents_renditions(
            documents=self.documents,
            get_rendition_data=lambda document_id: self.renditions,
            article_rendition_factory=article_rendition_factory_mock,
        )

        article_instance_mock.save.assert_called()

        self.assertEqual([], orphans)

    def test_has_orphans_when_try_register_an_orphan_rendition(self):
        article_rendition_factory_mock = MagicMock()
        article_rendition_factory_mock.side_effect = [models.Article.DoesNotExist]

        orphans = try_register_documents_renditions(
            documents=self.documents,
            get_rendition_data=lambda document_id: self.renditions,
            article_rendition_factory=article_rendition_factory_mock,
        )

        self.assertEqual(self.documents, orphans)
