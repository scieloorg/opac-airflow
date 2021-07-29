import os
import unittest
from unittest.mock import patch, MagicMock
import json

from airflow import DAG

from sync_kernel_to_website import (
    JournalFactory, IssueFactory,
    _get_relation_data_from_kernel_bundle,
)
from operations.sync_kernel_to_website_operations import (
    ArticleFactory,
    try_register_documents,
    ArticleRenditionFactory,
    try_register_documents_renditions,
    _get_bundle_pub_year,
    KernelFrontHasNoPubYearError,
    _get_bundle_id,
    _unpublish_repeated_documents,
)
from opac_schema.v1 import models
from operations.exceptions import InvalidOrderValueError


FIXTURES_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures")


def load_json_fixture(filename):
    with open(os.path.join(FIXTURES_PATH, filename)) as f:
        return json.load(f)


class MockArticle:

    def __init__(self, _id, pid, aop_pid, scielo_pids):
        self._id = _id
        self.pid = pid
        self.aop_pid = aop_pid
        self.scielo_pids = scielo_pids

    def save(self):
        pass


class JournalFactoryTests(unittest.TestCase):
    def setUp(self):
        self.journal_objects = patch("sync_kernel_to_website.models.Journal.objects")
        JournalObjectsMock = self.journal_objects.start()
        JournalObjectsMock.get.side_effect = models.Journal.DoesNotExist
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


class JournalFactoryExistsInWebsiteTests(unittest.TestCase):
    def setUp(self):
        self.journal_objects = patch(
            "operations.sync_kernel_to_website_operations.models.Journal.objects"
        )
        MockJournal = MagicMock(spec=models.Journal)
        MockJournal.logo_url = "/media/images/glogo.gif"
        JournalObjectsMock = self.journal_objects.start()
        JournalObjectsMock.get.return_value = MockJournal
        self.journal_data = load_json_fixture("kernel-journals-1678-4464.json")
        self.journal = JournalFactory(self.journal_data)

    def test_preserves_logo_if_already_set(self):
        self.assertEqual(self.journal.logo_url, "/media/images/glogo.gif")


class IssueFactoryTests(unittest.TestCase):
    def setUp(self):
        self.mongo_connect_mock = patch(
            "sync_kernel_to_website.mongo_connect"
        )
        self.mongo_connect_mock.start()
        self.journal_objects = patch(
            "sync_kernel_to_website.models.Journal.objects"
        )
        self.MockJournal = MagicMock(spec=models.Journal)
        JournalObjectsMock = self.journal_objects.start()
        JournalObjectsMock.get.return_value = self.MockJournal
        self.issue_objects = patch("sync_kernel_to_website.models.Issue.objects")
        IssueObjectsMock = self.issue_objects.start()
        IssueObjectsMock.get.side_effect = models.Issue.DoesNotExist

        self.issue_data = load_json_fixture("kernel-issues-0001-3714-1998-v29-n3.json")
        self.issue = IssueFactory(self.issue_data, "0001-3714", "12345")

    def tearDown(self):
        self.mongo_connect_mock.stop()
        self.journal_objects.stop()
        self.issue_objects.stop()

    def test_has_method_save(self):
        self.assertTrue(hasattr(self.issue, "save"))

    def test_attribute_mongodb_id(self):
        self.assertEqual(self.issue._id, "0001-3714-1998-v29-n3")

    def test_attribute_journal(self):
        self.assertEqual(self.issue.journal, self.MockJournal)

    def test_attribute_spe_text(self):
        self.assertEqual(self.issue.spe_text, "")

    def test_attribute_start_month(self):
        self.assertEqual(self.issue.start_month, 9)

    def test_attribute_end_month(self):
        self.assertEqual(self.issue.end_month, 9)

    def test_attribute_year(self):
        self.assertEqual(self.issue.year, "1998")

    def test_attribute_number(self):
        self.assertEqual(self.issue.number, "3")

    def test_attribute_volume(self):
        self.assertEqual(self.issue.volume, "29")

    def test_attribute_order(self):
        self.assertEqual(self.issue.order, "12345")

    def test_attribute_pid(self):
        self.assertEqual(self.issue.pid, "0001-371419980003")

    def test_attribute_label(self):
        self.assertEqual(self.issue.label, "v29n3")

    def test_attribute_suppl_text(self):
        self.assertIsNone(self.issue.suppl_text)

    def test_attribute_type(self):
        self.assertEqual(self.issue.type, "regular")

    def test_attribute_created(self):
        self.assertEqual(self.issue.created, "1998-09-01T00:00:00.000000Z")

    def test_attribute_updated(self):
        self.assertEqual(self.issue.updated, "2020-04-28T20:16:24.459467Z")

    def test_attribute_is_public(self):
        self.assertTrue(self.issue.is_public)


class IssueFactoryVolSupplTests(unittest.TestCase):
    def setUp(self):
        self.mongo_connect_mock = patch(
            "sync_kernel_to_website.mongo_connect"
        )
        self.mongo_connect_mock.start()
        self.journal_objects = patch(
            "sync_kernel_to_website.models.Journal.objects"
        )
        self.MockJournal = MagicMock(spec=models.Journal)
        JournalObjectsMock = self.journal_objects.start()
        JournalObjectsMock.get.return_value = self.MockJournal
        self.issue_objects = patch("sync_kernel_to_website.models.Issue.objects")
        IssueObjectsMock = self.issue_objects.start()
        IssueObjectsMock.get.side_effect = models.Issue.DoesNotExist

        self.issue_data = load_json_fixture("kernel-issues-0001-3714-1998-v29-s0.json")
        self.issue = IssueFactory(self.issue_data, "0001-3714", "12345")

    def tearDown(self):
        self.mongo_connect_mock.stop()
        self.journal_objects.stop()
        self.issue_objects.stop()

    def test_attribute_number(self):
        self.assertEqual(self.issue.number, None)

    def test_attribute_volume(self):
        self.assertEqual(self.issue.volume, "29")

    def test_attribute_label(self):
        self.assertEqual(self.issue.label, "v29s0")

    def test_attribute_suppl_text(self):
        self.assertEqual(self.issue.suppl_text, "0")

    def test_attribute_type(self):
        self.assertEqual(self.issue.type, "supplement")


class IssueFactoryNumSupplTests(unittest.TestCase):
    def setUp(self):
        self.mongo_connect_mock = patch(
            "sync_kernel_to_website.mongo_connect"
        )
        self.mongo_connect_mock.start()
        self.journal_objects = patch(
            "sync_kernel_to_website.models.Journal.objects"
        )
        self.MockJournal = MagicMock(spec=models.Journal)
        JournalObjectsMock = self.journal_objects.start()
        JournalObjectsMock.get.return_value = self.MockJournal
        self.issue_objects = patch("sync_kernel_to_website.models.Issue.objects")
        IssueObjectsMock = self.issue_objects.start()
        IssueObjectsMock.get.side_effect = models.Issue.DoesNotExist

        self.issue_data = load_json_fixture("kernel-issues-0001-3714-1998-n3-s0.json")
        self.issue = IssueFactory(self.issue_data, "0001-3714", "12345")

    def test_attribute_number(self):
        self.assertEqual(self.issue.number, "3")

    def test_attribute_volume(self):
        # idealmente volume deveria ser None
        self.assertEqual(self.issue.volume, '')

    def test_attribute_label(self):
        self.assertEqual(self.issue.label, "n3s0")

    def test_attribute_suppl_text(self):
        self.assertEqual(self.issue.suppl_text, "0")

    def test_attribute_type(self):
        self.assertEqual(self.issue.type, "supplement")


class IssueFactoryVolTests(unittest.TestCase):
    def setUp(self):
        self.mongo_connect_mock = patch(
            "sync_kernel_to_website.mongo_connect"
        )
        self.mongo_connect_mock.start()
        self.journal_objects = patch(
            "sync_kernel_to_website.models.Journal.objects"
        )
        self.MockJournal = MagicMock(spec=models.Journal)
        JournalObjectsMock = self.journal_objects.start()
        JournalObjectsMock.get.return_value = self.MockJournal
        self.issue_objects = patch("sync_kernel_to_website.models.Issue.objects")
        IssueObjectsMock = self.issue_objects.start()
        IssueObjectsMock.get.side_effect = models.Issue.DoesNotExist

        self.issue_data = load_json_fixture("kernel-issues-0001-3714-1998-v29.json")
        self.issue = IssueFactory(self.issue_data, "0001-3714", "12345")

    def test_attribute_number(self):
        self.assertIsNone(self.issue.number)

    def test_attribute_volume(self):
        self.assertEqual(self.issue.volume, "29")

    def test_attribute_label(self):
        self.assertEqual(self.issue.label, "v29")

    def test_attribute_suppl_text(self):
        self.assertIsNone(self.issue.suppl_text)

    def test_attribute_type(self):
        # deveria ser regular ou publicação contínua...
        self.assertEqual(self.issue.type, "volume_issue")


class IssueFactoryNumTests(unittest.TestCase):
    def setUp(self):
        self.mongo_connect_mock = patch(
            "sync_kernel_to_website.mongo_connect"
        )
        self.mongo_connect_mock.start()
        self.journal_objects = patch(
            "sync_kernel_to_website.models.Journal.objects"
        )
        self.MockJournal = MagicMock(spec=models.Journal)
        JournalObjectsMock = self.journal_objects.start()
        JournalObjectsMock.get.return_value = self.MockJournal
        self.issue_objects = patch("sync_kernel_to_website.models.Issue.objects")
        IssueObjectsMock = self.issue_objects.start()
        IssueObjectsMock.get.side_effect = models.Issue.DoesNotExist

        self.issue_data = load_json_fixture("kernel-issues-0001-3714-1998-n3.json")
        self.issue = IssueFactory(self.issue_data, "0001-3714", "12345")

    def tearDown(self):
        self.mongo_connect_mock.stop()
        self.journal_objects.stop()
        self.issue_objects.stop()

    def test_attribute_number(self):
        self.assertEqual(self.issue.number, "3")

    def test_attribute_volume(self):
        # idealmente volume deveria ser None
        self.assertEqual(self.issue.volume, "")

    def test_attribute_label(self):
        self.assertEqual(self.issue.label, "n3")

    def test_attribute_suppl_text(self):
        self.assertIsNone(self.issue.suppl_text)

    def test_attribute_type(self):
        self.assertEqual(self.issue.type, "regular")


class IssueFactoryAOPTests(unittest.TestCase):
    def setUp(self):
        self.mongo_connect_mock = patch(
            "sync_kernel_to_website.mongo_connect"
        )
        self.mongo_connect_mock.start()
        self.journal_objects = patch(
            "sync_kernel_to_website.models.Journal.objects"
        )
        self.MockJournal = MagicMock(spec=models.Journal)
        JournalObjectsMock = self.journal_objects.start()
        JournalObjectsMock.get.return_value = self.MockJournal
        self.issue_objects = patch("sync_kernel_to_website.models.Issue.objects")
        IssueObjectsMock = self.issue_objects.start()
        IssueObjectsMock.get.side_effect = models.Issue.DoesNotExist

        self.issue_data = load_json_fixture("kernel-issues-0001-3714-1998-aop.json")
        self.issue = IssueFactory(self.issue_data, "0001-3714", "12345")

    def tearDown(self):
        self.mongo_connect_mock.stop()
        self.journal_objects.stop()
        self.issue_objects.stop()

    def test_attribute_number(self):
        self.assertEqual(self.issue.number, "ahead")

    def test_attribute_volume(self):
        # idealmente volume deveria ser None
        self.assertEqual(self.issue.volume, '')

    def test_attribute_label(self):
        self.assertIsNone(self.issue.label)

    def test_attribute_suppl_text(self):
        self.assertIsNone(self.issue.suppl_text)

    def test_attribute_type(self):
        self.assertEqual(self.issue.type, "ahead")


class ArticleFactoryTests(unittest.TestCase):
    def setUp(self):
        self.article_objects = patch(
            "operations.sync_kernel_to_website_operations.models.Article.objects"
        )
        self.issue_objects = patch(
            "operations.sync_kernel_to_website_operations.models.Issue.objects"
        )
        ArticleObjectsMock = self.article_objects.start()
        self.issue_objects.start()

        ArticleObjectsMock.get.side_effect = models.Article.DoesNotExist

        self.document_front = load_json_fixture(
            "kernel-document-front-s1518-8787.2019053000621.json"
        )
        self.document = ArticleFactory(
            "67TH7T7CyPPmgtVrGXhWXVs", self.document_front, "issue-1", 621, ""
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

        self.assertEqual(self.document._id, "67TH7T7CyPPmgtVrGXhWXVs")
        self.assertEqual(self.document.aid, "67TH7T7CyPPmgtVrGXhWXVs")
        self.assertEqual(self.document.doi, "10.11606/S1518-8787.2019053000621")

        self.document.scielo_pids['other'] = sorted(self.document.scielo_pids.get('other'))

        self.assertDictEqual(self.document.scielo_pids, {
            'v1': 'S1518-8787(19)03000621',
            'v2': 'S1518-87872019053000621',
            'v3': '67TH7T7CyPPmgtVrGXhWXVs',
            'other': sorted(['S1518-87872019005000621', 'S1518-87872019053000621', '67TH7T7CyPPmgtVrGXhWXVs'])
        })

    def test_has_authors_attribute(self):
        self.assertTrue(hasattr(self.document, "authors"))

    def test_has_authors_meta_attribute(self):
        self.assertTrue(hasattr(self.document, "authors_meta"))

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
        self.assertEqual("2019-01-31", self.document.publication_date)

    def test_has_publication_date_attribute_with_just_year(self):

        document_dict = {"pub_date": [
                          {
                            "text": [
                              "2019"
                            ],
                            "pub_type": [
                              "epub"
                            ],
                            "pub_format": [],
                            "date_type": [],
                            "day": [
                              "31"
                            ],
                            "month": [
                              "01"
                            ],
                            "year": [
                              "2019"
                            ],
                            "season": []
                          }
                        ]}

        document = ArticleFactory(
            "67TH7T7CyPPmgtVrGXhWXVs", document_dict, "issue-1", 621, ""
        )

        self.assertTrue(hasattr(document, "publication_date"))
        self.assertEqual("2019", document.publication_date)

    def test_has_publication_date_attribute_with_just_month(self):

        document_dict = {"pub_date": [
                          {
                            "text": [
                              "01"
                            ],
                            "pub_type": [
                              "epub"
                            ],
                            "pub_format": [],
                            "date_type": [],
                            "day": [
                              "31"
                            ],
                            "month": [
                              "01"
                            ],
                            "year": [
                              "2019"
                            ],
                            "season": []
                          }
                        ]}

        document = ArticleFactory(
            "67TH7T7CyPPmgtVrGXhWXVs", document_dict, "issue-1", 621, ""
        )

        self.assertTrue(hasattr(document, "publication_date"))
        self.assertEqual("01", document.publication_date)

    def test_has_publication_date_attribute_with_just_month_year(self):

        document_dict = {"pub_date": [
                          {
                            "text": [
                              "01 2019"
                            ],
                            "pub_type": [
                              "epub"
                            ],
                            "pub_format": [],
                            "date_type": [],
                            "day": [
                              "31"
                            ],
                            "month": [
                              "01"
                            ],
                            "year": [
                              "2019"
                            ],
                            "season": []
                          }
                        ]}

        document = ArticleFactory(
            "67TH7T7CyPPmgtVrGXhWXVs", document_dict, "issue-1", 621, ""
        )

        self.assertTrue(hasattr(document, "publication_date"))
        self.assertEqual("2019-01", document.publication_date)

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
        self.assertEqual(621, self.document.order)

    def test_has_xml_attribute(self):
        self.assertTrue(hasattr(self.document, "xml"))

    def test_has_htmls_attribute(self):
        self.assertTrue(hasattr(self.document, "htmls"))

    def test_htmls_attibutes_should_be_populated_with_documents_languages(self):
        self.assertEqual([{"lang": "en"}, {"lang": "pt"}], self.document.htmls)

    def test_has_created_attribute(self):
        self.assertTrue(hasattr(self.document, "created"))
        self.assertIsNotNone(self.document.created)

    def test_has_updated_attribute(self):
        self.assertTrue(hasattr(self.document, "updated"))
        self.assertIsNotNone(self.document.updated)

    def test_order_attribute_returns_last_five_digits_of_pid_v2_if_document_order_is_invalid(self):
        for order in ("1bla", None):
            with self.subTest(order=order):
                article = ArticleFactory(
                    document_id="67TH7T7CyPPmgtVrGXhWXVs",
                    data=self.document_front,
                    issue_id="issue-1",
                    document_order=order,
                    document_xml_url=""
                )
                self.assertEqual(621, article.order)

    def test_order_attribute_raise_invalid_order_value_error_because_pid_v2_is_None_and_order_is_alnum(self):
        front = load_json_fixture(
            "kernel-document-front-s1518-8787.2019053000621_sem_pid_v2.json"
        )
        with self.assertRaises(InvalidOrderValueError):
            ArticleFactory(
                document_id="67TH7T7CyPPmgtVrGXhWXVs",
                data=front,
                issue_id="issue-1",
                document_order="bla",
                document_xml_url=""
            )

    def test_order_attribute_returns_zero_because_pid_v2_is_None_and_order_is_None(self):
        front = load_json_fixture(
            "kernel-document-front-s1518-8787.2019053000621_sem_pid_v2.json"
        )
        with self.assertRaises(InvalidOrderValueError):
            ArticleFactory(
                document_id="67TH7T7CyPPmgtVrGXhWXVs",
                data=front,
                issue_id="issue-1",
                document_order=None,
                document_xml_url=""
            )

    def test_order_attribute_returns_order(self):
        front = load_json_fixture(
            "kernel-document-front-s1518-8787.2019053000621_sem_pid_v2.json"
        )
        article = ArticleFactory(
            document_id=MagicMock(),
            data=front,
            issue_id=MagicMock(),
            document_order="1234",
            document_xml_url=MagicMock()
        )
        self.assertEqual(1234, article.order)

    def test_authors_meta_has_attribute_name(self):
        self.assertEqual(self.document.authors_meta[0].name, "Kindermann, Lucas")
        self.assertEqual(self.document.authors_meta[1].name, "Traebert, Jefferson")
        self.assertEqual(self.document.authors_meta[2].name, "Nunes, Rodrigo Dias")

    def test_authors_meta_has_attribute_orcid(self):
        self.assertEqual(self.document.authors_meta[0].orcid, "0000-0002-9789-501X")
        self.assertEqual(self.document.authors_meta[1].orcid, "0000-0002-7389-985X")
        self.assertEqual(self.document.authors_meta[2].orcid, "0000-0002-2261-8253")

    def test_authors_meta_has_attribute_affiliation(self):
        self.assertEqual(self.document.authors_meta[0].affiliation, "Universidade do Sul de Santa Catarina")
        self.assertEqual(self.document.authors_meta[1].affiliation, "Universidade do Sul de Santa Catarina")
        self.assertEqual(self.document.authors_meta[2].affiliation, "Universidade do Sul de Santa Catarina")


@patch("operations.sync_kernel_to_website_operations.models.Article.objects")
@patch("operations.sync_kernel_to_website_operations.models.Issue.objects")
class ExAOPArticleFactoryTests(unittest.TestCase):
    def setUp(self):
        self.document_front = load_json_fixture(
            "kernel-document-front-s1518-8787.2019053000621.json"
        )
        data = {
            "id": "0101-0101",
            "created": "2019-11-28T00:00:00.000000Z",
            "metadata": {},
        }
        self.issue = models.Issue()
        self.issue._id = "0101-0101-aop"
        self.issue.year = "2019"
        self.issue.number = "ahead"
        self.issue.url_segment = "2019.nahead"

    def test_sets_aop_url_segs(self, MockIssueObjects, MockArticleObjects):
        MockArticle = MagicMock(
            spec=models.Article,
            aop_url_segs=None,
            url_segment="10.151/S1518-8787.2019053000621"
        )
        MockArticle.issue = self.issue
        MockArticleObjects.get.return_value = MockArticle
        self.document = ArticleFactory(
            "67TH7T7CyPPmgtVrGXhWXVs", self.document_front, "issue-1", "1", ""
        )
        self.assertIsNotNone(self.document.aop_url_segs)
        self.assertIsInstance(self.document.aop_url_segs, models.AOPUrlSegments)
        self.assertEqual(
            self.document.aop_url_segs.url_seg_article,
            "10.151/S1518-8787.2019053000621"
        )
        self.assertEqual(
            self.document.aop_url_segs.url_seg_issue,
            "2019.nahead"
        )

    def test_article_factory_creates_aop_id_and_update_pid_with_scielo_pids_v2(
            self, MockIssueObjects, MockArticleObjects):
        MockArticle = MagicMock(
            spec=models.Article,
            aop_url_segs=None,
            url_segment="10.151/S1518-8787.2019053000621",
            pid="S1518-87872019005000621",
        )
        MockArticleObjects.get.return_value = MockArticle

        regular_issue_id = None

        # ArticleFactory
        self.document = ArticleFactory(
            "67TH7T7CyPPmgtVrGXhWXVs", self.document_front,
            regular_issue_id, "1", ""
        )
        self.assertEqual(self.document.pid, "S1518-87872019053000621")
        self.assertEqual(self.document.aop_pid, '')

    def test_article_factory_creates_aop_id_from_previous_pid_and_update_pid_with_scielo_pids_v2(
            self, MockIssueObjects, MockArticleObjects):
        MockArticle = MagicMock(
            spec=models.Article,
            aop_url_segs=None,
            url_segment="10.151/S1518-8787.2019053000621",
            pid="S1518-87872019005000621",
        )
        MockArticleObjects.get.return_value = MockArticle

        regular_issue_id = self.issue._id

        # obtém de kernel front: previous_pid (nao implementado no clea ainda)
        self.document_front = load_json_fixture(
            "kernel-document-front-s1518-8787.2019053000621_previous_pid.json"
        )

        # ArticleFactory
        self.document = ArticleFactory(
            "67TH7T7CyPPmgtVrGXhWXVs", self.document_front,
            regular_issue_id, "1", ""
        )
        self.assertEqual(self.document.pid, "S1518-87872019053000621")
        self.assertEqual(self.document.aop_pid, "S1518-8787XXXX005000621")


@patch("operations.sync_kernel_to_website_operations.models.Article.objects")
@patch("operations.sync_kernel_to_website_operations.models.Issue.objects")
class AbstractsArticleFactoryTests(unittest.TestCase):
    def setUp(self):
        self.document_front = load_json_fixture(
            "kernel-document-front-s1518-8787.2019053000621.json"
        )

    def test_no_trans_abstracts_attribute(self, MockIssueObjects, MockArticleObjects):
        MockArticleObjects.get.side_effect = models.Article.DoesNotExist
        self.document = ArticleFactory(
            "67TH7T7CyPPmgtVrGXhWXVs", self.document_front, "issue-1", 621, ""
        )

        self.assertTrue(hasattr(self.document, "abstracts"))
        self.assertEqual(2, len(self.document.abstracts))
        self.assertTrue(hasattr(self.document, "abstract_languages"))
        self.assertEqual(2, len(self.document.abstract_languages))

    def test_no_abstract(self, MockIssueObjects, MockArticleObjects):
        MockArticleObjects.get.side_effect = models.Article.DoesNotExist
        self.document_front["article_meta"] = [{
            "abstract": []
        }]
        self.document_front["trans_abstract"] = []
        self.document_front["sub_article"] = [{
            "article_meta": [{
                "abstract": []
            }]
        }]
        self.document = ArticleFactory(
            "67TH7T7CyPPmgtVrGXhWXVs", self.document_front, "issue-1", 621, ""
        )

        self.assertTrue(hasattr(self.document, "abstracts"))
        self.assertEqual(0, len(self.document.abstracts))
        self.assertTrue(hasattr(self.document, "abstract_languages"))
        self.assertEqual(0, len(self.document.abstract_languages))

    def test_has_trans_abstract(self, MockIssueObjects, MockArticleObjects):
        MockArticleObjects.get.side_effect = models.Article.DoesNotExist
        self.document_front["trans_abstract"] = [{
            "lang": ["fr"],
            "text": ["Resumen."],
            "title": ["Resumen"],
        }]
        self.document = ArticleFactory(
            "67TH7T7CyPPmgtVrGXhWXVs", self.document_front, "issue-1", 621, ""
        )

        self.assertTrue(hasattr(self.document, "abstracts"))
        self.assertEqual(3, len(self.document.abstracts))
        self.assertEqual(self.document.abstracts[0].language, "en")
        self.assertEqual(self.document.abstracts[1].language, "fr")
        self.assertEqual(self.document.abstracts[2].language, "pt")
        self.assertTrue(hasattr(self.document, "abstract_languages"))
        self.assertEqual(3, len(self.document.abstract_languages))
        self.assertEqual(self.document.abstract_languages, ["en", "fr", "pt"])

    def test_abstract_and_trans_abstract(self, MockIssueObjects, MockArticleObjects):
        MockArticleObjects.get.side_effect = models.Article.DoesNotExist
        self.document_front["article"] = [{
            "lang": ["en"],
        }]
        self.document_front["article_meta"] = [{
            "abstract": ["ABSTRACT: an abstract."]
        }]
        self.document_front["trans_abstract"] = [{
            "lang": ["pt"],
            "text": ["Resumo: um resumo."],
            "title": ["Resumo"],
        }]
        self.document_front["sub_article"] = [{
            "article_meta": [{
                "abstract": []
            }]
        }]
        self.document = ArticleFactory(
            "67TH7T7CyPPmgtVrGXhWXVs", self.document_front, "issue-1", 621, ""
        )

        self.assertTrue(hasattr(self.document, "abstracts"))
        self.assertEqual(2, len(self.document.abstracts))
        self.assertEqual(self.document.abstracts[0].language, "en")
        self.assertEqual(self.document.abstracts[0].text, "ABSTRACT: an abstract.")
        self.assertEqual(self.document.abstracts[1].language, "pt")
        self.assertEqual(self.document.abstracts[1].text, "Resumo: um resumo.")
        self.assertEqual(2, len(self.document.abstract_languages))
        self.assertEqual(self.document.abstract_languages, ["en", "pt"])


class RegisterDocumentTests(unittest.TestCase):
    def setUp(self):
        self.documents = ["67TH7T7CyPPmgtVrGXhWXVs"]
        self.document_front = load_json_fixture(
            "kernel-document-front-s1518-8787.2019053000621.json"
        )

        mk_hooks = patch("operations.sync_kernel_to_website_operations.hooks")
        self.mk_hooks = mk_hooks.start()

    def tearDown(self):
        self.mk_hooks.stop()

    def test_try_register_documents_call_save_methods_from_article_instance(self):
        article_factory_mock = MagicMock()
        article_instance_mock = MagicMock()
        article_factory_mock.return_value = article_instance_mock

        try_register_documents(
            documents=self.documents,
            get_relation_data=lambda document_id, _front_data: (
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
            get_relation_data=lambda _, _front_data: ("", {}),
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
            get_relation_data=lambda _, _front_data: (
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
            None,
        )

class ArticleRenditionFactoryTests(unittest.TestCase):
    def setUp(self):
        self.article_objects = patch(
            "operations.sync_kernel_to_website_operations.models.Article.objects"
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
            [
                {
                    "lang": "en",
                    "url": "//object-storage/file.pdf",
                    "type": "pdf",
                    "filename": "filename.pdf",
                }
            ],
            self.article.pdfs,
        )


class RegisterDocumentRenditionsTest(unittest.TestCase):
    def setUp(self):
        self.documents = ["67TH7T7CyPPmgtVrGXhWXVs"]
        self.document_front = load_json_fixture(
            "kernel-document-front-s1518-8787.2019053000621.json"
        )

        mk_hooks = patch("operations.sync_kernel_to_website_operations.hooks")
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


class KernelFrontDataTests(unittest.TestCase):

    def _get_article(self):
        return {
            "article_meta": [
              {
                "article_doi": [
                  "10.11606/S1518-8787.2019053000621"
                ],
                "article_publisher_id": [
                    "S1518-87872019053000621",
                    "67TH7T7CyPPmgtVrGXhWXVs",
                    "S1518-87872019005000621"
                ],
                "scielo_pid_v1": [
                    "S1518-8787(19)03000621"
                ],
                "scielo_pid_v2": [
                    "S1518-87872019053000621"
                ],
                "scielo_pid_v3": [
                    "67TH7T7CyPPmgtVrGXhWXVs"
                ],
                "pub_volume": [
                  "53"
                ],
                "pub_issue": []
              }
            ],
            "pub_date": [
              {
                "text": [
                  "31 01 2019"
                ],
                "pub_type": [
                  "epub"
                ],
                "pub_format": [],
                "date_type": [],
                "day": [
                  "31"
                ],
                "month": [
                  "01"
                ],
                "year": [
                  "2019"
                ],
                "season": []
              }
            ],
        }

    def test__get_bundle_pub_year_returns_date_type_collection_year(self):
        article = self._get_article()
        article_meta_pub_date = article["pub_date"]
        expected = "2019"
        result = _get_bundle_pub_year(article_meta_pub_date)
        self.assertEqual(expected, result)

    def test__get_bundle_pub_year_returns_pub_type_collection_year(self):
        article_meta_pub_date = [
            {
                "text": ["14 08 2020"],
                "pub_type": ["pub"],
                "pub_format": ["electronic"],
                "date_type": [],
                "day": ["14"],
                "month": ["08"],
                "year": ["2021"],
                "season": []
            }, {
                "text": ["08 2020"],
                "pub_type": ["collection"],
                "pub_format": ["electronic"],
                "date_type": [],
                "day": [],
                "month": ["08"],
                "year": ["2020"],
                "season": []
            }
        ]
        expected = "2020"
        result = _get_bundle_pub_year(article_meta_pub_date)
        self.assertEqual(expected, result)

    def test__get_bundle_pub_year_returns_any_pub_year(self):
        article_meta_pub_date = [
            {
                "text": ["14 08 2020"],
                "pub_type": [],
                "pub_format": ["electronic"],
                "date_type": ["pub"],
                "day": ["14"],
                "month": ["08"],
                "year": ["2021"],
                "season": []
            }
        ]
        expected = "2021"
        result = _get_bundle_pub_year(article_meta_pub_date)
        self.assertEqual(expected, result)

    def test__get_bundle_pub_year_raises_missing_pub_year_error(self):
        article_meta_pub_date = [
            {
                "text": ["14 08 2020"],
                "pub_type": [],
                "pub_format": ["electronic"],
                "date_type": ["pub"],
                "day": ["14"],
                "month": ["08"],
                "season": []
            }
        ]
        with self.assertRaises(KernelFrontHasNoPubYearError) as exc:
            _get_bundle_pub_year(article_meta_pub_date)
        self.assertEqual(
            "Missing publication year in: {}".format(article_meta_pub_date),
            str(exc.exception)
        )

    def test__get_bundle_pub_year_raises_missing_pub_year_error_2(self):
        article_meta_pub_date = None
        with self.assertRaises(KernelFrontHasNoPubYearError) as exc:
            _get_bundle_pub_year(article_meta_pub_date)
        self.assertEqual(
            "Missing publication year in: {}".format(article_meta_pub_date),
            str(exc.exception)
        )

    def test__get_bundle_id_returns_bundle_id(self):
        article = self._get_article()
        expected = "1518-8787-2019-v53"
        result = _get_bundle_id(article)
        self.assertEqual(expected, result)

    def test__get_bundle_id_returns_bundle_id_for_5_Suppl(self):
        article = self._get_article()
        article['article_meta'][0]['pub_issue'] = ['5 Suppl']
        expected = "1518-8787-2019-v53-n5-s0"
        result = _get_bundle_id(article)
        self.assertEqual(expected, result)

    def test__get_bundle_id_returns_bundle_id_for_5_Suppl_1(self):
        article = self._get_article()
        article['article_meta'][0]['pub_issue'] = ['5 Suppl 1']
        expected = "1518-8787-2019-v53-n5-s1"
        result = _get_bundle_id(article)
        self.assertEqual(expected, result)

    def test__get_bundle_id_returns_bundle_id_for_5_spe(self):
        article = self._get_article()
        article['article_meta'][0]['pub_issue'] = ['5 spe']
        expected = "1518-8787-2019-v53-n5spe"
        result = _get_bundle_id(article)
        self.assertEqual(expected, result)

    def test__get_bundle_id_returns_bundle_id_for_5_suppl(self):
        article = self._get_article()
        article['article_meta'][0]['pub_issue'] = ['5 suppl']
        expected = "1518-8787-2019-v53-n5-s0"
        result = _get_bundle_id(article)
        self.assertEqual(expected, result)

    def test__get_bundle_id_returns_bundle_id_for_5_suppl_1(self):
        article = self._get_article()
        article['article_meta'][0]['pub_issue'] = ['5 suppl 1']
        expected = "1518-8787-2019-v53-n5-s1"
        result = _get_bundle_id(article)
        self.assertEqual(expected, result)

    def test__get_bundle_id_returns_bundle_id_for_5_suppl_dot__1(self):
        article = self._get_article()
        article['article_meta'][0]['pub_issue'] = ['5 suppl. 1']
        expected = "1518-8787-2019-v53-n5-s1"
        result = _get_bundle_id(article)
        self.assertEqual(expected, result)

    def test__get_bundle_id_returns_bundle_id_for_25_Suppl_1(self):
        article = self._get_article()
        article['article_meta'][0]['pub_issue'] = ['25 Suppl 1']
        expected = "1518-8787-2019-v53-n25-s1"
        result = _get_bundle_id(article)
        self.assertEqual(expected, result)

    def test__get_bundle_id_returns_bundle_id_for_2_1_hyphen_5_suppl_1(self):
        article = self._get_article()
        article['article_meta'][0]['pub_issue'] = ['2-5 suppl 1']
        expected = "1518-8787-2019-v53-n2-5-s1"
        result = _get_bundle_id(article)
        self.assertEqual(expected, result)

    def test__get_bundle_id_returns_bundle_id_for_2spe(self):
        article = self._get_article()
        article['article_meta'][0]['pub_issue'] = ['2spe']
        expected = "1518-8787-2019-v53-n2spe"
        result = _get_bundle_id(article)
        self.assertEqual(expected, result)

    def test__get_bundle_id_returns_bundle_id_for_Spe(self):
        article = self._get_article()
        article['article_meta'][0]['pub_issue'] = ['Spe']
        expected = "1518-8787-2019-v53-nspe"
        result = _get_bundle_id(article)
        self.assertEqual(expected, result)

    def test__get_bundle_id_returns_bundle_id_for_Supl_dot__1(self):
        article = self._get_article()
        article['article_meta'][0]['pub_issue'] = ['Supl. 1']
        expected = "1518-8787-2019-v53-s1"
        result = _get_bundle_id(article)
        self.assertEqual(expected, result)

    def test__get_bundle_id_returns_bundle_id_for_Suppl(self):
        article = self._get_article()
        article['article_meta'][0]['pub_issue'] = ['Suppl']
        expected = "1518-8787-2019-v53-s0"
        result = _get_bundle_id(article)
        self.assertEqual(expected, result)

    def test__get_bundle_id_returns_bundle_id_for_Suppl_12(self):
        article = self._get_article()
        article['article_meta'][0]['pub_issue'] = ['Suppl 12']
        expected = "1518-8787-2019-v53-s12"
        result = _get_bundle_id(article)
        self.assertEqual(expected, result)

    def test__get_bundle_id_returns_bundle_id_for_s2(self):
        article = self._get_article()
        article['article_meta'][0]['pub_issue'] = ['s2']
        expected = "1518-8787-2019-v53-s2"
        result = _get_bundle_id(article)
        self.assertEqual(expected, result)

    def test__get_bundle_id_returns_bundle_id_for_spe(self):
        article = self._get_article()
        article['article_meta'][0]['pub_issue'] = ['spe']
        expected = "1518-8787-2019-v53-nspe"
        result = _get_bundle_id(article)
        self.assertEqual(expected, result)

    def test__get_bundle_id_returns_bundle_id_for_Especial(self):
        article = self._get_article()
        article['article_meta'][0]['pub_issue'] = ['Especial']
        expected = "1518-8787-2019-v53-nspe"
        result = _get_bundle_id(article)
        self.assertEqual(expected, result)

    def test__get_bundle_id_returns_bundle_id_for_spe_1(self):
        article = self._get_article()
        article['article_meta'][0]['pub_issue'] = ['spe 1']
        expected = "1518-8787-2019-v53-nspe1"
        result = _get_bundle_id(article)
        self.assertEqual(expected, result)

    def test__get_bundle_id_returns_bundle_id_for_spe_pr(self):
        article = self._get_article()
        article['article_meta'][0]['pub_issue'] = ['spe pr']
        expected = "1518-8787-2019-v53-nspepr"
        result = _get_bundle_id(article)
        self.assertEqual(expected, result)

    def test__get_bundle_id_returns_bundle_id_for_spe2(self):
        article = self._get_article()
        article['article_meta'][0]['pub_issue'] = ['spe2']
        expected = "1518-8787-2019-v53-nspe2"
        result = _get_bundle_id(article)
        self.assertEqual(expected, result)

    def test__get_bundle_id_returns_bundle_id_for_spe_dot_2(self):
        article = self._get_article()
        article['article_meta'][0]['pub_issue'] = ['spe.2']
        expected = "1518-8787-2019-v53-nspe2"
        result = _get_bundle_id(article)
        self.assertEqual(expected, result)

    def test__get_bundle_id_returns_bundle_id_for_supp_1(self):
        article = self._get_article()
        article['article_meta'][0]['pub_issue'] = ['supp 1']
        expected = "1518-8787-2019-v53-s1"
        result = _get_bundle_id(article)
        self.assertEqual(expected, result)

    def test__get_bundle_id_returns_bundle_id_for_suppl(self):
        article = self._get_article()
        article['article_meta'][0]['pub_issue'] = ['suppl']
        expected = "1518-8787-2019-v53-s0"
        result = _get_bundle_id(article)
        self.assertEqual(expected, result)

    def test__get_bundle_id_returns_bundle_id_for_suppl_1(self):
        article = self._get_article()
        article['article_meta'][0]['pub_issue'] = ['suppl 1']
        expected = "1518-8787-2019-v53-s1"
        result = _get_bundle_id(article)
        self.assertEqual(expected, result)

    def test__get_bundle_id_returns_bundle_id_for_suppl_12(self):
        article = self._get_article()
        article['article_meta'][0]['pub_issue'] = ['suppl 12']
        expected = "1518-8787-2019-v53-s12"
        result = _get_bundle_id(article)
        self.assertEqual(expected, result)

    def test__get_bundle_id_returns_bundle_id_for_suppl_1_hyphen_2(self):
        article = self._get_article()
        article['article_meta'][0]['pub_issue'] = ['suppl 1-2']
        expected = "1518-8787-2019-v53-s1-2"
        result = _get_bundle_id(article)
        self.assertEqual(expected, result)

    def test__get_bundle_id_returns_bundle_id_for_suppl_dot__1(self):
        article = self._get_article()
        article['article_meta'][0]['pub_issue'] = ['suppl. 1']
        expected = "1518-8787-2019-v53-s1"
        result = _get_bundle_id(article)
        self.assertEqual(expected, result)

    def test__get_bundle_id_returns_bundle_id_for____aop(self):
        article = self._get_article()
        article['article_meta'][0]['pub_volume'] = []
        article['article_meta'][0]['pub_issue'] = []
        expected = "1518-8787-aop"
        result = _get_bundle_id(article)
        self.assertEqual(expected, result)


@patch("sync_kernel_to_website.fetch_documents_front")
@patch("sync_kernel_to_website.fetch_bundles")
class TestGetRelationDataFromKernelBundle(unittest.TestCase):

    def _bundle_data(self):
        return (
            {
                "_id": "1518-8787-2019-v53",
                "id": "1518-8787-2019-v53",
                "created": "2021-04-23T10:02:40.486194Z",
                "updated": "2021-04-23T19:24:13.583105Z",
                "items": [
                    {"id": "tH9GfMmrX6dk3x9NtcDJG3v", "order": "00383"},
                    {"id": "mYnRLqHZJd3K3g7DSNdhLXB", "order": "00395"},
                    {"id": "TXvqPFmSZCdvK9trq5TzRhB", "order": "00381"}],
                "metadata": {
                    "publication_months": {"range": [3, 3]},
                    "publication_year": "2019", "volume": "53",
                    "pid": "1518-878720190530"}
            }
        )

    def _front_data(self):
        return (
            {
                "article_meta": [
                  {
                    "article_doi": [
                      "10.11606/S1518-8787.2019053000621"
                    ],
                    "article_publisher_id": [
                        "S1518-87872019053000621",
                        "TXvqPFmSZCdvK9trq5TzRhB",
                        "S1518-87872019005000621"
                    ],
                    "scielo_pid_v1": [
                        "S1518-8787(19)03000621"
                    ],
                    "scielo_pid_v2": [
                        "S1518-87872019053000621"
                    ],
                    "scielo_pid_v3": [
                        "TXvqPFmSZCdvK9trq5TzRhB"
                    ],
                    "pub_volume": [
                      "53"
                    ],
                    "pub_issue": []
                  }
                ],
                "pub_date": [
                  {
                    "text": [
                      "31 01 2019"
                    ],
                    "pub_type": [
                      "epub"
                    ],
                    "pub_format": [],
                    "date_type": [],
                    "day": [
                      "31"
                    ],
                    "month": [
                      "01"
                    ],
                    "year": [
                      "2019"
                    ],
                    "season": []
                  }
                ],
            }
        )

    def test__get_relation_data_from_kernel_bundle_calls_fetch_documents_front(
                self,
                mock_fetch_bundles,
                mock_fetch_documents_front,
            ):
        mock_fetch_bundles.return_value = self._bundle_data()
        mock_fetch_documents_front.return_value = self._front_data()

        expected = (
            "1518-8787-2019-v53",
            {"id": "TXvqPFmSZCdvK9trq5TzRhB", "order": "00381"})
        result = _get_relation_data_from_kernel_bundle(
            "TXvqPFmSZCdvK9trq5TzRhB", front_data=None)
        self.assertEqual(expected, result)
        mock_fetch_documents_front.assert_called_once()
        mock_fetch_bundles.assert_called_once()

    def test__get_relation_data_from_kernel_bundle_uses_front_data_provided(
                self,
                mock_fetch_bundles,
                mock_fetch_documents_front,
            ):
        mock_fetch_bundles.return_value = self._bundle_data()
        expected = (
            "1518-8787-2019-v53",
            {"id": "TXvqPFmSZCdvK9trq5TzRhB", "order": "00381"})
        result = _get_relation_data_from_kernel_bundle(
            "TXvqPFmSZCdvK9trq5TzRhB", front_data=self._front_data())
        self.assertEqual(expected, result)
        mock_fetch_documents_front.assert_not_called()
        mock_fetch_bundles.assert_called_once()

    def test__get_relation_data_from_kernel_bundle_fetch_bundles_not_called(
                self,
                mock_fetch_bundles,
                mock_fetch_documents_front,
            ):
        mock_fetch_bundles.return_value = self._bundle_data()
        mock_fetch_documents_front.return_value = {}
        expected = (None, {})
        result = _get_relation_data_from_kernel_bundle(
            "TXvqPFmSZCdvK9trq5TzRhB", front_data=None)
        self.assertEqual(expected, result)
        mock_fetch_documents_front.assert_called_once()
        mock_fetch_bundles.assert_not_called()

    def test__get_relation_data_from_kernel_bundle_fetch_bundles_has_no_docs(
                self,
                mock_fetch_bundles,
                mock_fetch_documents_front,
            ):
        mock_fetch_bundles.return_value = {}
        mock_fetch_documents_front.return_value = {}
        expected = ("1518-8787-2019-v53", {})
        result = _get_relation_data_from_kernel_bundle(
            "TXvqPFmSZCdvK9trq5TzRhB", front_data=self._front_data())
        self.assertEqual(expected, result)

    def test__get_relation_data_from_kernel_bundle_fetch_bundles_doc_not_found(
                self,
                mock_fetch_bundles,
                mock_fetch_documents_front,
            ):
        mock_fetch_bundles.return_value = self._bundle_data()
        mock_fetch_documents_front.return_value = {}
        expected = ("1518-8787-2019-v53", {})
        result = _get_relation_data_from_kernel_bundle(
            "TXvQPFmSZCdvK9trq5TzRhB", front_data=self._front_data())
        self.assertEqual(expected, result)


class ArticleFactoryDisplayFormatTests(unittest.TestCase):
    def setUp(self):
        self.article_objects = patch(
            "operations.sync_kernel_to_website_operations.models.Article.objects"
        )
        self.issue_objects = patch(
            "operations.sync_kernel_to_website_operations.models.Issue.objects"
        )
        ArticleObjectsMock = self.article_objects.start()
        self.issue_objects.start()

        ArticleObjectsMock.get.side_effect = models.Article.DoesNotExist

        self.document_front = load_json_fixture(
            "kernel-document-front-s1518-8787.2019053000621.display_fmt.json"
        )
        self.document = ArticleFactory(
            "67TH7T7CyPPmgtVrGXhWXVs", self.document_front, "issue-1", 621, ""
        )

    def tearDown(self):
        self.article_objects.stop()
        self.issue_objects.stop()

    def test_translated_titles_attribute(self):
        expected = [
            {
                "name": "Título em português <sup>1</sup>",
                "language": "pt",
            },
            {
                "name": "Título em español <sup>1</sup>",
                "language": "es",
            }
        ]
        self.assertEqual(
            expected[0]['name'], self.document.translated_titles[0].name)
        self.assertEqual(
            expected[1]['name'], self.document.translated_titles[1].name)

    def test_main_title_attribute(self):
        self.assertEqual("Article title <sup>1</sup>", self.document.title)


@patch("operations.sync_kernel_to_website_operations.models.Article.objects")
class TestUnpublishDocuments(unittest.TestCase):
    def test_unpublish_repeated_documents(self, mock_objects):
        document_id = "doc"
        doi = "doi"
        mock_objects.return_value = [
            MockArticle("id1", "pid1", aop_pid=None, scielo_pids=None),
            MockArticle("id2", "pid2", aop_pid=None, scielo_pids=None),
            MockArticle("doc", "pdi3", aop_pid=None, scielo_pids=None),
        ]
        result = _unpublish_repeated_documents(document_id, doi)
        self.assertListEqual(sorted(["id1", "id2", "pid1", "pid2"]), sorted(result))

