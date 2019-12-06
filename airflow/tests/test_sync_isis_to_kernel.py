import os
import json
import unittest

from unittest.mock import Mock
import tempfile

from xylose.scielodocument import Issue

from sync_isis_to_kernel import (
    mount_journals_issues_link,
    issue_as_kernel,
    issue_data_to_link,
    create_journal_issn_index,
    journal_as_kernel,
)
from .test_sync_kernel_to_website import load_json_fixture

FIXTURES_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures")


class TestIssueToLink(unittest.TestCase):
    def setUp(self):
        self.issues = load_json_fixture("isis-issues.json")

    def test_issue_data_to_link_with_supplement(self):
        suppl_field_expected = (
            ("v131", u"2", "2"),
            ("v132", u"2", "2"),
            ("v131", u"0", "0"),
            ("v132", u"0", "0"),
        )
        data = self.issues[-1]
        for field, value, expected in suppl_field_expected:
            with self.subTest(field=field, value=value, expected=expected):
                data[field] = [{u"_": value}]
                issue = Issue({"issue": data})
                result = issue_data_to_link(issue)
                self.assertEqual(result["supplement"], expected)

    def test_issue_data_to_link_without_volume(self):
        data = self.issues[-1]
        del data["v31"]
        issue = Issue({"issue": data})
        result = issue_data_to_link(issue)
        self.assertIsNone(result.get("volume"))

    def test_issue_data_to_link_without_number(self):
        data = self.issues[-1]
        del data["v32"]
        issue = Issue({"issue": data})
        result = issue_data_to_link(issue)
        self.assertIsNone(result.get("number"))

    def test_issue_data_to_link_without_supplement(self):
        issue = Issue({"issue": self.issues[-1]})
        result = issue_data_to_link(issue)
        self.assertIsNone(result.get("supplement"))

    def test_issue_data_to_link_returns_issue_data_to_link_to_journal(self):
        issue = Issue({"issue": self.issues[-1]})
        result = issue_data_to_link(issue)
        self.assertEqual(result["id"], "1678-4464-2018-v1-n1")
        self.assertEqual(result["number"], "1")
        self.assertEqual(result["volume"], "1")
        self.assertEqual(result["year"], "2018")


class UpdateJournalAndIssueLink(unittest.TestCase):
    def setUp(self):
        self.issues = load_json_fixture("isis-issues.json")

    def test_mount_journals_issues_link(self):
        journals_issues_link = mount_journals_issues_link(self.issues)
        self.assertEqual(
            journals_issues_link["1678-4464"],
            [
                {
                    "id": "1678-4464-2018-v1-n1",
                    "order": "1001",
                    "year": "2018",
                    "volume": "1",
                    "number": "1",
                }
            ],
        )

    def test_mount_journals_issues_link_should_not_contains_duplicates_issues_ids(self):
        issues = self.issues * 3
        journals_issues_link = mount_journals_issues_link(issues)

        self.assertEqual(
            journals_issues_link["1678-4464"],
            [
                {
                    "id": "1678-4464-2018-v1-n1",
                    "order": "1001",
                    "year": "2018",
                    "volume": "1",
                    "number": "1",
                }
            ],
        )
        self.assertEqual(len(journals_issues_link["1678-4464"]), 1)

    def test_mount_journals_issues_link_should_not_contains_pressrelease(self):
        self.issues[-1]["v31"][0]["_"] = "pr"
        journals_issues_link = mount_journals_issues_link(self.issues)

        self.assertDictEqual(journals_issues_link, {})

    def test_mount_journals_issues_link_should_not_contains_ahead_of_print(self):
        self.issues[-1]["v31"][0]["_"] = "ahead"
        journals_issues_link = mount_journals_issues_link(self.issues)

        self.assertDictEqual(journals_issues_link, {})


class TestSaveJournalIssnIndex(unittest.TestCase):
    def test_creates_index_file(self):
        self.journals = [
            {
                "scielo_issn": "0001-0001",
                "print_issn": "0001-0001",
                "electronic_issn": "0001-000X",
            },
            {
                "scielo_issn": "0002-0001",
                "print_issn": "0002-0002",
                "electronic_issn": "0002-000X",
            },
            {
                "scielo_issn": "0003-000X",
                "print_issn": "0003-0001",
                "electronic_issn": "0003-000X",
            },
            {"scielo_issn": "0004-000X", "electronic_issn": "0004-000X",},
        ]
        expected = json.dumps(
            {
                "0001-0001": "0001-0001",
                "0001-000X": "0001-0001",
                "0002-0001": "0002-0001",
                "0002-0002": "0002-0001",
                "0002-000X": "0002-0001",
                "0003-000X": "0003-000X",
                "0003-0001": "0003-000X",
                "0004-000X": "0004-000X",
            }
        )
        with tempfile.TemporaryDirectory() as tmpdirname:
            create_journal_issn_index(tmpdirname, self.journals)
            with open(os.path.join(tmpdirname, "issn_index.json")) as index_file:
                result = index_file.read()
                self.assertEqual(result, expected)


class TestIssueAsKernel(unittest.TestCase):
    def setUp(self):
        self.mocked_issue = Mock()
        self.mocked_issue.start_month = None
        self.mocked_issue.end_month = None
        self.mocked_issue.data = {"issue": {"v35": [{"_": "1234-0987"}]}}
        self.mocked_issue.publication_date = "2017-09"
        self.mocked_issue.volume = None
        self.mocked_issue.number = None
        self.mocked_issue.supplement_volume = None
        self.mocked_issue.supplement_number = None
        self.mocked_issue.titles = {}

    def test_issue_as_kernel_returns_volume(self):
        mocked_issue = self.mocked_issue
        mocked_issue.volume = "1"
        result = issue_as_kernel(mocked_issue)
        self.assertEqual("1", result["volume"])

    def test_issue_as_kernel_do_not_return_volume(self):
        mocked_issue = self.mocked_issue
        mocked_issue.volume = None
        result = issue_as_kernel(mocked_issue)
        self.assertEqual("", result["volume"])

    def test_issue_as_kernel_returns_number(self):
        mocked_issue = self.mocked_issue
        mocked_issue.number = "1"
        result = issue_as_kernel(mocked_issue)
        self.assertEqual("1", result["number"])

    def test_issue_as_kernel_do_not_return_number(self):
        mocked_issue = self.mocked_issue
        mocked_issue.number = None
        result = issue_as_kernel(mocked_issue)
        self.assertEqual("", result["number"])

    def test_issue_as_kernel_returns_volume_supplement(self):
        mocked_issue = self.mocked_issue
        mocked_issue.supplement_volume = "4"
        mocked_issue.supplement_number = None
        result = issue_as_kernel(mocked_issue)
        self.assertEqual("4", result["supplement"])

    def test_issue_as_kernel_returns_number_supplement(self):
        mocked_issue = self.mocked_issue
        mocked_issue.supplement_volume = None
        mocked_issue.supplement_number = "3"
        result = issue_as_kernel(mocked_issue)
        self.assertEqual("3", result["supplement"])

    def test_issue_as_kernel_returns_supplement(self):
        mocked_issue = self.mocked_issue
        mocked_issue.type = "supplement"
        mocked_issue.supplement_volume = None
        mocked_issue.supplement_number = None
        result = issue_as_kernel(mocked_issue)
        self.assertEqual("0", result["supplement"])

    def test_issue_as_kernel_do_not_return_supplement(self):
        mocked_issue = self.mocked_issue
        mocked_issue.supplement_volume = None
        mocked_issue.supplement_number = None
        mocked_issue.type = None
        result = issue_as_kernel(mocked_issue)
        self.assertNotIn("supplement", result.keys())

    def test_issue_as_kernel_returns_titles(self):
        mocked_issue = self.mocked_issue
        mocked_issue.titles = {
            "es": "Título en Español",
            "pt": "Título em Português",
            "en": "English Title",
        }
        result = issue_as_kernel(mocked_issue)
        expected = [
            {"language": "es", "value": "Título en Español"},
            {"language": "pt", "value": "Título em Português"},
            {"language": "en", "value": "English Title"},
        ]
        self.assertEqual(expected, result["titles"])

    def test_issue_as_kernel_returns_months_range(self):
        mocked_issue = self.mocked_issue
        mocked_issue.start_month = "1"
        mocked_issue.end_month = "3"
        result = issue_as_kernel(mocked_issue)
        self.assertEqual([1, 3], result["publication_months"]["range"])

    def test_issue_as_kernel_returns_month(self):
        mocked_issue = self.mocked_issue
        mocked_issue.start_month = "1"
        result = issue_as_kernel(mocked_issue)
        self.assertEqual(1, result["publication_months"]["month"])

    def test_issue_as_kernel_returns_publication_year(self):
        mocked_issue = self.mocked_issue
        mocked_issue.publication_date = "2017-09"
        result = issue_as_kernel(mocked_issue)
        self.assertEqual("2017", result["publication_year"])

    def test_issue_as_kernel_returns__id(self):
        mocked_issue = self.mocked_issue
        mocked_issue.number = "2"
        mocked_issue.volume = "3"
        mocked_issue.supplement_volume = "0"
        mocked_issue.publication_date = "2013-09"
        mocked_issue.data = {"issue": {"v35": [{"_": "1234-0987"}]}}
        result = issue_as_kernel(mocked_issue)
        self.assertIsNotNone(result["_id"])

class TestJournalAsKernel(unittest.TestCase):
    def setUp(self):
        self.mocked_journal = Mock()
        self.mocked_journal.publisher_name = ["Casa Publicadora"]
        self.mocked_journal.publisher_country = "BR", "Brasil"
        self.mocked_journal.publisher_state = "MG"
        self.mocked_journal.publisher_city = "Uberaba"
        self.mocked_journal.mission = {}
        self.mocked_journal.status_history = [
            ('date', 'status', 'reason'),
        ]
        self.mocked_journal.sponsors = []

    def test_journal_as_kernel_returns_name_of_institution_responsible_for(self):
        mocked_journal = self.mocked_journal
        result = journal_as_kernel(mocked_journal)
        self.assertEqual(
            "Casa Publicadora",
            result["institution_responsible_for"][0]["name"])

    def test_journal_as_kernel_returns_city_of_institution_responsible_for(self):
        mocked_journal = self.mocked_journal
        result = journal_as_kernel(mocked_journal)
        self.assertEqual(
            "Uberaba",
            result["institution_responsible_for"][0]["city"])

    def test_journal_as_kernel_returns_state_of_institution_responsible_for(self):
        mocked_journal = self.mocked_journal
        result = journal_as_kernel(mocked_journal)
        self.assertEqual(
            "MG",
            result["institution_responsible_for"][0]["state"])

    def test_journal_as_kernel_returns_country_of_institution_responsible_for(self):
        mocked_journal = self.mocked_journal
        result = journal_as_kernel(mocked_journal)
        self.assertEqual(
            "Brasil",
            result["institution_responsible_for"][0]["country"])
