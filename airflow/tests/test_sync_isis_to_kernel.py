import os
import json
import unittest
import tempfile

from airflow import DAG
from xylose.scielodocument import Issue

from dags.sync_isis_to_kernel import (
    mount_journals_issues_link,
    issue_data_to_link,
    create_journal_issn_index
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
                data[field] = [{u'_': value}]
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
                    'id': '1678-4464-2018-v1-n1',
                    'order': '1001',
                    'year': '2018',
                    'volume': '1',
                    'number': '1'
                }
            ]
        )

    def test_mount_journals_issues_link_should_not_contains_duplicates_issues_ids(self):
        issues = self.issues * 3
        journals_issues_link = mount_journals_issues_link(issues)

        self.assertEqual(
            journals_issues_link["1678-4464"],
            [
                {
                    'id': '1678-4464-2018-v1-n1',
                    'order': '1001',
                    'year': '2018',
                    'volume': '1',
                    'number': '1'
                }
            ]
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
            {
                "scielo_issn": "0004-000X",
                "electronic_issn": "0004-000X",
            },
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
