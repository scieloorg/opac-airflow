import os
import json
from copy import deepcopy
import http.client
import unittest
from unittest import mock

from airflow import DAG

from kernel_gate import mount_journals_issues_link
from .test_kernel_changes import load_json_fixture

FIXTURES_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures")


class UpdateJournalAndIssueLink(unittest.TestCase):
    def setUp(self):
        self.issues = load_json_fixture("isis-issues.json")

    def test_mount_journals_issues_link(self):
        journals_issues_link = mount_journals_issues_link(self.issues)
        self.assertEqual(journals_issues_link["1678-4464"], ["1678-4464-2018-v1-n1"])

    def test_mount_journals_issues_link_should_not_contains_duplicates_issues_ids(self):
        issues = self.issues * 3
        journals_issues_link = mount_journals_issues_link(issues)

        self.assertEqual(journals_issues_link["1678-4464"], ["1678-4464-2018-v1-n1"])
        self.assertEqual(len(journals_issues_link["1678-4464"]), 1)

    def test_mount_journals_issues_link_should_not_contains_pressrelease(self):
        self.issues[-1]["v31"][0]["_"] = "pr"
        journals_issues_link = mount_journals_issues_link(self.issues)

        self.assertDictEqual(journals_issues_link, {})

    def test_mount_journals_issues_link_should_not_contains_ahead_of_print(self):
        self.issues[-1]["v31"][0]["_"] = "ahead"
        journals_issues_link = mount_journals_issues_link(self.issues)

        self.assertDictEqual(journals_issues_link, {})
