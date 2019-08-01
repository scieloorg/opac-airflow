import os
import unittest
import json

from airflow import DAG

from kernel_changes import JournalFactory


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
