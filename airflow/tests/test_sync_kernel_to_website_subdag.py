import unittest


from subdags.sync_kernel_to_website_subdag import (
    _group_documents_by_bundle,
)


class TestGroupDocumentsByBundle(unittest.TestCase):

    def mock_get_relation_data(self, doc_id):
        data = {
            "RCgFV9MHSKmp6Msj5CPBZRb": (
                "issue_id",
                {"id": "RCgFV9MHSKmp6Msj5CPBZRb", "order": "00602"},
            ),
            "CGgFV9MHSKmp6Msj5CPBZRb": (
                "issue_id",
                {"id": "CGgFV9MHSKmp6Msj5CPBZRb", "order": "00604"},
            ),
            "HJgFV9MHSKmp6Msj5CPBZRb": (
                "issue_id",
                {"id": "HJgFV9MHSKmp6Msj5CPBZRb", "order": "00607"},
            ),
            "LLgFV9MHSKmp6Msj5CPBZRb": (
                "issue_id",
                {"id": "LLgFV9MHSKmp6Msj5CPBZRb", "order": "00609"},
            ),
            "RC13V9MHSKmp6Msj5CPBZRb": (
                "issue_id_2",
                {"id": "RC13V9MHSKmp6Msj5CPBZRb", "order": "00602"},
            ),
            "CG13V9MHSKmp6Msj5CPBZRb": (
                "issue_id_2",
                {"id": "CG13V9MHSKmp6Msj5CPBZRb", "order": "00604"},
            ),
            "HJ13V9MHSKmp6Msj5CPBZRb": (
                "issue_id_2",
                {"id": "HJ13V9MHSKmp6Msj5CPBZRb", "order": "00607"},
            ),
            "LL13V9MHSKmp6Msj5CPBZRb": (
                "issue_id_2",
                {"id": "LL13V9MHSKmp6Msj5CPBZRb", "order": "00609"},
            ),
        }
        return data.get(doc_id)

    def test__group_documents_by_bundle(self):
        document_ids = (
            "LL13V9MHSKmp6Msj5CPBZRb",
            "HJgFV9MHSKmp6Msj5CPBZRb",
            "CGgFV9MHSKmp6Msj5CPBZRb",
        )

        expected = {
            "issue_id_2": ["LL13V9MHSKmp6Msj5CPBZRb"],
            "issue_id": ["HJgFV9MHSKmp6Msj5CPBZRb", "CGgFV9MHSKmp6Msj5CPBZRb"],
        }
        result = _group_documents_by_bundle(
            document_ids, self.mock_get_relation_data)
        self.assertEqual(expected, result)

    def test__group_documents_by_bundle_returns_empty_dict(self):
        def mock_get_relation_data(doc_id):
            return (None, {})

        document_ids = (
            "XXX3V9MHSKmp6Msj5CPBZRb",
        )
        expected = {}
        result = _group_documents_by_bundle(
            document_ids, mock_get_relation_data)
        self.assertEqual(expected, result)
