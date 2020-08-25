from unittest import TestCase
from unittest.mock import patch, call

from airflow import DAG

from operations.check_website_operations import (
    concat_website_url_and_uri_list_items,
    check_uri_list,
    check_website_uri_list,
    get_webpage_href_and_src,
    not_found_expected_uri_items_in_web_page,
    get_document_webpage_uri,
    get_document_webpage_uri_list,
    check_uri_items_expected_in_webpage,
    check_document_uri_items,
    check_document_html,
    check_document_assets_availability,
)


class TestConcatWebsiteUrlAndUriListItems(TestCase):

    def test_concat_website_url_and_uri_list_items_for_none_website_url_and_none_uri_list_returns_empty_list(self):
        items = concat_website_url_and_uri_list_items(None, None)
        self.assertEqual([], items)

    def test_concat_website_url_and_uri_list_items_for_none_website_url_returns_empty_list(self):
        items = concat_website_url_and_uri_list_items(None, ['uri'])
        self.assertEqual([], items)

    def test_concat_website_url_and_uri_list_items_for_none_uri_list_returns_empty_list(self):
        items = concat_website_url_and_uri_list_items(['website'], None)
        self.assertEqual([], items)

    def test_concat_website_url_and_uri_list_items_returns_list(self):
        items = concat_website_url_and_uri_list_items(
            ['website1', 'website2'],
            ['/uri1', '/uri2'])
        self.assertEqual(
            ['website1/uri1',
             'website1/uri2',
             'website2/uri1',
             'website2/uri2', ],
            items)


class MockResponse:

    def __init__(self, code):
        self.status_code = code


class MockLogger:

    def __init__(self):
        self._info = []
        self._debug = []

    def info(self, msg):
        self._info.append(msg)

    def debug(self, msg):
        self._debug.append(msg)


class TestCheckUriList(TestCase):

    @patch('operations.check_website_operations.requests.head')
    def test_check_uri_list_for_status_code_200_returns_empty_list(self, mock_req_head):
        mock_req_head.side_effect = [MockResponse(200), MockResponse(200), ]
        uri_list = ["goodURI1", "goodURI2", ]
        result = check_uri_list(uri_list)
        self.assertEqual([], result)

    @patch('operations.check_website_operations.requests.head')
    def test_check_uri_list_for_status_code_301_returns_empty_list(self, mock_req_head):
        mock_req_head.side_effect = [MockResponse(301)]
        uri_list = ["URI"]
        result = check_uri_list(uri_list)
        self.assertEqual([], result)

    @patch('operations.check_website_operations.requests.head')
    def test_check_uri_list_for_status_code_302_returns_empty_list(self, mock_req_head):
        mock_req_head.side_effect = [MockResponse(302)]
        uri_list = ["URI"]
        result = check_uri_list(uri_list)
        self.assertEqual([], result)

    @patch('operations.check_website_operations.requests.head')
    def test_check_uri_list_for_status_code_404_returns_failure_list(self, mock_req_head):
        mock_req_head.side_effect = [MockResponse(404)]
        uri_list = ["BAD_URI"]
        result = check_uri_list(uri_list)
        self.assertEqual(
            uri_list,
            result)

    @patch('operations.check_website_operations.requests.head')
    def test_check_uri_list_for_status_code_429_returns_failure_list(self, mock_req_head):
        mock_req_head.side_effect = [MockResponse(429), MockResponse(404)]
        uri_list = ["BAD_URI"]
        result = check_uri_list(uri_list)
        self.assertEqual(
            uri_list,
            result)

    @patch('operations.check_website_operations.retry_after')
    @patch('operations.check_website_operations.requests.head')
    def test_check_uri_list_for_status_code_200_after_retries_returns_failure_list(self, mock_req_head, mock_retry_after):
        mock_retry_after.return_value = [
            0.1, 0.2, 0.4, 0.8, 1,
        ]
        mock_req_head.side_effect = [
            MockResponse(429),
            MockResponse(502),
            MockResponse(503),
            MockResponse(504),
            MockResponse(500),
            MockResponse(200),
        ]
        uri_list = ["GOOD_URI"]
        result = check_uri_list(uri_list)
        self.assertEqual([], result)

    @patch('operations.check_website_operations.retry_after')
    @patch('operations.check_website_operations.requests.head')
    def test_check_uri_list_for_status_code_404_after_retries_returns_failure_list(self, mock_req_head, mock_retry_after):
        mock_retry_after.return_value = [
            0.1, 0.2, 0.4, 0.8, 1,
        ]
        mock_req_head.side_effect = [
            MockResponse(429),
            MockResponse(502),
            MockResponse(404),
        ]
        uri_list = ["BAD_URI"]
        result = check_uri_list(uri_list)
        self.assertEqual(["BAD_URI"], result)


class TestCheckWebsiteUriList(TestCase):

    def test_check_website_uri_list_raises_value_error_because_website_urls_are_missing(self):
        with self.assertRaises(ValueError):
            check_website_uri_list('/path/uri_list_file_path.lst', [])

    @patch("operations.check_website_operations.Logger.info")
    @patch("operations.check_website_operations.read_file")
    def test_check_website_uri_list_informs_zero_uri(self, mock_read_file, mock_info):
        mock_read_file.return_value = []
        uri_list_file_path = "/tmp/uri_list_2010-10-09.lst"
        website_url_list = ["http://www.scielo.br", "https://newscielo.br"]
        check_website_uri_list(uri_list_file_path, website_url_list)
        self.assertEqual(
            mock_info.call_args_list,
            [
                call('Quantidade de URI: %i', 0),
                call("Encontrados: %i/%i", 0, 0),
            ]
        )

    @patch("operations.check_website_operations.Logger.info")
    @patch("operations.check_website_operations.requests.head")
    @patch("operations.check_website_operations.read_file")
    def test_check_website_uri_list_informs_that_all_were_found(self, mock_read_file, mock_head, mock_info):
        mock_read_file.return_value = (
            "/scielo.php?script=sci_serial&pid=0001-3765\n"
            "/scielo.php?script=sci_issues&pid=0001-3765\n"
            "/scielo.php?script=sci_issuetoc&pid=0001-376520200005\n"
            "/scielo.php?script=sci_arttext&pid=S0001-37652020000501101\n"
        ).split()
        mock_head.side_effect = [
            MockResponse(200),
            MockResponse(200),
            MockResponse(200),
            MockResponse(200),
            MockResponse(200),
            MockResponse(200),
            MockResponse(200),
            MockResponse(200),
        ]
        uri_list_file_path = "/tmp/uri_list_2010-10-09.lst"
        website_url_list = ["http://www.scielo.br", "https://newscielo.br"]
        check_website_uri_list(uri_list_file_path, website_url_list)
        self.assertEqual(
            mock_info.call_args_list,
            [
                call('Quantidade de URI: %i', 8),
                call("Encontrados: %i/%i", 8, 8),
            ]
        )

    @patch("operations.check_website_operations.Logger.info")
    @patch("operations.check_website_operations.requests.head")
    @patch("operations.check_website_operations.read_file")
    def test_check_website_uri_list_informs_that_some_of_uri_items_were_not_found(self, mock_read_file, mock_head, mock_info):
        mock_read_file.return_value = (
            "/scielo.php?script=sci_serial&pid=0001-3765\n"
            "/scielo.php?script=sci_issues&pid=0001-3765\n"
            "/scielo.php?script=sci_issuetoc&pid=0001-376520200005\n"
            "/scielo.php?script=sci_arttext&pid=S0001-37652020000501101"
        ).split()
        mock_head.side_effect = [
            MockResponse(200),
            MockResponse(404),
            MockResponse(200),
            MockResponse(200),
            MockResponse(500),
            MockResponse(404),
            MockResponse(200),
            MockResponse(200),
            MockResponse(200),
            MockResponse(200),
            MockResponse(200),
        ]
        uri_list_file_path = "/tmp/uri_list_2010-10-09.lst"
        website_url_list = ["http://www.scielo.br", "https://newscielo.br"]
        check_website_uri_list(uri_list_file_path, website_url_list)
        bad_uri_1 = "http://www.scielo.br/scielo.php?script=sci_issues&pid=0001-3765"
        bad_uri_2 = "https://newscielo.br/scielo.php?script=sci_serial&pid=0001-3765"

        self.assertEqual(
            mock_info.call_args_list,
            [
                call('Quantidade de URI: %i', 8),
                call("Retry to access '%s' after %is", bad_uri_2, 5),
                call("The URL '%s' returned the status code '%s' after %is",
                     bad_uri_2, 404, 5),
                call("Não encontrados (%i/%i):\n%s", 2, 8,
                     "\n".join([
                        bad_uri_1,
                        bad_uri_2,
                    ])),
            ]
        )


class TestGetWebpageHrefAndSrc(TestCase):

    def test_get_webpage_href_and_src_returns_src_and_href(self):
        content = """<root>
            <img src="bla.jpg"/>
            <p><x src="g.jpg"/></p>
            <a href="d.jpg"/>
            </root>"""
        expected = {
            "href": ["d.jpg"],
            "src": ["bla.jpg", "g.jpg"],
        }
        result = get_webpage_href_and_src(content)
        self.assertEqual(expected, result)

    def test_get_webpage_href_and_src_returns_src(self):
        content = """<root>
            <p><img src="bla.jpg"/></p><x src="g.jpg"/>
            </root>"""
        expected = {
            "href": [],
            "src": ["bla.jpg", "g.jpg"],
        }
        result = get_webpage_href_and_src(content)
        self.assertEqual(expected, result)

    def test_get_webpage_href_and_src_returns_href(self):
        content = '<root><a href="d.jpg"/></root>'
        expected = {
            "href": ["d.jpg"],
            "src": [],
        }
        result = get_webpage_href_and_src(content)
        self.assertEqual(expected, result)


class TestNotFoundExpectedUriItemsInWebPage(TestCase):

    def test_not_found_expected_uri_items_in_web_page_returns_empty_set(self):
        expected_uri_items = [
            "a.png",
            "b.png",
        ]
        web_page_uri_items = [
            "a.png",
            "b.png",
        ]
        result = not_found_expected_uri_items_in_web_page(
            expected_uri_items, web_page_uri_items)
        self.assertEqual(set(), result)

    def test_not_found_expected_uri_items_in_web_page_returns_a_b(self):
        expected_uri_items = [
            "a.png",
            "b.png",
        ]
        web_page_uri_items = [
            "x.png",
            "y.png",
        ]
        result = not_found_expected_uri_items_in_web_page(
            expected_uri_items, web_page_uri_items)
        self.assertEqual({"a.png", "b.png"}, result)

    def test_not_found_expected_uri_items_in_web_page_returns_b(self):
        expected_uri_items = [
            "a.png",
            "b.png",
        ]
        web_page_uri_items = [
            "a.png",
            "y.png",
        ]
        result = not_found_expected_uri_items_in_web_page(
            expected_uri_items, web_page_uri_items)
        self.assertEqual({"b.png"}, result)


class TestGetDocumentUri(TestCase):

    def test_get_document_webpage_uri_returns_uri_with_all_the_parameters(self):
        data = {
            "acron": "abcdef", "doc_id": "klamciekdoalei", "format": "x",
            "lang": "vv"
        }
        expected = "/j/abcdef/a/klamciekdoalei?format=x&lang=vv"
        result = get_document_webpage_uri(data)
        self.assertEqual(expected, result)

    def test_get_document_webpage_uri_returns_uri_without_lang(self):
        data = {
            "acron": "abcdef", "doc_id": "klamciekdoalei", "format": "x",
        }
        expected = "/j/abcdef/a/klamciekdoalei?format=x"
        result = get_document_webpage_uri(data)
        self.assertEqual(expected, result)

    def test_get_document_webpage_uri_returns_uri_without_format(self):
        data = {
            "acron": "abcdef", "doc_id": "klamciekdoalei",
            "lang": "vv"
        }
        expected = "/j/abcdef/a/klamciekdoalei?lang=vv"
        result = get_document_webpage_uri(data)
        self.assertEqual(expected, result)

    def test_get_document_webpage_uri_returns_uri_without_format_and_without_lang(self):
        data = {
            "acron": "abcdef", "doc_id": "klamciekdoalei",
        }
        expected = "/j/abcdef/a/klamciekdoalei"
        result = get_document_webpage_uri(data)
        self.assertEqual(expected, result)


class TestGetDocumentWebpageUriList(TestCase):

    def test_get_document_webpage_uri_list_returns_uri_data_list_using_new_pattern(self):
        doc_id = "ldld"
        doc_data_list = [
            {"lang": "en", "format": "html", "pid_v2": "pid-v2",
             "acron": "xjk", "doc_id_for_human": "artigo-1234"},
            {"lang": "en", "format": "pdf", "pid_v2": "pid-v2",
             "acron": "xjk", "doc_id_for_human": "artigo-1234"},
            {"lang": "es", "format": "html", "pid_v2": "pid-v2",
             "acron": "xjk", "doc_id_for_human": "artigo-1234"},
            {"lang": "es", "format": "pdf", "pid_v2": "pid-v2",
             "acron": "xjk", "doc_id_for_human": "artigo-1234"},
        ]
        expected = [
            {
                "lang": "en",
                "format": "html",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "/j/xjk/a/ldld?format=html&lang=en",
            },
            {
                "lang": "en",
                "format": "pdf",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "/j/xjk/a/ldld?format=pdf&lang=en",
            },
            {
                "lang": "es",
                "format": "html",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "/j/xjk/a/ldld?format=html&lang=es",
            },
            {
                "lang": "es",
                "format": "pdf",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "/j/xjk/a/ldld?format=pdf&lang=es",
            },
        ]
        result = get_document_webpage_uri_list(doc_id, doc_data_list, get_document_webpage_uri)
        self.assertEqual(expected, result)

    def test_get_document_webpage_uri_list_raises_value_error_if_acron_is_none_and_using_new_uri_pattern(self):
        doc_id = "ldld"
        with self.assertRaises(ValueError):
            get_document_webpage_uri_list(doc_id, [], get_document_webpage_uri)

    def test_get_document_webpage_uri_list_raises_value_error_if_acron_is_empty_str_and_using_new_uri_pattern(self):
        doc_id = "ldld"
        with self.assertRaises(ValueError):
            get_document_webpage_uri_list(doc_id, [], get_document_webpage_uri)

    def test_get_document_webpage_uri_list_returns_uri_data_list_using_classic_pattern(self):
        doc_id = "S1234-56782000123412313"
        doc_data_list = [
            {"lang": "en", "format": "html",
             "pid_v2": "S1234-56782000123412313",
             "acron": "xjk", "doc_id_for_human": "artigo-1234"},
            {"lang": "en", "format": "pdf",
             "pid_v2": "S1234-56782000123412313",
             "acron": "xjk", "doc_id_for_human": "artigo-1234"},
            {"lang": "es", "format": "html",
             "pid_v2": "S1234-56782000123412313",
             "acron": "xjk", "doc_id_for_human": "artigo-1234"},
            {"lang": "es", "format": "pdf",
             "pid_v2": "S1234-56782000123412313",
             "acron": "xjk", "doc_id_for_human": "artigo-1234"},
        ]
        expected = [
            {
                "lang": "en",
                "format": "html",
                "pid_v2": "S1234-56782000123412313",
                "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "S1234-56782000123412313",
                "uri": "/scielo.php?script=sci_arttext&pid=S1234-56782000123412313&tlng=en",
            },
            {
                "lang": "en",
                "format": "pdf",
                "pid_v2": "S1234-56782000123412313",
                "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "S1234-56782000123412313",
                "uri": "/scielo.php?script=sci_pdf&pid=S1234-56782000123412313&tlng=en",
            },
            {
                "lang": "es",
                "format": "html",
                "pid_v2": "S1234-56782000123412313",
                "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "S1234-56782000123412313",
                "uri": "/scielo.php?script=sci_arttext&pid=S1234-56782000123412313&tlng=es",
            },
            {
                "lang": "es",
                "format": "pdf",
                "pid_v2": "S1234-56782000123412313",
                "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "S1234-56782000123412313",
                "uri": "/scielo.php?script=sci_pdf&pid=S1234-56782000123412313&tlng=es",
            },
        ]
        result = get_document_webpage_uri_list(doc_id, doc_data_list)
        self.assertEqual(expected, result)


class TestCheckWebpageInnerUriList(TestCase):

    def test_check_uri_items_expected_in_webpage_returns_success(self):
        uri_items_expected_in_webpage = [
            "asset_uri_1.jpg",
            "asset_uri_2.jpg",
            "/j/xyz/a/lokiujyht?format=html&lang=en",
            "/j/xyz/a/lokiujyht?format=pdf&lang=es",
            "/j/xyz/a/lokiujyht?format=pdf&lang=en",
        ]
        assets_data = [
            {
                "prefix": "asset_uri_1",
                "uri_alternatives": [
                    "asset_uri_1.tiff", "asset_uri_1.jpg", "asset_uri_1.png"]
            },
            {
                "prefix": "asset_uri_2",
                "uri_alternatives": [
                    "asset_uri_2.tiff", "asset_uri_2.jpg", "asset_uri_2.png"]
            }
        ]
        other_versions_uri_data = [
            {
                "lang": "en",
                "format": "html",
                "acron": "xyz",
                "doc_id": "lokiujyht",
            },
            {
                "lang": "es",
                "format": "pdf",
                "acron": "xyz",
                "doc_id": "lokiujyht",
            },
            {
                "lang": "en",
                "format": "pdf",
                "acron": "xyz",
                "doc_id": "lokiujyht",
            },
        ]
        expected = [
            {
                "type": "asset",
                "id": "asset_uri_1",
                "present_in_html": True,
                "uri": "asset_uri_1.jpg",
            },
            {
                "type": "asset",
                "id": "asset_uri_2",
                "present_in_html": True,
                "uri": "asset_uri_2.jpg",
            },
            {
                "type": "html",
                "id": "en",
                "present_in_html": True,
                "uri": "/j/xyz/a/lokiujyht?format=html&lang=en",
            },
            {
                "type": "pdf",
                "id": "es",
                "present_in_html": True,
                "uri": "/j/xyz/a/lokiujyht?format=pdf&lang=es",
            },
            {
                "type": "pdf",
                "id": "en",
                "present_in_html": True,
                "uri": "/j/xyz/a/lokiujyht?format=pdf&lang=en",
            },
        ]
        result = check_uri_items_expected_in_webpage(uri_items_expected_in_webpage,
                    assets_data, other_versions_uri_data)
        self.assertEqual(expected, result)

    def test_check_uri_items_expected_in_webpage_returns_not_found_asset_uri(self):
        uri_items_expected_in_webpage = [
            "asset_uri_1.jpg",
        ]
        assets_data = [
            {
                "prefix": "asset_uri_1",
                "uri_alternatives": [
                    "asset_uri_1.tiff", "asset_uri_1.jpg", "asset_uri_1.png"]
            },
            {
                "prefix": "asset_uri_2",
                "uri_alternatives": [
                    "asset_uri_2.tiff", "asset_uri_2.jpg", "asset_uri_2.png"]
            }
        ]
        other_versions_uri_data = [
        ]
        expected = [
            {
                "type": "asset",
                "id": "asset_uri_1",
                "present_in_html": True,
                "uri": "asset_uri_1.jpg",
            },
            {
                "type": "asset",
                "id": "asset_uri_2",
                "present_in_html": False,
                "uri": [
                    "asset_uri_2.tiff", "asset_uri_2.jpg", "asset_uri_2.png"],
            },
        ]
        result = check_uri_items_expected_in_webpage(uri_items_expected_in_webpage,
                    assets_data, other_versions_uri_data)
        self.assertEqual(expected, result)

    def test_check_uri_items_expected_in_webpage_returns_not_found_pdf(self):
        uri_items_expected_in_webpage = [
            "/j/xyz/a/lokiujyht?format=html&lang=en",
            "/j/xyz/a/lokiujyht?format=pdf&lang=en",
        ]
        assets_data = [
        ]
        other_versions_uri_data = [
            {
                "lang": "en",
                "format": "html",
                "acron": "xyz",
                "doc_id": "lokiujyht",
            },
            {
                "lang": "es",
                "format": "pdf",
                "acron": "xyz",
                "doc_id": "lokiujyht",
            },
            {
                "lang": "en",
                "format": "pdf",
                "acron": "xyz",
                "doc_id": "lokiujyht",
            },
        ]
        expected = [
            {
                "type": "html",
                "id": "en",
                "present_in_html": True,
                "uri": "/j/xyz/a/lokiujyht?format=html&lang=en",
            },
            {
                "type": "pdf",
                "id": "es",
                "present_in_html": False,
                "uri": [
                        "/j/xyz/a/lokiujyht?format=pdf&lang=es",
                        "/j/xyz/a/lokiujyht?lang=es&format=pdf",
                        "/j/xyz/a/lokiujyht?format=pdf",
                        "/j/xyz/a/lokiujyht/?format=pdf&lang=es",
                        "/j/xyz/a/lokiujyht/?lang=es&format=pdf",
                        "/j/xyz/a/lokiujyht/?format=pdf",
                    ]
            },
            {
                "type": "pdf",
                "id": "en",
                "present_in_html": True,
                "uri": "/j/xyz/a/lokiujyht?format=pdf&lang=en",
            },
        ]
        result = check_uri_items_expected_in_webpage(
                    uri_items_expected_in_webpage,
                    assets_data, other_versions_uri_data)
        self.assertEqual(expected, result)

    def test_check_uri_items_expected_in_webpage_returns_not_found_html(self):
        uri_items_expected_in_webpage = [
            "/j/xyz/a/lokiujyht?format=pdf&lang=es",
            "/j/xyz/a/lokiujyht?format=pdf&lang=en",
        ]
        assets_data = [
        ]
        other_versions_uri_data = [
            {
                "lang": "en",
                "format": "html",
                "acron": "xyz",
                "doc_id": "lokiujyht",
            },
            {
                "lang": "es",
                "format": "pdf",
                "acron": "xyz",
                "doc_id": "lokiujyht",
            },
            {
                "lang": "en",
                "format": "pdf",
                "acron": "xyz",
                "doc_id": "lokiujyht",
            },
        ]
        expected = [
            {
                "type": "html",
                "id": "en",
                "present_in_html": False,
                "uri": [
                        "/j/xyz/a/lokiujyht?format=html&lang=en",
                        "/j/xyz/a/lokiujyht?lang=en&format=html",
                        "/j/xyz/a/lokiujyht?format=html",
                        "/j/xyz/a/lokiujyht?lang=en",
                        "/j/xyz/a/lokiujyht/?format=html&lang=en",
                        "/j/xyz/a/lokiujyht/?lang=en&format=html",
                        "/j/xyz/a/lokiujyht/?format=html",
                        "/j/xyz/a/lokiujyht/?lang=en",
                    ],
            },
            {
                "type": "pdf",
                "id": "es",
                "present_in_html": True,
                "uri": "/j/xyz/a/lokiujyht?format=pdf&lang=es",
            },
            {
                "type": "pdf",
                "id": "en",
                "present_in_html": True,
                "uri": "/j/xyz/a/lokiujyht?format=pdf&lang=en",
            },
        ]
        result = check_uri_items_expected_in_webpage(uri_items_expected_in_webpage,
                    assets_data, other_versions_uri_data)
        self.assertEqual(expected, result)


class TestCheckDocumentUriItems(TestCase):

    @patch("operations.check_website_operations.get_webpage_content")
    @patch("operations.check_website_operations.access_uri")
    def test_check_document_uri_items_returns_success(self, mock_access_uri,
                                                    mock_get_webpage_content):
        website_url = "https://www.scielo.br"
        doc_data_list = [
            {
                "lang": "en",
                "format": "html",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "/j/xjk/a/ldld?format=html&lang=en",
            },
            {
                "lang": "en",
                "format": "pdf",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "/j/xjk/a/ldld?format=pdf&lang=en",
            },
            {
                "lang": "es",
                "format": "html",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "/j/xjk/a/ldld?format=html&lang=es",
            },
            {
                "lang": "es",
                "format": "pdf",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "/j/xjk/a/ldld?format=pdf&lang=es",
            },
        ]
        assets_data = [
            {
                "prefix": "asset_uri_1",
                "uri_alternatives": [
                    "asset_uri_1.tiff", "asset_uri_1.jpg", "asset_uri_1.png"]
            },
            {
                "prefix": "asset_uri_2",
                "uri_alternatives": [
                    "asset_uri_2.tiff", "asset_uri_2.jpg", "asset_uri_2.png"]
            }
        ]
        mock_get_webpage_content.side_effect = [
            """
            <a href="asset_uri_2.jpg"/>
            <a href="asset_uri_1.jpg"/>
            <a href="/j/xjk/a/ldld?format=pdf&lang=es"/>
            <a href="/j/xjk/a/ldld?format=pdf&lang=en"/>
            <a href="/j/xjk/a/ldld?format=html&lang=es"/>
            """,
            """
            <a href="asset_uri_2.jpg"/>
            <a href="asset_uri_1.jpg"/>
            <a href="/j/xjk/a/ldld?format=pdf&lang=es"/>
            <a href="/j/xjk/a/ldld?format=pdf&lang=en"/>
            <a href="/j/xjk/a/ldld?format=html&lang=en"/>
            """,
        ]
        mock_access_uri.side_effect = [True, True]
        expected = [
            {
                "lang": "en",
                "format": "html",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "https://www.scielo.br/j/xjk/a/ldld?format=html&lang=en",
                "available": True,
                "components": [
                    {
                        "type": "asset",
                        "id": "asset_uri_1",
                        "present_in_html": True,
                        "uri": "asset_uri_1.jpg",
                    },
                    {
                        "type": "asset",
                        "id": "asset_uri_2",
                        "present_in_html": True,
                        "uri": "asset_uri_2.jpg",
                    },
                    {
                        "type": "pdf",
                        "id": "en",
                        "present_in_html": True,
                        "uri": "/j/xjk/a/ldld?format=pdf&lang=en",
                    },
                    {
                        "type": "html",
                        "id": "es",
                        "present_in_html": True,
                        "uri": "/j/xjk/a/ldld?format=html&lang=es",
                    },
                    {
                        "type": "pdf",
                        "id": "es",
                        "present_in_html": True,
                        "uri": "/j/xjk/a/ldld?format=pdf&lang=es",
                    },
                ]
            },
            {
                "lang": "en",
                "format": "pdf",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "https://www.scielo.br/j/xjk/a/ldld?format=pdf&lang=en",
                "available": True,
            },
            {
                "lang": "es",
                "format": "html",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "https://www.scielo.br/j/xjk/a/ldld?format=html&lang=es",
                "available": True,
                "components": [
                    {
                        "type": "asset",
                        "id": "asset_uri_1",
                        "present_in_html": True,
                        "uri": "asset_uri_1.jpg",
                    },
                    {
                        "type": "asset",
                        "id": "asset_uri_2",
                        "present_in_html": True,
                        "uri": "asset_uri_2.jpg",
                    },
                    {
                        "type": "html",
                        "id": "en",
                        "present_in_html": True,
                        "uri": "/j/xjk/a/ldld?format=html&lang=en",
                    },
                    {
                        "type": "pdf",
                        "id": "en",
                        "present_in_html": True,
                        "uri": "/j/xjk/a/ldld?format=pdf&lang=en",
                    },
                    {
                        "type": "pdf",
                        "id": "es",
                        "present_in_html": True,
                        "uri": "/j/xjk/a/ldld?format=pdf&lang=es",
                    },
                ]
            },
            {
                "lang": "es",
                "format": "pdf",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "https://www.scielo.br/j/xjk/a/ldld?format=pdf&lang=es",
                "available": True,
            },
        ]
        result = check_document_uri_items(website_url, doc_data_list, assets_data)
        self.assertListEqual(expected, result)

    @patch("operations.check_website_operations.get_webpage_content")
    @patch("operations.check_website_operations.access_uri")
    def test_check_document_uri_items_returns_pdf_is_not_available_although_it_is_present_in_html(self, mock_access_uri,
                                                    mock_get_webpage_content):
        website_url = "https://www.scielo.br"
        doc_data_list = [
            {
                "lang": "en",
                "format": "html",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "/j/xjk/a/ldld?format=html&lang=en",
            },
            {
                "lang": "en",
                "format": "pdf",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "/j/xjk/a/ldld?format=pdf&lang=en",
            },
        ]
        assets_data = [
        ]
        mock_get_webpage_content.return_value = """
        <a href="/j/xjk/a/ldld?format=pdf&lang=en"/>
        """
        mock_access_uri.return_value = False
        expected = [
            {
                "lang": "en",
                "format": "html",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "https://www.scielo.br/j/xjk/a/ldld?format=html&lang=en",
                "available": True,
                "components": [
                    {
                        "type": "pdf",
                        "id": "en",
                        "present_in_html": True,
                        "uri": "/j/xjk/a/ldld?format=pdf&lang=en",
                    },
                ]
            },
            {
                "lang": "en",
                "format": "pdf",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "https://www.scielo.br/j/xjk/a/ldld?format=pdf&lang=en",
                "available": False,
            },
        ]
        result = check_document_uri_items(website_url, doc_data_list, assets_data)
        self.assertListEqual(expected, result)

    @patch("operations.check_website_operations.get_webpage_content")
    @patch("operations.check_website_operations.access_uri")
    def test_check_document_uri_items_returns_pdf_is_available_although_it_is_not_present_in_html(self, mock_access_uri,
                                                    mock_get_webpage_content):
        website_url = "https://www.scielo.br"
        doc_data_list = [
            {
                "lang": "en",
                "format": "html",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "/j/xjk/a/ldld?format=html&lang=en",
            },
            {
                "lang": "en",
                "format": "pdf",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "/j/xjk/a/ldld?format=pdf&lang=en",
            },
        ]
        assets_data = [
        ]
        mock_get_webpage_content.return_value = """
         """
        mock_access_uri.return_value = True
        expected = [
            {
                "lang": "en",
                "format": "html",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "https://www.scielo.br/j/xjk/a/ldld?format=html&lang=en",
                "available": True,
                "components": [
                    {
                        "type": "pdf",
                        "id": "en",
                        "present_in_html": False,
                        "uri": [
                            "/j/xjk/a/ldld?format=pdf&lang=en",
                            "/j/xjk/a/ldld?lang=en&format=pdf",
                            "/j/xjk/a/ldld?format=pdf",
                            "/j/xjk/a/ldld/?format=pdf&lang=en",
                            "/j/xjk/a/ldld/?lang=en&format=pdf",
                            "/j/xjk/a/ldld/?format=pdf",
                        ],
                    },
                ]
            },
            {
                "lang": "en",
                "format": "pdf",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "https://www.scielo.br/j/xjk/a/ldld?format=pdf&lang=en",
                "available": True,
            },
        ]
        result = check_document_uri_items(website_url, doc_data_list, assets_data)
        self.assertListEqual(expected, result)

    @patch("operations.check_website_operations.get_webpage_content")
    @patch("operations.check_website_operations.access_uri")
    def test_check_document_uri_items_returns_html_es_is_not_available_although_it_is_present_in_html_en(self, mock_access_uri,
                                                    mock_get_webpage_content):
        website_url = "https://www.scielo.br"
        doc_data_list = [
            {
                "lang": "en",
                "format": "html",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "/j/xjk/a/ldld?format=html&lang=en",
            },
            {
                "lang": "es",
                "format": "html",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "/j/xjk/a/ldld?format=html&lang=es",
            },
        ]
        assets_data = [
        ]
        mock_get_webpage_content.side_effect = [
            """
            Versão ingles no formato html
            <a href="/j/xjk/a/ldld?format=html&lang=es"/>
            """,
            None,
        ]
        mock_access_uri.return_value = False
        expected = [
            {
                "lang": "en",
                "format": "html",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "https://www.scielo.br/j/xjk/a/ldld?format=html&lang=en",
                "available": True,
                "components": [
                    {
                        "type": "html",
                        "id": "es",
                        "present_in_html": True,
                        "uri": "/j/xjk/a/ldld?format=html&lang=es",
                    },
                ]
            },
            {
                "lang": "es",
                "format": "html",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "https://www.scielo.br/j/xjk/a/ldld?format=html&lang=es",
                "available": False,
            },
        ]
        result = check_document_uri_items(website_url, doc_data_list, assets_data)
        self.assertListEqual(expected, result)

    @patch("operations.check_website_operations.get_webpage_content")
    @patch("operations.check_website_operations.access_uri")
    def test_check_document_uri_items_returns_html_es_is_available_although_it_is_not_present_in_html_en(self, mock_access_uri,
                                                    mock_get_webpage_content):
        website_url = "https://www.scielo.br"
        doc_data_list = [
            {
                "lang": "en",
                "format": "html",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "/j/xjk/a/ldld?format=html&lang=en",
            },
            {
                "lang": "es",
                "format": "html",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "/j/xjk/a/ldld?format=html&lang=es",
            },
        ]
        assets_data = [
        ]
        mock_get_webpage_content.side_effect = [
            "documento sem links, conteúdo do html em Ingles",
            """
            conteúdo do documento em espanhol com link para a versão ingles
                <a href="/j/xjk/a/ldld?format=html"/>
            """,
        ]

        mock_access_uri.return_value = True
        expected = [
            {
                "lang": "en",
                "format": "html",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "https://www.scielo.br/j/xjk/a/ldld?format=html&lang=en",
                "available": True,
                "components": [
                    {
                        "type": "html",
                        "id": "es",
                        "present_in_html": False,
                        "uri": [
                            "/j/xjk/a/ldld?format=html&lang=es",
                            "/j/xjk/a/ldld?lang=es&format=html",
                            "/j/xjk/a/ldld?format=html",
                            "/j/xjk/a/ldld?lang=es",
                            "/j/xjk/a/ldld/?format=html&lang=es",
                            "/j/xjk/a/ldld/?lang=es&format=html",
                            "/j/xjk/a/ldld/?format=html",
                            "/j/xjk/a/ldld/?lang=es",
                        ],
                    },
                ]
            },
            {
                "lang": "es",
                "format": "html",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "https://www.scielo.br/j/xjk/a/ldld?format=html&lang=es",
                "available": True,
                "components": [
                    {
                        "type": "html",
                        "id": "en",
                        "present_in_html": True,
                        "uri": "/j/xjk/a/ldld?format=html",
                    },
                ]
            },
        ]
        result = check_document_uri_items(website_url, doc_data_list, assets_data)
        self.assertListEqual(expected, result)


class TestCheckDocumentHtml(TestCase):

    @patch("operations.check_website_operations.get_webpage_content")
    def test_check_document_html_returns_not_available(self, mock_content):
        mock_content.return_value = None
        uri = "https://..."
        assets_data = []
        other_versions_data = []
        expected = {"available": False}
        result = check_document_html(uri, assets_data, other_versions_data)
        self.assertEqual(expected, result)

    @patch("operations.check_website_operations.get_webpage_content")
    def test_check_document_html_returns_available_and_empty_components(self, mock_content):
        mock_content.return_value = ""
        uri = "https://..."
        assets_data = []
        other_versions_data = []
        expected = {
            "available": True,
            "components": [],
        }
        result = check_document_html(uri, assets_data, other_versions_data)
        self.assertEqual(expected, result)

    @patch("operations.check_website_operations.get_webpage_content")
    def test_check_document_html_returns_available_and_components_are_absent(self, mock_content):
        mock_content.return_value = ""
        uri = "https://..."

        assets_data = [
            {
                "prefix": "asset_uri_1",
                "uri_alternatives": [
                    "asset_uri_1.tiff", "asset_uri_1.jpg", "asset_uri_1.png"]
            },
        ]
        other_versions_data = [
            {
                "lang": "en",
                "format": "html",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "/j/xjk/a/ldld?format=html&lang=en",
            },
        ]
        expected = {
            "available": True,
            "components": [
                {
                    "type": "asset",
                    "id": "asset_uri_1",
                    "present_in_html": False,
                    "uri": [
                        "asset_uri_1.tiff", "asset_uri_1.jpg",
                        "asset_uri_1.png"],
                },
                {
                    "type": "html",
                    "id": "en",
                    "present_in_html": False,
                    "uri": [
                        "/j/xjk/a/ldld?format=html&lang=en",
                        "/j/xjk/a/ldld?lang=en&format=html",
                        "/j/xjk/a/ldld?format=html",
                        "/j/xjk/a/ldld?lang=en",
                        "/j/xjk/a/ldld/?format=html&lang=en",
                        "/j/xjk/a/ldld/?lang=en&format=html",
                        "/j/xjk/a/ldld/?format=html",
                        "/j/xjk/a/ldld/?lang=en",
                    ],
                },
            ]
        }
        result = check_document_html(uri, assets_data, other_versions_data)
        self.assertEqual(expected, result)

    @patch("operations.check_website_operations.get_webpage_content")
    def test_check_document_html_returns_available_and_components_are_present(self, mock_content):
        mock_content.return_value = """
        <img src="asset_uri_1.jpg"/>
        <a href="/j/xjk/a/ldld?lang=en"/>
        """
        uri = "https://..."

        assets_data = [
            {
                "prefix": "asset_uri_1",
                "uri_alternatives": [
                    "asset_uri_1.tiff", "asset_uri_1.jpg", "asset_uri_1.png"]
            },
        ]
        other_versions_data = [
            {
                "lang": "en",
                "format": "html",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "/j/xjk/a/ldld?format=html&lang=en",
            },
        ]
        expected = {
            "available": True,
            "components": [
                {
                    "type": "asset",
                    "id": "asset_uri_1",
                    "present_in_html": True,
                    "uri": "asset_uri_1.jpg"
                },
                {
                    "type": "html",
                    "id": "en",
                    "present_in_html": True,
                    "uri": "/j/xjk/a/ldld?lang=en",
                },
            ]
        }
        result = check_document_html(uri, assets_data, other_versions_data)
        self.assertEqual(expected, result)


class TestCheckDocumentAssetsAvailability(TestCase):
    @patch("operations.check_website_operations.access_uri")
    def test_check_document_assets_availability_returns_one_of_three_is_false(self, mock_access_uri):
        mock_access_uri.side_effect = [not None, False, not None]
        assets_data = [
            {
                "prefix": "a01",
                "asset_alternatives": [
                    {"asset_id": "a01.png",
                     "uri": "uri de a01.png no object store"},
                    {"asset_id": "a01.jpg",
                     "uri": "uri de a01.jpg no object store"},
                ]
            },
            {
                "prefix": "a02",
                "asset_alternatives": [
                    {"asset_id": "a02.png",
                     "uri": "uri de a02.png no object store"},
                ]
            }
        ]
        expected = [
            {"asset_id": "a01.png",
             "uri": "uri de a01.png no object store",
             "available": True},
            {"asset_id": "a01.jpg",
             "uri": "uri de a01.jpg no object store",
             "available": False},
            {"asset_id": "a02.png",
             "uri": "uri de a02.png no object store",
             "available": True},
        ]
        result = check_document_assets_availability(assets_data)
        self.assertListEqual(expected, result)

    def test_check_document_assets_availability_returns_empty_list(self):
        assets_data = []
        expected = []
        result = check_document_assets_availability(assets_data)
        self.assertEqual(expected, result)
