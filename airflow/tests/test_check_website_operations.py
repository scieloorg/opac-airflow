from unittest import TestCase, skip
from unittest.mock import patch, call, MagicMock
import requests

from airflow import DAG

from operations.check_website_operations import (
    concat_website_url_and_uri_list_items,
    check_uri_list,
    check_website_uri_list,
    find_uri_items,
    get_document_webpage_uri,
    get_document_webpages_data,
    check_uri_items_expected_in_webpage,
    check_document_webpages_availability,
    check_document_html,
    check_document_assets_availability,
    check_document_renditions_availability,
    format_document_webpage_availability_to_register,
    format_document_items_availability_to_register,
    get_kernel_document_id_from_classic_document_uri,
    do_request,
    is_valid_response,
)


class TestDoRequest(TestCase):

    @patch("operations.check_website_operations.datetime")
    @patch("operations.check_website_operations.requests_get")
    def test_do_request_returns_response_timestamp(self, mock_get, mock_dt):
        mock_response = requests.Response()
        mock_response.status_code = 200
        mock_dt.utcnow.side_effect = ["start", "end"]
        mock_get.return_value = mock_response
        result = do_request("https://uri.org/8793/")
        self.assertEqual("start", result.start_time)
        self.assertEqual("end", result.end_time)

    @patch("operations.check_website_operations.requests_get")
    def test_do_request_returns_response(self, mock_get):
        mock_response = MockResponse(200)
        mock_get.return_value = mock_response
        result = do_request("https://uri.org/8793/")
        self.assertEqual(200, result.status_code)

    @patch("operations.check_website_operations.requests_get")
    def test_do_request_returns_none(self, mock_get):
        mock_get.return_value = False
        result = do_request("inhttps://uri.org/8793/")
        self.assertEqual(None, result.status_code)

    @patch("operations.check_website_operations.requests_get")
    def test_do_request_returns_500_after_9_tries(self, mock_get):
        mock_get.side_effect = 15 * [MockResponse(500)]
        result = do_request(
            "https://uri.org/8793/",
            secs_sequence=(0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8))
        self.assertEqual(500, result.status_code)

    @patch("operations.check_website_operations.requests_get")
    def test_do_request_returns_301_after_3_tries(self, mock_get):
        mock_get.side_effect = 2 * [MockResponse(500)] + [MockResponse(301)]
        result = do_request(
            "https://uri.org/8793/",
            secs_sequence=(0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8))
        self.assertEqual(301, result.status_code)


class TestIsValidResponse(TestCase):

    def test_is_valid_response_returns_true(self):
        result = is_valid_response(MockResponse(200))
        self.assertEqual(True, result)

    def test_is_valid_response_returns_false(self):
        result = is_valid_response(MockResponse(500))
        self.assertEqual(False, result)

    def test_is_valid_response_returns_false_for_response_equal_to_none(self):
        result = is_valid_response(None)
        self.assertEqual(False, result)


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

    def __init__(self, code, text=None):
        self.status_code = code
        self.text = text or ""
        self.start_time = "start timestamp"
        self.end_time = "end timestamp"


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
    def test_check_uri_list_for_status_code_500_returns_failure_list(self, mock_req_head):
        mock_req_head.side_effect = [MockResponse(500), MockResponse(404)]
        uri_list = ["BAD_URI"]
        result = check_uri_list(uri_list)
        self.assertEqual(
            uri_list,
            result)

    @patch('operations.check_website_operations.retry_after')
    @patch('operations.check_website_operations.requests.head')
    def test_check_uri_list_for_status_code_200_after_retries_returns_failure_list(self, mock_req_head, mock_retry_after):
        mock_retry_after.return_value = [
            0, 0.1, 0.2, 0.4, 0.8, 1,
        ]
        mock_req_head.side_effect = [
            MockResponse(500),
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
            0, 0.1, 0.2, 0.4, 0.8, 1,
        ]
        mock_req_head.side_effect = [
            MockResponse(500),
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
        self.assertIn(
            call('Quantidade de URI: %i', 8),
            mock_info.call_args_list
        )
        self.assertIn(
            call("Encontrados: %i/%i", 8, 8),
            mock_info.call_args_list
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

        self.assertIn(
            call("Não encontrados (%i/%i):\n%s", 2, 8,
                 "\n".join([
                        bad_uri_1,
                        bad_uri_2,
                    ])),
            mock_info.call_args_list
        )
        self.assertIn(
            call('Quantidade de URI: %i', 8),
            mock_info.call_args_list
        )


class TestFindUriItems(TestCase):

    def test_find_uri_items_returns_src_and_href(self):
        content = """<root>
            <img src="bla.jpg"/>
            <p><x src="g.jpg"/></p>
            <a href="d.jpg"/>
            </root>"""
        expected = ["bla.jpg", "d.jpg", "g.jpg"]
        result = find_uri_items(content)
        self.assertEqual(expected, result)

    def test_find_uri_items_returns_src(self):
        content = """<root>
            <p><img src="bla.jpg"/></p><x src="g.jpg"/>
            </root>"""
        expected = ["bla.jpg", "g.jpg"]
        result = find_uri_items(content)
        self.assertEqual(expected, result)

    def test_find_uri_items_returns_href(self):
        content = '<root><a href="d.jpg"/></root>'
        expected = ["d.jpg"]
        result = find_uri_items(content)
        self.assertEqual(expected, result)


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


class TestGetDocumentWebPagesData(TestCase):

    def test_get_document_webpages_data_returns_uri_data_list_using_new_pattern(self):
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
                "uri_alternatives": [
                    '/j/xjk/a/ldld?format=html&lang=en',
                    '/j/xjk/a/ldld?lang=en&format=html',
                    '/j/xjk/a/ldld?format=html',
                    '/j/xjk/a/ldld?lang=en',
                    '/j/xjk/a/ldld/?format=html&lang=en',
                    '/j/xjk/a/ldld/?lang=en&format=html',
                    '/j/xjk/a/ldld/?format=html',
                    '/j/xjk/a/ldld/?lang=en',
                ],
            },
            {
                "lang": "en",
                "format": "pdf",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "/j/xjk/a/ldld?format=pdf&lang=en",
                "uri_alternatives": [
                    '/j/xjk/a/ldld?format=pdf&lang=en',
                    '/j/xjk/a/ldld?lang=en&format=pdf',
                    '/j/xjk/a/ldld?format=pdf',
                    '/j/xjk/a/ldld/?format=pdf&lang=en',
                    '/j/xjk/a/ldld/?lang=en&format=pdf',
                    '/j/xjk/a/ldld/?format=pdf',
                ],
            },
            {
                "lang": "es",
                "format": "html",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "/j/xjk/a/ldld?format=html&lang=es",
                "uri_alternatives": [
                    '/j/xjk/a/ldld?format=html&lang=es',
                    '/j/xjk/a/ldld?lang=es&format=html',
                    '/j/xjk/a/ldld?format=html',
                    '/j/xjk/a/ldld?lang=es',
                    '/j/xjk/a/ldld/?format=html&lang=es',
                    '/j/xjk/a/ldld/?lang=es&format=html',
                    '/j/xjk/a/ldld/?format=html',
                    '/j/xjk/a/ldld/?lang=es',
                ],
            },
            {
                "lang": "es",
                "format": "pdf",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "/j/xjk/a/ldld?format=pdf&lang=es",
                "uri_alternatives": [
                    '/j/xjk/a/ldld?format=pdf&lang=es',
                    '/j/xjk/a/ldld?lang=es&format=pdf',
                    '/j/xjk/a/ldld?format=pdf',
                    '/j/xjk/a/ldld/?format=pdf&lang=es',
                    '/j/xjk/a/ldld/?lang=es&format=pdf',
                    '/j/xjk/a/ldld/?format=pdf',
                ],
            },
        ]
        result = get_document_webpages_data(doc_id, doc_data_list, get_document_webpage_uri)
        self.assertEqual(expected, result)

    def test_get_document_webpages_data_raises_value_error_if_acron_is_none_and_using_new_uri_pattern(self):
        doc_id = "ldld"
        with self.assertRaises(ValueError):
            get_document_webpages_data(doc_id, [], get_document_webpage_uri)

    def test_get_document_webpages_data_raises_value_error_if_acron_is_empty_str_and_using_new_uri_pattern(self):
        doc_id = "ldld"
        with self.assertRaises(ValueError):
            get_document_webpages_data(doc_id, [], get_document_webpage_uri)

    def test_get_document_webpages_data_returns_uri_data_list_using_classic_pattern(self):
        doc_id = "ldld"
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
                "doc_id": "ldld",
                "uri": "/scielo.php?script=sci_arttext&pid=S1234-56782000123412313&tlng=en",
                "uri_alternatives": [
                    '/j/xjk/a/ldld?format=html&lang=en',
                    '/j/xjk/a/ldld?lang=en&format=html',
                    '/j/xjk/a/ldld?format=html',
                    '/j/xjk/a/ldld?lang=en',
                    '/j/xjk/a/ldld/?format=html&lang=en',
                    '/j/xjk/a/ldld/?lang=en&format=html',
                    '/j/xjk/a/ldld/?format=html',
                    '/j/xjk/a/ldld/?lang=en',
                ],
            },
            {
                "lang": "en",
                "format": "pdf",
                "pid_v2": "S1234-56782000123412313",
                "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "/scielo.php?script=sci_pdf&pid=S1234-56782000123412313&tlng=en",
                "uri_alternatives": [
                    '/j/xjk/a/ldld?format=pdf&lang=en',
                    '/j/xjk/a/ldld?lang=en&format=pdf',
                    '/j/xjk/a/ldld?format=pdf',
                    '/j/xjk/a/ldld/?format=pdf&lang=en',
                    '/j/xjk/a/ldld/?lang=en&format=pdf',
                    '/j/xjk/a/ldld/?format=pdf',
                ],
            },
            {
                "lang": "es",
                "format": "html",
                "pid_v2": "S1234-56782000123412313",
                "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "/scielo.php?script=sci_arttext&pid=S1234-56782000123412313&tlng=es",
                "uri_alternatives": [
                    '/j/xjk/a/ldld?format=html&lang=es',
                    '/j/xjk/a/ldld?lang=es&format=html',
                    '/j/xjk/a/ldld?format=html',
                    '/j/xjk/a/ldld?lang=es',
                    '/j/xjk/a/ldld/?format=html&lang=es',
                    '/j/xjk/a/ldld/?lang=es&format=html',
                    '/j/xjk/a/ldld/?format=html',
                    '/j/xjk/a/ldld/?lang=es',
                ],
            },
            {
                "lang": "es",
                "format": "pdf",
                "pid_v2": "S1234-56782000123412313",
                "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "/scielo.php?script=sci_pdf&pid=S1234-56782000123412313&tlng=es",
                "uri_alternatives": [
                    '/j/xjk/a/ldld?format=pdf&lang=es',
                    '/j/xjk/a/ldld?lang=es&format=pdf',
                    '/j/xjk/a/ldld?format=pdf',
                    '/j/xjk/a/ldld/?format=pdf&lang=es',
                    '/j/xjk/a/ldld/?lang=es&format=pdf',
                    '/j/xjk/a/ldld/?format=pdf',
                ],
            },
        ]
        result = get_document_webpages_data(doc_id, doc_data_list)
        self.assertEqual(expected, result)


class TestCheckWebpageInnerUriList(TestCase):

    def test_check_uri_items_expected_in_webpage_returns_success(self):
        uri_items_in_html = [
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
        other_webpages_uri_data = [
            {
                "lang": "en",
                "format": "html",
                "acron": "xyz",
                "doc_id": "lokiujyht",
                "uri_alternatives": [
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
                "lang": "es",
                "format": "pdf",
                "acron": "xyz",
                "doc_id": "lokiujyht",
                "uri_alternatives": [
                    "/j/xyz/a/lokiujyht?format=pdf&lang=es",
                    "/j/xyz/a/lokiujyht?lang=es&format=pdf",
                    "/j/xyz/a/lokiujyht?format=pdf",
                    "/j/xyz/a/lokiujyht/?format=pdf&lang=es",
                    "/j/xyz/a/lokiujyht/?lang=es&format=pdf",
                    "/j/xyz/a/lokiujyht/?format=pdf",
                ],
            },
            {
                "lang": "en",
                "format": "pdf",
                "acron": "xyz",
                "doc_id": "lokiujyht",
                "uri_alternatives": [
                    "/j/xyz/a/lokiujyht?format=pdf&lang=en",
                    "/j/xyz/a/lokiujyht?lang=en&format=pdf",
                    "/j/xyz/a/lokiujyht?format=pdf",
                    "/j/xyz/a/lokiujyht/?format=pdf&lang=en",
                    "/j/xyz/a/lokiujyht/?lang=en&format=pdf",
                    "/j/xyz/a/lokiujyht/?format=pdf",
                ],
            },
        ]
        expected = [
            {
                "type": "asset",
                "id": "asset_uri_1",
                "present_in_html": [
                    "asset_uri_1.jpg",
                ],
                "absent_in_html": [
                    "asset_uri_1.tiff", "asset_uri_1.png",
                ],
            },
            {
                "type": "asset",
                "id": "asset_uri_2",
                "present_in_html": [
                    "asset_uri_2.jpg",
                ],
                "absent_in_html": [
                    "asset_uri_2.tiff", "asset_uri_2.png",
                ],
            },
            {
                "type": "html",
                "id": "en",
                "present_in_html": [
                    "/j/xyz/a/lokiujyht?format=html&lang=en",
                ],
            },
            {
                "type": "pdf",
                "id": "es",
                "present_in_html": [
                    "/j/xyz/a/lokiujyht?format=pdf&lang=es",
                ],
            },
            {
                "type": "pdf",
                "id": "en",
                "present_in_html": [
                    "/j/xyz/a/lokiujyht?format=pdf&lang=en",
                ],
            },
        ]
        result, missing = check_uri_items_expected_in_webpage(uri_items_in_html,
                    assets_data, other_webpages_uri_data)
        self.assertEqual(expected, result)
        self.assertEqual(0, missing)

    def test_check_uri_items_expected_in_webpage_returns_not_found_asset_uri(self):
        uri_items_in_html = [
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
        other_webpages_uri_data = [
        ]
        expected = [
            {
                "type": "asset",
                "id": "asset_uri_1",
                "present_in_html": [
                    "asset_uri_1.jpg",
                ],
                "absent_in_html": [
                    "asset_uri_1.tiff", "asset_uri_1.png",
                ],
            },
            {
                "type": "asset",
                "id": "asset_uri_2",
                "present_in_html": [],
                "absent_in_html": [
                    "asset_uri_2.tiff", "asset_uri_2.jpg", "asset_uri_2.png"],
            },
        ]
        result, missing = check_uri_items_expected_in_webpage(uri_items_in_html,
                    assets_data, other_webpages_uri_data)
        self.assertEqual(expected, result)
        self.assertEqual(1, missing)

    def test_check_uri_items_expected_in_webpage_returns_not_found_pdf(self):
        uri_items_in_html = [
            "/j/xyz/a/lokiujyht?format=html&lang=en",
            "/j/xyz/a/lokiujyht?format=pdf&lang=en",
        ]
        assets_data = [
        ]
        other_webpages_uri_data = [
            {
                "lang": "en",
                "format": "html",
                "acron": "xyz",
                "doc_id": "lokiujyht",
                "uri_alternatives": [
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
                "lang": "es",
                "format": "pdf",
                "acron": "xyz",
                "doc_id": "lokiujyht",
                "uri_alternatives": [
                    "/j/xyz/a/lokiujyht?format=pdf&lang=es",
                    "/j/xyz/a/lokiujyht?lang=es&format=pdf",
                    "/j/xyz/a/lokiujyht?format=pdf",
                    "/j/xyz/a/lokiujyht/?format=pdf&lang=es",
                    "/j/xyz/a/lokiujyht/?lang=es&format=pdf",
                    "/j/xyz/a/lokiujyht/?format=pdf",
                ],
            },
            {
                "lang": "en",
                "format": "pdf",
                "acron": "xyz",
                "doc_id": "lokiujyht",
                "uri_alternatives": [
                    "/j/xyz/a/lokiujyht?format=pdf&lang=en",
                    "/j/xyz/a/lokiujyht?lang=en&format=pdf",
                    "/j/xyz/a/lokiujyht?format=pdf",
                    "/j/xyz/a/lokiujyht/?format=pdf&lang=en",
                    "/j/xyz/a/lokiujyht/?lang=en&format=pdf",
                    "/j/xyz/a/lokiujyht/?format=pdf",
                ],
            },
        ]
        expected = [
            {
                "type": "html",
                "id": "en",
                "present_in_html": [
                    "/j/xyz/a/lokiujyht?format=html&lang=en",
                ],
            },
            {
                "type": "pdf",
                "id": "es",
                "present_in_html": [],
                "absent_in_html": [
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
                "present_in_html": [
                    "/j/xyz/a/lokiujyht?format=pdf&lang=en",
                ],
            },
        ]
        result, missing = check_uri_items_expected_in_webpage(
                    uri_items_in_html,
                    assets_data, other_webpages_uri_data)
        self.assertEqual(expected, result)
        self.assertEqual(1, missing)

    def test_check_uri_items_expected_in_webpage_returns_not_found_html(self):
        uri_items_in_html = [
            "/j/xyz/a/lokiujyht?format=pdf&lang=es",
            "/j/xyz/a/lokiujyht?format=pdf&lang=en",
        ]
        assets_data = [
        ]
        other_webpages_uri_data = [
            {
                "lang": "en",
                "format": "html",
                "acron": "xyz",
                "doc_id": "lokiujyht",
                "uri_alternatives": [
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
                "lang": "es",
                "format": "pdf",
                "acron": "xyz",
                "doc_id": "lokiujyht",
                "uri_alternatives": [
                    "/j/xyz/a/lokiujyht?format=pdf&lang=es",
                    "/j/xyz/a/lokiujyht?lang=es&format=pdf",
                    "/j/xyz/a/lokiujyht?format=pdf",
                    "/j/xyz/a/lokiujyht/?format=pdf&lang=es",
                    "/j/xyz/a/lokiujyht/?lang=es&format=pdf",
                    "/j/xyz/a/lokiujyht/?format=pdf",
                ],
            },
            {
                "lang": "en",
                "format": "pdf",
                "acron": "xyz",
                "doc_id": "lokiujyht",
                "uri_alternatives": [
                    "/j/xyz/a/lokiujyht?format=pdf&lang=en",
                    "/j/xyz/a/lokiujyht?lang=en&format=pdf",
                    "/j/xyz/a/lokiujyht?format=pdf",
                    "/j/xyz/a/lokiujyht/?format=pdf&lang=en",
                    "/j/xyz/a/lokiujyht/?lang=en&format=pdf",
                    "/j/xyz/a/lokiujyht/?format=pdf",
                ],
            },
        ]
        expected = [
            {
                "type": "html",
                "id": "en",
                "present_in_html": [],
                "absent_in_html": [
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
                "present_in_html": [
                    "/j/xyz/a/lokiujyht?format=pdf&lang=es",
                ],
            },
            {
                "type": "pdf",
                "id": "en",
                "present_in_html": [
                    "/j/xyz/a/lokiujyht?format=pdf&lang=en",
                ],
            },
        ]
        result, missing = check_uri_items_expected_in_webpage(uri_items_in_html,
                    assets_data, other_webpages_uri_data)
        self.assertEqual(expected, result)
        self.assertEqual(1, missing)


class TestCheckDocumentUriItemsAvailability(TestCase):

    @patch("operations.check_website_operations.do_request")
    def test_check_document_webpages_availability_returns_success(self, mock_do_request):
        website_url = "https://www.scielo.br"
        doc_data_list = [
            {
                "lang": "en",
                "format": "html",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "/j/xjk/a/ldld?format=html&lang=en",
                "uri_alternatives": [
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
            {
                "lang": "en",
                "format": "pdf",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "/j/xjk/a/ldld?format=pdf&lang=en",
                "uri_alternatives": [
                    "/j/xjk/a/ldld?format=pdf&lang=en",
                    "/j/xjk/a/ldld?lang=en&format=pdf",
                    "/j/xjk/a/ldld?format=pdf",
                    "/j/xjk/a/ldld?lang=en",
                    "/j/xjk/a/ldld/?format=pdf&lang=en",
                    "/j/xjk/a/ldld/?lang=en&format=pdf",
                    "/j/xjk/a/ldld/?format=pdf",
                    "/j/xjk/a/ldld/?lang=en",
                ],
            },
            {
                "lang": "es",
                "format": "html",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "/j/xjk/a/ldld?format=html&lang=es",
                "uri_alternatives": [
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
            {
                "lang": "es",
                "format": "pdf",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "/j/xjk/a/ldld?format=pdf&lang=es",
                "uri_alternatives": [
                    "/j/xjk/a/ldld?format=pdf&lang=es",
                    "/j/xjk/a/ldld?lang=es&format=pdf",
                    "/j/xjk/a/ldld?format=pdf",
                    "/j/xjk/a/ldld?lang=es",
                    "/j/xjk/a/ldld/?format=pdf&lang=es",
                    "/j/xjk/a/ldld/?lang=es&format=pdf",
                    "/j/xjk/a/ldld/?format=pdf",
                    "/j/xjk/a/ldld/?lang=es",
                ],
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
        content = [
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
        mock_do_request.side_effect = [
            MockResponse(200, content[0]),
            MockResponse(200),
            MockResponse(200, content[1]),
            MockResponse(200)]
        expected = [
            {
                "lang": "en",
                "format": "html",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "https://www.scielo.br/j/xjk/a/ldld?format=html&lang=en",
                "available": True,
                "status code": 200,
                "start time": "start timestamp",
                "end time": "end timestamp",
                "components": [
                    {
                        "type": "asset",
                        "id": "asset_uri_1",
                        "present_in_html": [
                            "asset_uri_1.jpg",
                        ],
                        "absent_in_html": [
                            "asset_uri_1.tiff", "asset_uri_1.png",
                        ],
                    },
                    {
                        "type": "asset",
                        "id": "asset_uri_2",
                        "present_in_html": [
                            "asset_uri_2.jpg",
                        ],
                        "absent_in_html": [
                            "asset_uri_2.tiff", "asset_uri_2.png",
                        ],
                    },
                    {
                        "type": "pdf",
                        "id": "en",
                        "present_in_html": [
                            "/j/xjk/a/ldld?format=pdf&lang=en",
                        ],
                    },
                    {
                        "type": "html",
                        "id": "es",
                        "present_in_html": [
                            "/j/xjk/a/ldld?format=html&lang=es",
                        ],
                    },
                    {
                        "type": "pdf",
                        "id": "es",
                        "present_in_html": [
                            "/j/xjk/a/ldld?format=pdf&lang=es",
                        ],
                    },
                ],
                "missing components quantity": 0,
                "total expected components": 5,
            },
            {
                "lang": "en",
                "format": "pdf",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "https://www.scielo.br/j/xjk/a/ldld?format=pdf&lang=en",
                "available": True,
                "status code": 200,
                "start time": "start timestamp",
                "end time": "end timestamp",
            },
            {
                "lang": "es",
                "format": "html",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "https://www.scielo.br/j/xjk/a/ldld?format=html&lang=es",
                "available": True,
                "status code": 200,
                "start time": "start timestamp",
                "end time": "end timestamp",
                "components": [
                    {
                        "type": "asset",
                        "id": "asset_uri_1",
                        "present_in_html": [
                            "asset_uri_1.jpg",
                        ],
                        "absent_in_html": [
                            "asset_uri_1.tiff", "asset_uri_1.png",
                        ],
                    },
                    {
                        "type": "asset",
                        "id": "asset_uri_2",
                        "present_in_html": [
                            "asset_uri_2.jpg",
                        ],
                        "absent_in_html": [
                            "asset_uri_2.tiff", "asset_uri_2.png",
                        ],
                    },
                    {
                        "type": "html",
                        "id": "en",
                        "present_in_html": [
                            "/j/xjk/a/ldld?format=html&lang=en",
                        ],
                    },
                    {
                        "type": "pdf",
                        "id": "en",
                        "present_in_html": [
                            "/j/xjk/a/ldld?format=pdf&lang=en",
                        ],
                    },
                    {
                        "type": "pdf",
                        "id": "es",
                        "present_in_html": [
                            "/j/xjk/a/ldld?format=pdf&lang=es",
                        ],
                    },
                ],
                "missing components quantity": 0,
                "total expected components": 5,
            },
            {
                "lang": "es",
                "format": "pdf",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "https://www.scielo.br/j/xjk/a/ldld?format=pdf&lang=es",
                "available": True,
                "status code": 200,
                "start time": "start timestamp",
                "end time": "end timestamp",
            },
        ]
        result, summary = check_document_webpages_availability(website_url, doc_data_list, assets_data)
        self.assertDictEqual({
                "unavailable doc webpages": 0,
                "missing components": 0,
                "total expected components": 10,
            },
            summary
        )
        self.assertListEqual(expected, result)

    @patch("operations.check_website_operations.do_request")
    def test_check_document_webpages_availability_returns_pdf_is_not_available_although_it_is_present_in_html(self, mock_do_request):
        website_url = "https://www.scielo.br"
        doc_data_list = [
            {
                "lang": "en",
                "format": "html",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "/j/xjk/a/ldld?format=html&lang=en",
                "uri_alternatives": [
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
            {
                "lang": "en",
                "format": "pdf",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "/j/xjk/a/ldld?format=pdf&lang=en",
                "uri_alternatives": [
                    "/j/xjk/a/ldld?format=pdf&lang=en",
                    "/j/xjk/a/ldld?lang=en&format=pdf",
                    "/j/xjk/a/ldld?format=pdf",
                    "/j/xjk/a/ldld?lang=en",
                    "/j/xjk/a/ldld/?format=pdf&lang=en",
                    "/j/xjk/a/ldld/?lang=en&format=pdf",
                    "/j/xjk/a/ldld/?format=pdf",
                    "/j/xjk/a/ldld/?lang=en",
                ],
            },
        ]
        assets_data = [
        ]
        content = """
        <a href="/j/xjk/a/ldld?format=pdf&lang=en"/>
        """
        mock_do_request.side_effect = [
            MockResponse(200, content),
            MockResponse(None)]
        expected = [
            {
                "lang": "en",
                "format": "html",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "https://www.scielo.br/j/xjk/a/ldld?format=html&lang=en",
                "available": True,
                "status code": 200,
                "start time": "start timestamp",
                "end time": "end timestamp",
                "components": [
                    {
                        "type": "pdf",
                        "id": "en",
                        "present_in_html": [
                            "/j/xjk/a/ldld?format=pdf&lang=en",
                        ],
                    },
                ],
                "missing components quantity": 0,
                "total expected components": 1,
            },
            {
                "lang": "en",
                "format": "pdf",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "https://www.scielo.br/j/xjk/a/ldld?format=pdf&lang=en",
                "available": False,
                "status code": None,
                "start time": "start timestamp",
                "end time": "end timestamp",
            },
        ]
        result, summary = check_document_webpages_availability(website_url, doc_data_list, assets_data)
        self.assertDictEqual(
            {
                "unavailable doc webpages": 1,
                "missing components": 0,
                "total expected components": 1,
            },
            summary
        )
        self.assertListEqual(expected, result)

    @patch("operations.check_website_operations.do_request")
    def test_check_document_webpages_availability_returns_pdf_is_available_although_it_is_not_present_in_html(self, mock_do_request):
        website_url = "https://www.scielo.br"
        doc_data_list = [
            {
                "lang": "en",
                "format": "html",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "/j/xjk/a/ldld?format=html&lang=en",
                "uri_alternatives": [
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
            {
                "lang": "en",
                "format": "pdf",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "/j/xjk/a/ldld?format=pdf&lang=en",
                "uri_alternatives": [
                    "/j/xjk/a/ldld?format=pdf&lang=en",
                    "/j/xjk/a/ldld?lang=en&format=pdf",
                    "/j/xjk/a/ldld?format=pdf",
                    "/j/xjk/a/ldld?lang=en",
                    "/j/xjk/a/ldld/?format=pdf&lang=en",
                    "/j/xjk/a/ldld/?lang=en&format=pdf",
                    "/j/xjk/a/ldld/?format=pdf",
                    "/j/xjk/a/ldld/?lang=en",
                ],
            },
        ]
        assets_data = [
        ]
        mock_do_request.side_effect = [
            MockResponse(200, ""),
            MockResponse(200)
        ]
        expected = [
            {
                "lang": "en",
                "format": "html",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "https://www.scielo.br/j/xjk/a/ldld?format=html&lang=en",
                "available": True,
                "status code": 200,
                "start time": "start timestamp",
                "end time": "end timestamp",
                "components": [
                    {
                        "type": "pdf",
                        "id": "en",
                        "present_in_html": [],
                        "absent_in_html": [
                            "/j/xjk/a/ldld?format=pdf&lang=en",
                            "/j/xjk/a/ldld?lang=en&format=pdf",
                            "/j/xjk/a/ldld?format=pdf",
                            "/j/xjk/a/ldld?lang=en",
                            "/j/xjk/a/ldld/?format=pdf&lang=en",
                            "/j/xjk/a/ldld/?lang=en&format=pdf",
                            "/j/xjk/a/ldld/?format=pdf",
                            "/j/xjk/a/ldld/?lang=en",
                        ],
                    },
                ],
                "missing components quantity": 1,
                "total expected components": 1,
                "existing_uri_items_in_html": [],
            },
            {
                "lang": "en",
                "format": "pdf",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "https://www.scielo.br/j/xjk/a/ldld?format=pdf&lang=en",
                "available": True,
                "status code": 200,
                "start time": "start timestamp",
                "end time": "end timestamp",
            },
        ]
        result, summary = check_document_webpages_availability(
            website_url, doc_data_list, assets_data)
        self.assertDictEqual({
                "unavailable doc webpages": 0,
                "missing components": 1,
                "total expected components": 1,
            },
            summary
        )
        self.assertListEqual(expected, result)

    @patch("operations.check_website_operations.do_request")
    def test_check_document_webpages_availability_returns_html_es_is_not_available_although_it_is_present_in_html_en(self, mock_do_request):
        website_url = "https://www.scielo.br"
        doc_data_list = [
            {
                "lang": "en",
                "format": "html",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "/j/xjk/a/ldld?format=html&lang=en",
                "uri_alternatives": [
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
            {
                "lang": "es",
                "format": "html",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "/j/xjk/a/ldld?format=html&lang=es",
                "uri_alternatives": [
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
        assets_data = [
        ]
        mock_do_request.side_effect = [
            MockResponse(200,
                """
                Versão ingles no formato html
                <a href="/j/xjk/a/ldld?format=html&lang=es"/>
                """
            ),
            MockResponse(None)
        ]
        expected = [
            {
                "lang": "en",
                "format": "html",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "https://www.scielo.br/j/xjk/a/ldld?format=html&lang=en",
                "available": True,
                "status code": 200,
                "start time": "start timestamp",
                "end time": "end timestamp",
                "components": [
                    {
                        "type": "html",
                        "id": "es",
                        "present_in_html": [
                            "/j/xjk/a/ldld?format=html&lang=es",
                        ],
                    },
                ],
                "missing components quantity": 0,
                "total expected components": 1,
            },
            {
                "lang": "es",
                "format": "html",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "https://www.scielo.br/j/xjk/a/ldld?format=html&lang=es",
                "available": False,
                "status code": None,
                "start time": "start timestamp",
                "end time": "end timestamp",
                "missing components quantity": 1,
                "total expected components": 1,
            },
        ]
        result, summary = check_document_webpages_availability(website_url, doc_data_list, assets_data)
        self.assertDictEqual(
            {
                "unavailable doc webpages": 1,
                "missing components": 1,
                "total expected components": 2,
            },
            summary
        )
        self.assertListEqual(expected, result)

    @patch("operations.check_website_operations.do_request")
    def test_check_document_webpages_availability_returns_html_es_is_available_although_it_is_not_present_in_html_en(self, mock_do_request):
        website_url = "https://www.scielo.br"
        doc_data_list = [
            {
                "lang": "en",
                "format": "html",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "/j/xjk/a/ldld?format=html&lang=en",
                "uri_alternatives": [
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
            {
                "lang": "es",
                "format": "html",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "/j/xjk/a/ldld?format=html&lang=es",
                "uri_alternatives": [
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
        assets_data = [
        ]
        mock_do_request.side_effect = [
            MockResponse(
                200, "documento sem links, conteúdo do html em Ingles"),
            MockResponse(
                200,
                """
                conteúdo do documento em espanhol com link para a versão ingles
                    <a href="/j/xjk/a/ldld?format=html"/>
                """
            ),
        ]
        expected = [
            {
                "lang": "en",
                "format": "html",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "https://www.scielo.br/j/xjk/a/ldld?format=html&lang=en",
                "available": True,
                "status code": 200,
                "start time": "start timestamp",
                "end time": "end timestamp",
                "components": [
                    {
                        "type": "html",
                        "id": "es",
                        "present_in_html": [],
                        "absent_in_html": [
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
                ],
                "missing components quantity": 1,
                "total expected components": 1,
                "existing_uri_items_in_html": []
            },
            {
                "lang": "es",
                "format": "html",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "https://www.scielo.br/j/xjk/a/ldld?format=html&lang=es",
                "available": True,
                "status code": 200,
                "start time": "start timestamp",
                "end time": "end timestamp",
                "components": [
                    {
                        "type": "html",
                        "id": "en",
                        "present_in_html": [
                            "/j/xjk/a/ldld?format=html",
                        ],
                    },
                ],
                "missing components quantity": 0,
                "total expected components": 1,
            },
        ]
        result, summary = check_document_webpages_availability(
            website_url, doc_data_list, assets_data)
        self.assertDictEqual(
            {
                "unavailable doc webpages": 0,
                "missing components": 1,
                "total expected components": 2,
            },
            summary
        )
        self.assertListEqual(expected, result)


class TestCheckDocumentHtml(TestCase):

    @patch("operations.check_website_operations.datetime")
    @patch("operations.check_website_operations.requests.get")
    def test_check_document_html_returns_not_available(self, mock_get, mock_dt):
        mock_get.return_value = None
        mock_dt.utcnow.side_effect = ["start timestamp", "end timestamp"]
        uri = "https://..."
        assets_data = []
        other_webpages_data = []
        expected = {
            "available": False, "status code": None,
            "start time": "start timestamp",
            "end time": "end timestamp",
            "missing components quantity": 0,
            "total expected components": 0,
        }
        result = check_document_html(uri, assets_data, other_webpages_data)
        self.assertEqual(expected, result)

    @patch("operations.check_website_operations.datetime")
    @patch("operations.check_website_operations.requests.get")
    def test_check_document_html_returns_available_and_empty_components(self, mock_get, mock_dt):
        mock_response = MockResponse(200, "")
        mock_get.return_value = mock_response
        mock_dt.utcnow.side_effect = ["start timestamp", "end timestamp"]
        uri = "https://..."
        assets_data = []
        other_webpages_data = []
        expected = {
            "available": True,
            "status code": 200,
            "start time": "start timestamp",
            "end time": "end timestamp",
            "components": [],
            "missing components quantity": 0,
            "total expected components": 0,
        }
        result = check_document_html(uri, assets_data, other_webpages_data)
        self.assertEqual(expected, result)

    @patch("operations.check_website_operations.datetime")
    @patch("operations.check_website_operations.requests.get")
    def test_check_document_html_returns_available_and_components_are_absent(self, mock_get, mock_dt):
        mock_response = MockResponse(200, "")
        mock_get.return_value = mock_response
        mock_dt.utcnow.side_effect = ["start timestamp", "end timestamp"]
        uri = "https://..."

        assets_data = [
            {
                "prefix": "asset_uri_1",
                "uri_alternatives": [
                    "asset_uri_1.tiff", "asset_uri_1.jpg", "asset_uri_1.png"]
            },
        ]
        other_webpages_data = [
            {
                "lang": "en",
                "format": "html",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "/j/xjk/a/ldld?format=html&lang=en",
                "uri_alternatives": [
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
        expected = {
            "available": True,
            "status code": 200,
            "start time": "start timestamp",
            "end time": "end timestamp",
            "components": [
                {
                    "type": "asset",
                    "id": "asset_uri_1",
                    "present_in_html": [],
                    "absent_in_html": [
                        "asset_uri_1.tiff", "asset_uri_1.jpg",
                        "asset_uri_1.png"],
                },
                {
                    "type": "html",
                    "id": "en",
                    "present_in_html": [],
                    "absent_in_html": [
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
            ],
            "missing components quantity": 2,
            "total expected components": 2,
            "existing_uri_items_in_html": []
        }
        result = check_document_html(uri, assets_data, other_webpages_data)
        self.assertEqual(expected, result)

    @patch("operations.check_website_operations.datetime")
    @patch("operations.check_website_operations.requests.get")
    def test_check_document_html_returns_available_and_components_are_present(self, mock_get, mock_dt):
        mock_response = MockResponse(200, "")
        mock_response.text = """
        <img src="asset_uri_1.jpg"/>
        <a href="/j/xjk/a/ldld?lang=en"/>
        """
        mock_get.return_value = mock_response
        mock_dt.utcnow.side_effect = ["start timestamp", "end timestamp"]

        uri = "https://..."

        assets_data = [
            {
                "prefix": "asset_uri_1",
                "uri_alternatives": [
                    "asset_uri_1.tiff", "asset_uri_1.jpg", "asset_uri_1.png"]
            },
        ]
        other_webpages_data = [
            {
                "lang": "en",
                "format": "html",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "/j/xjk/a/ldld?format=html&lang=en",
                "uri_alternatives": [
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
        expected = {
            "available": True,
            "status code": 200,
            "start time": "start timestamp",
            "end time": "end timestamp",
            "components": [
                {
                    "type": "asset",
                    "id": "asset_uri_1",
                    "present_in_html": ["asset_uri_1.jpg"],
                    "absent_in_html": [
                        "asset_uri_1.tiff", "asset_uri_1.png"
                    ],
                },
                {
                    "type": "html",
                    "id": "en",
                    "present_in_html": [
                        "/j/xjk/a/ldld?lang=en",
                    ],
                },
            ],
            "missing components quantity": 0,
            "total expected components": 2,
        }
        result = check_document_html(uri, assets_data, other_webpages_data)
        self.assertEqual(expected, result)


class TestCheckDocumentAssetsAvailability(TestCase):
    @patch("operations.check_website_operations.do_request")
    def test_check_document_assets_availability_returns_one_of_three_is_false(self, mock_do_request):
        mock_do_request.side_effect = [
            MockResponse(200),
            MockResponse(None),
            MockResponse(200)]
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
             "available": True,
             "status code": 200,
             "start time": "start timestamp",
             "end time": "end timestamp",
            },
            {"asset_id": "a01.jpg",
             "uri": "uri de a01.jpg no object store",
             "available": False,
             "status code": None,
             "start time": "start timestamp",
             "end time": "end timestamp",
            },
            {"asset_id": "a02.png",
             "uri": "uri de a02.png no object store",
             "available": True,
             "status code": 200,
             "start time": "start timestamp",
             "end time": "end timestamp",
            },
        ]
        result, q_unavailable = check_document_assets_availability(assets_data)
        self.assertEqual(1, q_unavailable)
        self.assertListEqual(expected, result)

    def test_check_document_assets_availability_returns_empty_list(self):
        assets_data = []
        expected = []
        result, q_unavailable = check_document_assets_availability(assets_data)
        self.assertEqual(0, q_unavailable)
        self.assertEqual(expected, result)


class TestCheckDocumentRenditionsAvailability(TestCase):
    @patch("operations.check_website_operations.do_request")
    def test_check_document_renditions_availability_returns_one_of_three_is_false(self, mock_do_request):
        mock_do_request.side_effect = [MockResponse(200), MockResponse(None)]
        renditions = [
            {
                "lang": "es",
                "uri": "uri de original.pdf no object store",
            },
            {
                "lang": "en",
                "uri": "uri de original-en.pdf  no object store",
            }
        ]
        expected = [
            {
                "lang": "es",
                "uri": "uri de original.pdf no object store",
                "available": True,
                "status code": 200,
                "start time": "start timestamp",
                "end time": "end timestamp",
            },
            {
                "lang": "en",
                "uri": "uri de original-en.pdf  no object store",
                "available": False,
                "status code": None,
                "start time": "start timestamp",
                "end time": "end timestamp",
            }
        ]
        result, q_unavailable = check_document_renditions_availability(
            renditions)
        self.assertEqual(1, q_unavailable)
        self.assertListEqual(expected, result)

    def test_check_document_renditions_availability_returns_empty_list(self):
        rendition = []
        expected = []
        result, q_unavailable = check_document_renditions_availability(
            rendition)
        self.assertEqual(0, q_unavailable)
        self.assertEqual(expected, result)


class TestFormatDocumentVersionsAvailabilityToRegister(TestCase):

    @skip("enquanto nao tem ha definicao da tabela para o grafana")
    def test_format_document_webpage_availability_to_register(self):
        data = {
            "doc_id": "DMKLOLSJ",
            "pid_v2": "S1234-12342020123412345",
            "doc_id_for_human": "1234-1234-acron-45-9-12345",
            "format": "html",
            "lang": "en",
            "uri": "/j/acron/a/DOC/?format=html&lang=en",
            "available": True,
            "components": [
                {
                    "type": "asset",
                    "id": "1234-1234-acron-45-9-12345-f01",
                    "present_in_html": [
                        "https://1234-1234-acron-45-9-12345-f01.jpg",
                    ],
                },
                {
                    "type": "asset",
                    "id": "1234-1234-acron-45-9-12345-f02",
                    "present_in_html": [],
                    "absent_in_html": [
                        "https://1234-1234-acron-45-9-12345-f02.jpg"],
                },
                {
                    "type": "pdf",
                    "id": "en",
                    "present_in_html": [
                        "/j/acron/a/DOC/?format=pdf&lang=en",
                    ],
                },
            ],
            "missing components quantity": 1,
            "total expected components": 3,
            "existing_uri_items_in_html": [
                '/j/acron/a/DOC/?format=pdf&lang=en',
                'https://1234-1234-acron-45-9-12345-f01.jpg']
        }

        extra_data = {"chave": "valor", "chave2": "valor 2"}
        expected = [
            {
                "chave": "valor",
                "chave2": "valor 2",
                "annotation": "",
                "doc_id": "DMKLOLSJ",
                "pid_v2": "S1234-12342020123412345",
                "doc_id_for_human": "1234-1234-acron-45-9-12345",
                "type": "html",
                "id": "en",
                "uri": "/j/acron/a/DOC/?format=html&lang=en",
                "status": "available",
            },
            {
                "chave": "valor",
                "chave2": "valor 2",
                "annotation": "",
                "doc_id": "DMKLOLSJ",
                "pid_v2": "S1234-12342020123412345",
                "doc_id_for_human": "1234-1234-acron-45-9-12345",
                "type": "asset",
                "id": "1234-1234-acron-45-9-12345-f01",
                "uri": "https://1234-1234-acron-45-9-12345-f01.jpg",
                "status": "present in HTML",
            },
            {
                "chave": "valor",
                "chave2": "valor 2",
                "annotation": ("Existing in HTML:\n"
                               "/j/acron/a/DOC/?format=pdf&lang=en\n"
                               "https://1234-1234-acron-45-9-12345-f01.jpg"),
                "doc_id": "DMKLOLSJ",
                "pid_v2": "S1234-12342020123412345",
                "doc_id_for_human": "1234-1234-acron-45-9-12345",
                "type": "asset",
                "id": "1234-1234-acron-45-9-12345-f02",
                "uri": str(["https://1234-1234-acron-45-9-12345-f02.jpg"]),
                "status": "absent in HTML",
            },
            {
                "chave": "valor",
                "chave2": "valor 2",
                "annotation": "",
                "doc_id": "DMKLOLSJ",
                "pid_v2": "S1234-12342020123412345",
                "doc_id_for_human": "1234-1234-acron-45-9-12345",
                "type": "pdf",
                "id": "en",
                "uri": "/j/acron/a/DOC/?format=pdf&lang=en",
                "status": "present in HTML",
            },
        ]
        result = format_document_webpage_availability_to_register(
            data, extra_data)
        self.assertEqual(expected, result)


class TestFormatDocumentItemsAvailabilityToRegister(TestCase):

    @skip("enquanto nao tem ha definicao da tabela para o grafana")
    def test_format_document_items_availability_to_register(self):
        document_data = {
            "doc_id": "DMKLOLSJ",
            "pid_v2": "S1234-12342020123412345",
            "doc_id_for_human": "1234-1234-acron-45-9-12345",
        }
        document_items_availability = [
            {
                "type": "asset",
                "id": "1234-1234-acron-45-9-12345-f01",
                "available": False,
                "uri": "https://1234-1234-acron-45-9-12345-f01.jpg",
            },
            {
                "type": "asset",
                "id": "1234-1234-acron-45-9-12345-f02",
                "available": True,
                "uri": "https://1234-1234-acron-45-9-12345-f02.jpg",
            },
        ]
        extra_data = {"chave": "valor", "chave2": "valor 2"}
        expected = [
            {
                "chave": "valor", "chave2": "valor 2",
                "annotation": "",
                "doc_id": "DMKLOLSJ",
                "pid_v2": "S1234-12342020123412345",
                "doc_id_for_human": "1234-1234-acron-45-9-12345",
                "type": "asset",
                "id": "1234-1234-acron-45-9-12345-f01",
                "uri": "https://1234-1234-acron-45-9-12345-f01.jpg",
                "status": "not available",
            },
            {
                "chave": "valor", "chave2": "valor 2",
                "annotation": "",
                "doc_id": "DMKLOLSJ",
                "pid_v2": "S1234-12342020123412345",
                "doc_id_for_human": "1234-1234-acron-45-9-12345",
                "type": "asset",
                "id": "1234-1234-acron-45-9-12345-f02",
                "uri": "https://1234-1234-acron-45-9-12345-f02.jpg",
                "status": "available",
            },
        ]
        result = format_document_items_availability_to_register(
            document_data, document_items_availability, extra_data)
        self.assertEqual(expected, result)


class Testget_kernel_document_id_from_classic_document_uri(TestCase):

    @patch("operations.check_website_operations.do_request")
    def test_get_kernel_document_id_from_classic_document_uri_returns_doc_id(self, mock_do_request):
        classic_uri = "https://new.scielo.br/scielo.php?pid=S1234-12342020123412345&script=sci_arttext"
        response = MockResponse(200)
        response.headers = {
            "Location":
            "https://new.scielo.br/j/axy/a/QP3q9cMWqBGyHjpRsJ6CyVb"}
        mock_do_request.return_value = response
        expected = "QP3q9cMWqBGyHjpRsJ6CyVb"
        result = get_kernel_document_id_from_classic_document_uri(classic_uri)
        self.assertEqual(expected, result)

    @patch("operations.check_website_operations.do_request")
    def test_get_kernel_document_id_from_classic_document_uri_returns_none(self, mock_do_request):
        classic_uri = "https://new.scielo.br/scielo.php?pid=S1234-12342020123412345&script=sci_arttext"
        mock_do_request.return_value = None
        expected = None
        result = get_kernel_document_id_from_classic_document_uri(classic_uri)
        self.assertEqual(expected, result)
