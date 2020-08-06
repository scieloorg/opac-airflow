from unittest import TestCase
from unittest.mock import patch, call

from airflow import DAG

from operations.check_website_operations import (
    concat_website_url_and_uri_list_items,
    check_uri_list,
    check_website_uri_list,
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

    @patch("operations.check_website_operations.Logger.info")
    @patch("operations.check_website_operations.requests.head")
    @patch("operations.check_website_operations.read_file")
    def test_check_website_uri_list_inform_that_all_were_found(self, mock_read_file, mock_head, mock_info):
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
    def test_check_website_uri_list_inform_that_some_of_uri_items_were_not_found(self, mock_read_file, mock_head, mock_info):
        mock_read_file.return_value = (
            "/scielo.php?script=sci_serial&pid=0001-3765\n"
            "/scielo.php?script=sci_issues&pid=0001-3765\n"
            "/scielo.php?script=sci_issuetoc&pid=0001-376520200005\n"
            "/scielo.php?script=sci_arttext&pid=S0001-37652020000501101\n"
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
