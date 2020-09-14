from unittest import TestCase, skip
from unittest.mock import patch, call, MagicMock
import tempfile
import os
import shutil
import json
from datetime import datetime, timedelta
from csv import writer

import requests

from airflow import DAG

from operations.check_website_operations import (
    concat_website_url_and_uri_list_items,
    check_uri_items,
    check_website_uri_list,
    find_uri_items,
    filter_uri_list,
    get_document_webpage_uri,
    get_document_webpages_data,
    check_asset_uri_items_expected_in_webpage,
    check_doc_webpage_uri_items_expected_in_webpage,
    check_document_webpages_availability,
    check_pdf_webpages_availability,
    check_html_webpages_availability,
    check_document_html,
    check_document_html_content,
    check_document_assets_availability,
    check_document_renditions_availability,
    format_document_webpage_availability_to_register,
    format_document_items_availability_to_register,
    get_kernel_document_id_from_classic_document_uri,
    get_classic_document_webpage_uri,
    do_request,
    is_valid_response,
    check_document_availability,
    get_uri_list_from_pid_dict,
    get_pid_v3_list,
    get_pid_list_from_csv,
    group_items_by_script_name,
    get_journal_issue_doc_pids,
    format_sci_page_availability_result_to_register,
    calculate_missing_and_total,
    fixes_for_json,
    format_document_availability_result_to_register,
    get_status,
    check_website_uri_list_deeply,
    group_doc_data_by_webpage_type,
    add_responses,
)

T5 = datetime.utcnow()
END_TIME = T5 - timedelta(seconds=1)
START_TIME = T5 - timedelta(seconds=3)
T0 = T5 - timedelta(seconds=5)

DURATION = (END_TIME - START_TIME).seconds
T0_to_T5 = (T5 - T0).seconds


def read_file(file_path):
    with open(file_path, "r") as fp:
        c = fp.read()
    return c


class TestDoRequest(TestCase):

    @patch("operations.check_website_operations.datetime")
    @patch("operations.check_website_operations.requests_get")
    def test_do_request_returns_response_timestamp(self, mock_get, mock_dt):
        mock_response = requests.Response()
        mock_response.status_code = 200
        mock_dt.utcnow.side_effect = [START_TIME, END_TIME]
        mock_get.return_value = mock_response
        result = do_request("https://uri.org/8793/")
        self.assertEqual(START_TIME, result.start_time)
        self.assertEqual(END_TIME, result.end_time)

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

    def __init__(self, code, text=None, loc_doc_id=None):
        self.status_code = code
        self.text = text or ""
        self.start_time = START_TIME
        self.end_time = END_TIME
        if loc_doc_id:
            self.headers = {"Location": "/j/acron/a/{}".format(loc_doc_id)}


class MockClientResponse:

    def __init__(self, code, uri, _text=None):
        self.end_time = END_TIME
        self.start_time = START_TIME
        self.status_code = code
        self.uri = uri
        self.text = _text


class MockLogger:

    def __init__(self):
        self._info = []
        self._debug = []

    def info(self, msg):
        self._info.append(msg)

    def debug(self, msg):
        self._debug.append(msg)


class TestCheckUriItems(TestCase):

    @patch('operations.check_website_operations.datetime')
    @patch('operations.check_website_operations.async_requests.parallel_requests')
    def test_check_uri_items_for_status_code_200_returns_success(self, mock_parallel, mock_dt):
        mock_parallel.return_value = [
            MockClientResponse(200, "goodURI1"),
            MockClientResponse(200, "goodURI2"),
        ]
        mock_dt.utcnow.side_effect = [
            START_TIME, END_TIME
        ] * 2
        uri_list = ["goodURI1", "goodURI2", ]
        result = check_uri_items(uri_list)
        expected = [
            {
                "available": True,
                "status code": 200,
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
                "uri": "goodURI1",
            },
            {
                "available": True,
                "status code": 200,
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
                "uri": "goodURI2",
            },
        ]
        self.assertEqual(expected, result[0])
        self.assertEqual([], result[1])

    @patch('operations.check_website_operations.datetime')
    @patch('operations.check_website_operations.async_requests.parallel_requests')
    def test_check_uri_items_for_status_code_301_returns_success(self, mock_parallel_reqs, mock_dt):
        mock_parallel_reqs.return_value = [
            MockClientResponse(301, "URI"),
        ]
        mock_dt.utcnow.side_effect = [
            START_TIME, END_TIME
        ]
        uri_list = ["URI"]
        result = check_uri_items(uri_list)
        expected = [
            {
                "available": True,
                "status code": 301,
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
                "uri": "URI",
            },
        ]
        self.assertEqual(expected, result[0])
        self.assertEqual([], result[1])

    @patch('operations.check_website_operations.datetime')
    @patch('operations.check_website_operations.async_requests.parallel_requests')
    def test_check_uri_items_for_status_code_302_returns_success(self, mock_parallel_reqs, mock_dt):
        mock_parallel_reqs.return_value = [
            MockClientResponse(302, "URI"),
        ]
        mock_dt.utcnow.side_effect = [
            START_TIME, END_TIME
        ]
        uri_list = ["URI"]
        result = check_uri_items(uri_list)
        expected = [
            {
                "available": True,
                "status code": 302,
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
                "uri": "URI",
            },
        ]
        self.assertEqual(expected, result[0])
        self.assertEqual([], result[1])

    @patch('operations.check_website_operations.datetime')
    @patch('operations.check_website_operations.async_requests.parallel_requests')
    def test_check_uri_items_for_status_code_404_returns_failure_list(self, mock_parallel_reqs, mock_dt):
        mock_parallel_reqs.return_value = [
            MockClientResponse(404, "BAD_URI"),
        ]
        mock_dt.utcnow.side_effect = [
            START_TIME, END_TIME
        ]
        uri_list = ["BAD_URI"]
        expected = [
            {
                "available": False,
                "status code": 404,
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
                "uri": "BAD_URI",
            },
        ]
        result = check_uri_items(uri_list)
        self.assertEqual([], result[0])
        self.assertEqual(expected, result[1])

    @patch('operations.check_website_operations.datetime')
    @patch('operations.check_website_operations.async_requests.parallel_requests')
    def test_check_uri_items_for_status_code_500_returns_failure_list(self, mock_parallel_reqs, mock_dt):
        mock_parallel_reqs.return_value = [
            MockClientResponse(404, "BAD_URI"),
        ]
        mock_dt.utcnow.side_effect = [
            START_TIME, END_TIME
        ]
        uri_list = ["BAD_URI"]
        expected = [
            {
                "available": False,
                "status code": 404,
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
                "uri": "BAD_URI",
            },
        ]
        result = check_uri_items(uri_list)
        self.assertEqual([], result[0])
        self.assertEqual(expected, result[1])


class TestCheckWebsiteUriList(TestCase):

    @patch("operations.check_website_operations.Logger.info")
    def test_check_website_uri_list_informs_zero_uri(self, mock_info):
        check_website_uri_list([])
        self.assertEqual(
            mock_info.call_args_list,
            [
                call("Total %s URIs: %i", "", 0),
                call("Total available %s URIs: %i/%i", "", 0, 0),
            ]
        )

    @patch("operations.check_website_operations.Logger.info")
    @patch("operations.check_website_operations.async_requests.parallel_requests")
    def test_check_website_uri_list_informs_that_all_were_found(self, mock_parallel_reqs, mock_info):
        uri_list = (
            "https://www.scielo.br/scielo.php?script=sci_serial&pid=0001-3765",
            "https://www.scielo.br/scielo.php?script=sci_issues&pid=0001-3765",
            "https://www.scielo.br/scielo.php?script=sci_issuetoc&pid=0001-376520200005",
            "https://www.scielo.br/scielo.php?script=sci_arttext&pid=S0001-37652020000501101",
            "https://new.scielo.br/scielo.php?script=sci_serial&pid=0001-3765",
            "https://new.scielo.br/scielo.php?script=sci_issues&pid=0001-3765",
            "https://new.scielo.br/scielo.php?script=sci_issuetoc&pid=0001-376520200005",
            "https://new.scielo.br/scielo.php?script=sci_arttext&pid=S0001-37652020000501101",
        )
        mock_parallel_reqs.return_value = [
            MockClientResponse(200, uri_list[0]),
            MockClientResponse(200, uri_list[1]),
            MockClientResponse(200, uri_list[2]),
            MockClientResponse(200, uri_list[3]),
            MockClientResponse(200, uri_list[4]),
            MockClientResponse(200, uri_list[5]),
            MockClientResponse(200, uri_list[6]),
            MockClientResponse(200, uri_list[7]),
        ]
        check_website_uri_list(uri_list)
        self.assertIn(
            call('Total %s URIs: %i', '', 8),
            mock_info.call_args_list
        )
        self.assertIn(
            call("Total available %s URIs: %i/%i", "", 8, 8),
            mock_info.call_args_list
        )

    @patch("operations.check_website_operations.Logger.info")
    @patch("operations.check_website_operations.async_requests.parallel_requests")
    def test_check_website_uri_list_informs_that_some_of_uri_items_were_not_found(self, mock_parallel_reqs, mock_info):
        uri_list = (
            "https://www.scielo.br/scielo.php?script=sci_serial&pid=0001-3765",
            "https://www.scielo.br/scielo.php?script=sci_issues&pid=0001-3765",
            "https://www.scielo.br/scielo.php?script=sci_issuetoc&pid=0001-376520200005",
            "https://www.scielo.br/scielo.php?script=sci_arttext&pid=S0001-37652020000501101",
            "https://new.scielo.br/scielo.php?script=sci_serial&pid=0001-3765",
            "https://new.scielo.br/scielo.php?script=sci_issues&pid=0001-3765",
            "https://new.scielo.br/scielo.php?script=sci_issuetoc&pid=0001-376520200005",
            "https://new.scielo.br/scielo.php?script=sci_arttext&pid=S0001-37652020000501101",
        )
        mock_parallel_reqs.return_value = [
            MockClientResponse(
                200,
                "https://www.scielo.br/scielo.php?"
                "script=sci_serial&pid=0001-3765"),
            MockClientResponse(
                404,
                "https://www.scielo.br/scielo.php?"
                "script=sci_issues&pid=0001-3765"),
            MockClientResponse(
                200,
                "https://www.scielo.br/scielo.php?"
                "script=sci_issuetoc&pid=0001-376520200005"),
            MockClientResponse(
                200,
                "https://www.scielo.br/scielo.php?"
                "script=sci_arttext&pid=S0001-37652020000501101"),
            MockClientResponse(
                404,
                "https://new.scielo.br/scielo.php?"
                "script=sci_serial&pid=0001-3765"),
            MockClientResponse(
                200,
                "https://new.scielo.br/scielo.php?"
                "script=sci_issues&pid=0001-3765"),
            MockClientResponse(
                200,
                "https://new.scielo.br/scielo.php?"
                "script=sci_issuetoc&pid=0001-376520200005"),
            MockClientResponse(
                200,
                "https://new.scielo.br/scielo.php?"
                "script=sci_arttext&pid=S0001-37652020000501101"),
        ]
        check_website_uri_list(uri_list)
        bad_uri_1 = "https://www.scielo.br/scielo.php?script=sci_issues&pid=0001-3765"
        bad_uri_2 = "https://new.scielo.br/scielo.php?script=sci_serial&pid=0001-3765"

        self.assertIn(
            call("Unavailable %s URIs (%i/%i):\n%s", "", 2, 8,
                 "\n".join([
                        bad_uri_2,
                        bad_uri_1,
                    ])),
            mock_info.call_args_list
        )
        self.assertIn(
            call('Total %s URIs: %i', '', 8),
            mock_info.call_args_list
        )

    def test_check_website_uri_list_returns_0_success_and_0_failures(self):
        result = check_website_uri_list([])
        expected_success = []
        expected_failures = []
        self.assertEqual(expected_success, result[0])
        self.assertEqual(expected_failures, result[1])

    @patch("operations.check_website_operations.async_requests.parallel_requests")
    def test_check_website_uri_list_returns_8_success_and_0_failures(self, mock_parallel_reqs):
        uri_list = (
            "https://www.scielo.br/scielo.php?script=sci_serial&pid=0001-3765",
            "https://www.scielo.br/scielo.php?script=sci_issues&pid=0001-3765",
            "https://www.scielo.br/scielo.php?script=sci_issuetoc&pid=0001-376520200005",
            "https://www.scielo.br/scielo.php?script=sci_arttext&pid=S0001-37652020000501101",
            "https://new.scielo.br/scielo.php?script=sci_serial&pid=0001-3765",
            "https://new.scielo.br/scielo.php?script=sci_issues&pid=0001-3765",
            "https://new.scielo.br/scielo.php?script=sci_issuetoc&pid=0001-376520200005",
            "https://new.scielo.br/scielo.php?script=sci_arttext&pid=S0001-37652020000501101",
        )
        mock_parallel_reqs.return_value = [
            MockClientResponse(200, uri_list[0]),
            MockClientResponse(200, uri_list[1]),
            MockClientResponse(200, uri_list[2]),
            MockClientResponse(200, uri_list[3]),
            MockClientResponse(200, uri_list[4]),
            MockClientResponse(200, uri_list[5]),
            MockClientResponse(200, uri_list[6]),
            MockClientResponse(200, uri_list[7]),
        ]
        result = check_website_uri_list(uri_list)
        expected_success = [
            {
                "available": True,
                "status code": 200,
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
                "uri": uri_list[0]
            },
            {
                "available": True,
                "status code": 200,
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
                "uri": uri_list[1]
            },
            {
                "available": True,
                "status code": 200,
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
                "uri": uri_list[2]
            },
            {
                "available": True,
                "status code": 200,
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
                "uri": uri_list[3]
            },
            {
                "available": True,
                "status code": 200,
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
                "uri": uri_list[4]
            },
            {
                "available": True,
                "status code": 200,
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
                "uri": uri_list[5]
            },
            {
                "available": True,
                "status code": 200,
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
                "uri": uri_list[6]
            },
            {
                "available": True,
                "status code": 200,
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
                "uri": uri_list[7]
            },
        ]
        expected_failures = []
        self.assertEqual(expected_success, result[0])
        self.assertEqual(expected_failures, result[1])

    @patch("operations.check_website_operations.datetime")
    @patch("operations.check_website_operations.async_requests.parallel_requests")
    def test_check_website_uri_list_returns_6_success_and_2_failures_that_some_of_uri_items_were_not_found(self, mock_parallel_reqs, mock_dt):
        uri_list = (
            "https://www.scielo.br/scielo.php?script=sci_serial&pid=0001-3765",
            "https://www.scielo.br/scielo.php?script=sci_issues&pid=0001-3765",
            "https://www.scielo.br/scielo.php?script=sci_issuetoc&pid=0001-376520200005",
            "https://www.scielo.br/scielo.php?script=sci_arttext&pid=S0001-37652020000501101",
            "https://new.scielo.br/scielo.php?script=sci_serial&pid=0001-3765",
            "https://new.scielo.br/scielo.php?script=sci_issues&pid=0001-3765",
            "https://new.scielo.br/scielo.php?script=sci_issuetoc&pid=0001-376520200005",
            "https://new.scielo.br/scielo.php?script=sci_arttext&pid=S0001-37652020000501101",
        )
        mock_dt.utcnow.side_effect = [
            START_TIME, END_TIME,
        ] * 8
        mock_parallel_reqs.return_value = [
            MockClientResponse(
                200,
                "https://www.scielo.br/scielo.php?"
                "script=sci_serial&pid=0001-3765"),
            MockClientResponse(
                404,
                "https://www.scielo.br/scielo.php?"
                "script=sci_issues&pid=0001-3765"),
            MockClientResponse(
                200,
                "https://www.scielo.br/scielo.php?"
                "script=sci_issuetoc&pid=0001-376520200005"),
            MockClientResponse(
                200,
                "https://www.scielo.br/scielo.php?"
                "script=sci_arttext&pid=S0001-37652020000501101"),
            MockClientResponse(
                404,
                "https://new.scielo.br/scielo.php?"
                "script=sci_serial&pid=0001-3765"),
            MockClientResponse(
                200,
                "https://new.scielo.br/scielo.php?"
                "script=sci_issues&pid=0001-3765"),
            MockClientResponse(
                200,
                "https://new.scielo.br/scielo.php?"
                "script=sci_issuetoc&pid=0001-376520200005"),
            MockClientResponse(
                200,
                "https://new.scielo.br/scielo.php?"
                "script=sci_arttext&pid=S0001-37652020000501101"),
        ]
        result = check_website_uri_list(uri_list)
        expected_success = [
            {
                "available": True,
                "status code": 200,
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
                "uri": uri_list[0]
            },
            {
                "available": True,
                "status code": 200,
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
                "uri": uri_list[2]
            },
            {
                "available": True,
                "status code": 200,
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
                "uri": uri_list[3]
            },
            {
                "available": True,
                "status code": 200,
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
                "uri": uri_list[5]
            },
            {
                "available": True,
                "status code": 200,
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
                "uri": uri_list[6]
            },
            {
                "available": True,
                "status code": 200,
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
                "uri": uri_list[7]
            },
        ]
        expected_failures = [
            {
                "available": False,
                "status code": 404,
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
                "uri": uri_list[1]
            },
            {
                "available": False,
                "status code": 404,
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
                "uri": uri_list[4]
            },
        ]
        self.assertEqual(expected_success, result[0])
        self.assertEqual(expected_failures, result[1])


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

    def test_find_uri_items(self):
        expected = [
            "https://minio.scielo.br/documentstore/1809-4457/BrT6FWNFFR3KBKHZVPN8Y9N/8972aaa0916382b6f2d51a6d22732bb083851913.png"
        ]
        result = find_uri_items("""
            <body>
            <img src="https://minio.scielo.br/documentstore/1809-4457/BrT6FWNFFR3KBKHZVPN8Y9N/8972aaa0916382b6f2d51a6d22732bb083851913.png"/>
            </body>
        """)
        self.assertEqual(expected, result)


class TestFilterUriList(TestCase):

    def test_filter_uri_list_returns_(self):
        uri_items = [
            "https://minio.scielo.br/documentstore/1809-4457/BrT6FWNFFR3KBKHZVPN8Y9N/8972aaa0916382b6f2d51a6d22732bb083851913.png"
        ]
        expected = [
            "https://minio.scielo.br/documentstore/1809-4457/BrT6FWNFFR3KBKHZVPN8Y9N/8972aaa0916382b6f2d51a6d22732bb083851913.png"
        ]
        result = filter_uri_list(uri_items, expected_netlocs=["minio.scielo.br"])
        self.assertEqual(expected, result)

    def test_filter_uri_list_returns_path_started_with_slash(self):
        expected = [
            "/j/acron/esa",
        ]
        uri_list = [
            "/j/acron/esa",
            "#end"
        ]
        result = filter_uri_list(uri_list, expected_netlocs=[""])
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
            get_document_webpages_data(doc_id, [{}], get_document_webpage_uri)

    def test_get_document_webpages_data_raises_value_error_if_acron_is_empty_str_and_using_new_uri_pattern(self):
        doc_id = "ldld"
        with self.assertRaises(ValueError):
            get_document_webpages_data(doc_id, [{}], get_document_webpage_uri)

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
        result = get_document_webpages_data(doc_id, doc_data_list, get_classic_document_webpage_uri)
        self.assertEqual(expected, result)


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
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
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
                "total missing components": 0,
                "total expected components": 5,
                "pdf": {"total": 2, "missing": 0},
                "html": {"total": 1, "missing": 0},
                "assets": {
                    "total expected": 2,
                    "total missing": 0,
                    "total alternatives": 6,
                    "total alternatives present in html": 2,
                },
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
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
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
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
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
                "total expected components": 5,
                "total missing components": 0,
                "html": {"total": 1, "missing": 0},
                "pdf": {"total": 2, "missing": 0},
                "assets": {
                    "total expected": 2,
                    "total missing": 0,
                    "total alternatives": 6,
                    "total alternatives present in html": 2,
                },
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
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
            },
        ]
        object_store_url = None
        result, summary = check_document_webpages_availability(
            website_url, doc_data_list, assets_data, object_store_url)
        self.assertDictEqual({
            "web html":
                {"total": 2, "total unavailable": 0, "total incomplete": 0, },
            "web pdf":
                {"total": 2, "total unavailable": 0},
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
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
                "components": [
                    {
                        "type": "pdf",
                        "id": "en",
                        "present_in_html": [
                            "/j/xjk/a/ldld?format=pdf&lang=en",
                        ],
                    },
                ],
                "total missing components": 0,
                "total expected components": 1,
                "pdf": {"total": 1, "missing": 0},
                "assets": {
                    "total expected": 0,
                    "total missing": 0,
                    "total alternatives": 0,
                    "total alternatives present in html": 0,
                },
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
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
            },
        ]
        object_store_url = None
        result, summary = check_document_webpages_availability(
            website_url, doc_data_list, assets_data, object_store_url)
        self.assertDictEqual(
            {
                "web html": {"total": 1, "total unavailable": 0, "total incomplete": 0},
                "web pdf": {"total": 1, "total unavailable": 1},  
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
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
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
                "total expected components": 1,
                "total missing components": 1,
                "pdf": {"total": 1, "missing": 1},
                "assets": {
                    "total expected": 0,
                    "total missing": 0,
                    "total alternatives": 0,
                    "total alternatives present in html": 0,
                },
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
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
            },
        ]
        object_store_url = None
        result, summary = check_document_webpages_availability(
            website_url, doc_data_list, assets_data, object_store_url)
        self.assertDictEqual({
                "web html": {"total": 1, "total unavailable": 0, "total incomplete": 1},
                "web pdf": {"total": 1, "total unavailable": 0},  
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
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
                "components": [
                    {
                        "type": "html",
                        "id": "es",
                        "present_in_html": [
                            "/j/xjk/a/ldld?format=html&lang=es",
                        ],
                    },
                ],
                "total missing components": 0,
                "total expected components": 1,
                "html": {"total": 1, "missing": 0},
                "assets": {
                    "total expected": 0,
                    "total missing": 0,
                    "total alternatives": 0,
                    "total alternatives present in html": 0,
                },
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
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
                "total missing components": 1,
                "total expected components": 1,
            },
        ]
        object_store_url = None
        result, summary = check_document_webpages_availability(
            website_url, doc_data_list, assets_data, object_store_url)
        self.assertDictEqual(
            {
                "web html": {
                    "total": 2, "total unavailable": 1, "total incomplete": 0},
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
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
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
                "total expected components": 1,
                "total missing components": 1,
                "html": {"total": 1, "missing": 1},
                "assets": {
                    "total expected": 0,
                    "total missing": 0,
                    "total alternatives": 0,
                    "total alternatives present in html": 0,
                },
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
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
                "components": [
                    {
                        "type": "html",
                        "id": "en",
                        "present_in_html": [
                            "/j/xjk/a/ldld?format=html",
                        ],
                    },
                ],
                "total expected components": 1,
                "total missing components": 0,
                "html": {"total": 1, "missing": 0},
                "assets": {
                    "total expected": 0,
                    "total missing": 0,
                    "total alternatives": 0,
                    "total alternatives present in html": 0,
                },
            },
        ]
        object_store_url = None
        result, summary = check_document_webpages_availability(
            website_url, doc_data_list, assets_data, object_store_url)
        self.assertDictEqual(
            {
            "web html":
                {"total": 2, "total unavailable": 0, "total incomplete": 1, },
            },
            summary
        )
        self.assertListEqual(expected, result)


class TestCheckDocumentHtml(TestCase):

    @patch("operations.check_website_operations.datetime")
    @patch("operations.check_website_operations.requests.get")
    def test_check_document_html_returns_not_available(self, mock_get, mock_dt):
        mock_get.return_value = None
        mock_dt.utcnow.side_effect = [START_TIME, END_TIME]
        uri = "https://..."
        assets_data = []
        other_webpages_data = []
        expected = {
            "available": False, "status code": None,
            "start time": START_TIME,
            "end time": END_TIME,
            "duration": DURATION,
            "total expected components": 0,
            "total missing components": 0,
        }
        object_store_url = None
        result = check_document_html(
            uri, assets_data, other_webpages_data, object_store_url)
        self.assertEqual(expected, result)

    @patch("operations.check_website_operations.datetime")
    @patch("operations.check_website_operations.requests.get")
    def test_check_document_html_returns_available_and_empty_components(self, mock_get, mock_dt):
        mock_response = MockResponse(200, "")
        mock_get.return_value = mock_response
        mock_dt.utcnow.side_effect = [START_TIME, END_TIME]
        uri = "https://..."
        assets_data = []
        other_webpages_data = []
        expected = {
            "available": True,
            "status code": 200,
            "start time": START_TIME,
            "end time": END_TIME,
            "duration": DURATION,
            "components": [],
            "total missing components": 0,
            "total expected components": 0,
            "assets": {
                "total expected": 0,
                "total missing": 0,
                "total alternatives": 0,
                "total alternatives present in html": 0,
            },
        }
        object_store_url = None
        result = check_document_html(
            uri, assets_data, other_webpages_data, object_store_url)
        self.assertEqual(expected, result)

    @patch("operations.check_website_operations.datetime")
    @patch("operations.check_website_operations.requests.get")
    def test_check_document_html_returns_available_and_components_are_absent(self, mock_get, mock_dt):
        mock_response = MockResponse(200, "")
        mock_get.return_value = mock_response
        mock_dt.utcnow.side_effect = [START_TIME, END_TIME]
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
            "start time": START_TIME,
            "end time": END_TIME,
            "duration": DURATION,
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
            "total missing components": 2,
            "total expected components": 2,
            "html": {"total": 1, "missing": 1},
            "assets": {
                "total expected": 1,
                "total missing": 1,
                "total alternatives": 3,
                "total alternatives present in html": 0,
            },
            "existing_uri_items_in_html": []
        }
        object_store_url = None
        result = check_document_html(
            uri, assets_data, other_webpages_data, object_store_url)
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
        mock_dt.utcnow.side_effect = [START_TIME, END_TIME]

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
            "start time": START_TIME,
            "end time": END_TIME,
            "duration": DURATION,
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
            "total missing components": 0,
            "total expected components": 2,
            "html": {"total": 1, "missing": 0},
            
            "assets": {
                "total expected": 1,
                "total missing": 0,
                "total alternatives": 3,
                "total alternatives present in html": 1,
            },
        }
        object_store_url = None
        result = check_document_html(
            uri, assets_data, other_webpages_data, object_store_url)
        self.assertEqual(expected, result)


class TestCheckDocumentHtmlContent(TestCase):

    def test_check_document_html_content_returns_not_available(self):
        assets_data = []
        other_webpages_data = []
        expected = {
            "total expected components": 0,
            "total missing components": 0,
        }
        object_store_url = None
        result = check_document_html_content(
            None, assets_data, other_webpages_data, object_store_url)
        self.assertEqual(expected, result)

    def test_check_document_html_content_returns_available_and_empty_components(self):
        assets_data = []
        other_webpages_data = []
        expected = {
            "components": [],
            "total missing components": 0,
            "total expected components": 0,
            "assets": {
                "total expected": 0,
                "total missing": 0,
                "total alternatives": 0,
                "total alternatives present in html": 0,
            },
        }
        object_store_url = None
        result = check_document_html_content(
            "", assets_data, other_webpages_data, object_store_url)
        self.assertEqual(expected, result)

    def test_check_document_html_content_returns_available_and_components_are_absent(self):
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
            "total missing components": 2,
            "total expected components": 2,
            "html": {"total": 1, "missing": 1},
            "assets": {
                "total expected": 1,
                "total missing": 1,
                "total alternatives": 3,
                "total alternatives present in html": 0,
            },
            "existing_uri_items_in_html": []
        }
        object_store_url = None
        result = check_document_html_content(
            "", assets_data, other_webpages_data, object_store_url)
        self.assertEqual(expected, result)

    def test_check_document_html_content_returns_available_and_components_are_present(self):
        content = """
        <img src="asset_uri_1.jpg"/>
        <a href="/j/xjk/a/ldld?lang=en"/>
        """
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
            "total missing components": 0,
            "total expected components": 2,
            "html": {"total": 1, "missing": 0},
            "assets": {
                "total expected": 1,
                "total missing": 0,
                "total alternatives": 3,
                "total alternatives present in html": 1,
            },
        }
        object_store_url = None
        result = check_document_html_content(
            content, assets_data, other_webpages_data, object_store_url)
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
             "start time": START_TIME,
             "end time": END_TIME,
             "duration": DURATION,
            },
            {"asset_id": "a01.jpg",
             "uri": "uri de a01.jpg no object store",
             "available": False,
             "status code": None,
             "start time": START_TIME,
             "end time": END_TIME,
             "duration": DURATION,
            },
            {"asset_id": "a02.png",
             "uri": "uri de a02.png no object store",
             "available": True,
             "status code": 200,
             "start time": START_TIME,
             "end time": END_TIME,
             "duration": DURATION,
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
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
            },
            {
                "lang": "en",
                "uri": "uri de original-en.pdf  no object store",
                "available": False,
                "status code": None,
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
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
            "total missing components": 1,
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


class TestGetKernelDocumentIdFromClassicDocumentUri(TestCase):

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


class TestCheckDocumentAvailability(TestCase):

    def get_document_manifest_pt(self):
        return """{
          "id": "BrT6FWNFFR3KBKHZVPN8Y9N",
          "versions": [
            {
              "data": "https://minio.scielo.br/documentstore/1809-4457/BrT6FWNFFR3KBKHZVPN8Y9N/580326b45763246eb9eaaac55e660a7aaa3ff686.xml",
              "assets": {
                "1809-4457-esa-s1413-41522020182506-gf1.png": [
                  [
                    "2020-08-10T11:38:46.759190Z",
                    "https://minio.scielo.br/documentstore/1809-4457/BrT6FWNFFR3KBKHZVPN8Y9N/8972aaa0916382b6f2d51a6d22732bb083851913.png"
                  ]
                ],
                "1809-4457-esa-s1413-41522020182506-gf1.thumbnail.jpg": [
                  [
                    "2020-08-10T11:38:46.759305Z",
                    "https://minio.scielo.br/documentstore/1809-4457/BrT6FWNFFR3KBKHZVPN8Y9N/3c30f9fec6947d47f404043fe08aaca8bc51b1fb.jpg"
                  ]
                ],
                "1809-4457-esa-s1413-41522020182506-gf2.png": [
                  [
                    "2020-08-10T11:38:46.759419Z",
                    "https://minio.scielo.br/documentstore/1809-4457/BrT6FWNFFR3KBKHZVPN8Y9N/36080074121a60c8e28fa1b28876e1adad4fe5d7.png"
                  ]
                ],
                "1809-4457-esa-s1413-41522020182506-gf2.thumbnail.jpg": [
                  [
                    "2020-08-10T11:38:46.759568Z",
                    "https://minio.scielo.br/documentstore/1809-4457/BrT6FWNFFR3KBKHZVPN8Y9N/b5b4bb9bc267794ececde428a33f5af705b0b1a6.jpg"
                  ]
                ],
                "1809-4457-esa-s1413-41522020182506-gf3.png": [
                  [
                    "2020-08-10T11:38:46.759706Z",
                    "https://minio.scielo.br/documentstore/1809-4457/BrT6FWNFFR3KBKHZVPN8Y9N/73a98051b6cf623aeb1146017ceb0b947df75ec8.png"
                  ]
                ],
                "1809-4457-esa-s1413-41522020182506-gf3.thumbnail.jpg": [
                  [
                    "2020-08-10T11:38:46.759859Z",
                    "https://minio.scielo.br/documentstore/1809-4457/BrT6FWNFFR3KBKHZVPN8Y9N/df14e57dc001993fd7f3fbcefa642e40e6964224.jpg"
                  ]
                ]
              },
              "timestamp": "2020-08-10T11:38:46.758253Z",
              "renditions": [
                {
                  "filename": "1809-4457-esa-s1413-41522020182506.pdf",
                  "data": [
                    {
                      "timestamp": "2020-08-10T11:38:47.049682Z",
                      "url": "https://minio.scielo.br/documentstore/1809-4457/BrT6FWNFFR3KBKHZVPN8Y9N/409acdeb8f632022d41b3d94a3f00a837867937c.pdf",
                      "size_bytes": 289032
                    }
                  ],
                  "mimetype": "application/pdf",
                  "lang": "pt"
                }
              ]
            }
          ],
          "_id": "BrT6FWNFFR3KBKHZVPN8Y9N"
        }
        """

    @patch("operations.check_website_operations.datetime")
    @patch("operations.check_website_operations.async_requests.parallel_requests")
    @patch("operations.check_website_operations.requests.get")
    @patch("operations.docs_utils.hooks.kernel_connect")
    def test_check_document_availability_returns_doc_is_totally_complete(
            self, mock_doc_manifest, mock_get, mock_request, mock_dt):
        doc_id = "BrT6FWNFFR3KBKHZVPN8Y9N"
        website_url = "https://www.scielo.br"
        object_store_url = "https://minio.scielo.br"
        mock_doc_manifest.return_value = read_file(
            "./tests/fixtures/BrT6FWNFFR3KBKHZVPN8Y9N.manifest.json")
        mock_get.return_value = MockResponse(
                200,
                read_file(
                    "./tests/fixtures/BrT6FWNFFR3KBKHZVPN8Y9N.xml")
                )
        mock_request.side_effect = [
            [
                MockClientResponse(
                    200,
                    "https://www.scielo.br/j/esa/a/BrT6FWNFFR3KBKHZVPN8Y9N"
                    "?format=html&lang=pt",
                    read_file(
                        "./tests/fixtures/BrT6FWNFFR3KBKHZVPN8Y9N_pt.html"
                    )
                ),
            ],
            [
                MockClientResponse(
                    200,
                    "https://www.scielo.br/j/esa/a/BrT6FWNFFR3KBKHZVPN8Y9N"
                    "?format=pdf&lang=pt"
                ),
            ],
        ]

        mock_dt.utcnow.side_effect = [T0] + ([START_TIME, END_TIME] * 7) + [T5]

        web_html_availability = [{
            "lang": "pt",
            "format": "html",
            "pid_v2": "S1413-41522020005004201",
            "acron": "esa",
            "doc_id_for_human": "1809-4457-esa-ahead-S1413-41522020182506",
            "doc_id": "BrT6FWNFFR3KBKHZVPN8Y9N",
            "uri": "https://www.scielo.br/j/esa/a/BrT6FWNFFR3KBKHZVPN8Y9N?format=html&lang=pt",
            "available": True,
            "status code": 200,
            "start time": START_TIME,
            "end time": END_TIME,
            "duration": DURATION,
            "components": [
                {
                    "type": "asset",
                    "id": "1809-4457-esa-s1413-41522020182506-gf1",
                    "present_in_html": [
                        "https://minio.scielo.br/documentstore/1809-4457/"
                        "BrT6FWNFFR3KBKHZVPN8Y9N/"
                        "8972aaa0916382b6f2d51a6d22732bb083851913.png",
                    ],
                    "absent_in_html": [
                        "https://minio.scielo.br/documentstore/1809-4457/"
                        "BrT6FWNFFR3KBKHZVPN8Y9N/"
                        "3c30f9fec6947d47f404043fe08aaca8bc51b1fb.jpg",
                    ],
                },
                {
                    "type": "asset",
                    "id": "1809-4457-esa-s1413-41522020182506-gf2",
                    "present_in_html": [
                    ],
                    "absent_in_html": [
                        "https://minio.scielo.br/documentstore/1809-4457/"
                        "BrT6FWNFFR3KBKHZVPN8Y9N/"
                        "36080074121a60c8e28fa1b28876e1adad4fe5d7.png",
                        "https://minio.scielo.br/documentstore/1809-4457/"
                        "BrT6FWNFFR3KBKHZVPN8Y9N/"
                        "b5b4bb9bc267794ececde428a33f5af705b0b1a6.jpg",
                    ],
                },
                {
                    "type": "asset",
                    "id": "1809-4457-esa-s1413-41522020182506-gf3",
                    "present_in_html": [
                    ],
                    "absent_in_html": [
                        "https://minio.scielo.br/documentstore/1809-4457/"
                        "BrT6FWNFFR3KBKHZVPN8Y9N/"
                        "73a98051b6cf623aeb1146017ceb0b947df75ec8.png",
                        "https://minio.scielo.br/documentstore/1809-4457/"
                        "BrT6FWNFFR3KBKHZVPN8Y9N/"
                        "df14e57dc001993fd7f3fbcefa642e40e6964224.jpg",
                    ],
                },
                {
                    "type": "pdf",
                    "id": "pt",
                    "present_in_html": [
                        "/j/esa/a/BrT6FWNFFR3KBKHZVPN8Y9N/?format=pdf&lang=pt",
                    ],
                },
            ],
            "total missing components": 2,
            "total expected components": 4,
            "pdf": {"total": 1, "missing": 0},
            "assets": {
                "total expected": 3,
                "total missing": 2,
                "total alternatives": 6,
                "total alternatives present in html": 1,
            },
            "existing_uri_items_in_html": [
                '/about/',
                '/j/esa/',
                '/j/esa/a/BrT6FWNFFR3KBKHZVPN8Y9N/?format=pdf&lang=pt',
                '/j/esa/a/BrT6FWNFFR3KBKHZVPN8Y9N/?lang=pt',
                '/j/esa/a/S1413-41522020005004201/?format=pdf&lang=pt',
                '/j/esa/a/TWyHMQBS4H6tyrXPZhcWxps/',
                '/j/esa/a/nhg9DgZSsvnhXjq7qCN7cvc/',
                '/j/esa/i/9999.nahead/',
                '/journal/esa/about/#about',
                '/journal/esa/about/#contact',
                '/journal/esa/about/#editors',
                '/journal/esa/about/#instructions',
                '/journal/esa/feed/',
                '/journals/alpha?status=current',
                '/journals/thematic?status=current',
                '/media/images/esa_glogo.gif',
                '/set_locale/es/',
                '/set_locale/pt_BR/',
                '/static/img/oa_logo_32.png',
                '/static/js/scielo-article-min.js',
                '/static/js/scielo-bundle-min.js',
                '/static/js/scienceopen.js',
                'https://minio.scielo.br/documentstore/1809-4457/BrT6FWNFFR3KBKHZVPN8Y9N/8972aaa0916382b6f2d51a6d22732bb083851913.png'
            ],
        }]
        web_pdf_availability = [{
            "lang": "pt",
            "format": "pdf",
            "pid_v2": "S1413-41522020005004201",
            "acron": "esa",
            "doc_id_for_human": "1809-4457-esa-ahead-S1413-41522020182506",
            "doc_id": "BrT6FWNFFR3KBKHZVPN8Y9N",
            "uri": "https://www.scielo.br/j/esa/a/BrT6FWNFFR3KBKHZVPN8Y9N?format=pdf&lang=pt",
            "available": True,
            "status code": 200,
            "start time": START_TIME,
            "end time": END_TIME,
            "duration": DURATION,
        }]
        # https://minio.scielo.br/documentstore/1809-4457/BrT6FWNFFR3KBKHZVPN8Y9N/409acdeb8f632022d41b3d94a3f00a837867937c.pdf
        renditions_availability = [
            {
                "lang": "pt",
                "uri": (
                    "https://minio.scielo.br/documentstore/1809-4457/"
                    "BrT6FWNFFR3KBKHZVPN8Y9N/"
                    "409acdeb8f632022d41b3d94a3f00a837867937c.pdf"
                ),
                "available": True,
                "status code": 200,
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
            }
        ]
        assets_availability = [
            {
                "asset_id": "1809-4457-esa-s1413-41522020182506-gf1.png",
                "uri": (
                    "https://minio.scielo.br/documentstore/1809-4457/"
                    "BrT6FWNFFR3KBKHZVPN8Y9N/"
                    "8972aaa0916382b6f2d51a6d22732bb083851913.png"
                ),
                "available": True,
                "status code": 200,
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
            },
            {
                "asset_id": "1809-4457-esa-s1413-41522020182506-gf1.thumbnail.jpg",
                "uri": (
                    "https://minio.scielo.br/documentstore/1809-4457/"
                    "BrT6FWNFFR3KBKHZVPN8Y9N/"
                    "3c30f9fec6947d47f404043fe08aaca8bc51b1fb.jpg"
                ),
                "available": True,
                "status code": 200,
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
            },
            {
                "asset_id": "1809-4457-esa-s1413-41522020182506-gf2.png",
                "uri": (
                    "https://minio.scielo.br/documentstore/1809-4457/"
                    "BrT6FWNFFR3KBKHZVPN8Y9N/"
                    "36080074121a60c8e28fa1b28876e1adad4fe5d7.png"
                ),
                "available": True,
                "status code": 200,
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
            },
            {
                "asset_id": "1809-4457-esa-s1413-41522020182506-gf2.thumbnail.jpg",
                "uri": (
                    "https://minio.scielo.br/documentstore/1809-4457/"
                    "BrT6FWNFFR3KBKHZVPN8Y9N/"
                    "b5b4bb9bc267794ececde428a33f5af705b0b1a6.jpg"
                ),
                "available": True,
                "status code": 200,
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
            },
            {
                "asset_id": "1809-4457-esa-s1413-41522020182506-gf3.png",
                "uri": (
                    "https://minio.scielo.br/documentstore/1809-4457/"
                    "BrT6FWNFFR3KBKHZVPN8Y9N/"
                    "73a98051b6cf623aeb1146017ceb0b947df75ec8.png"
                ),
                "available": True,
                "status code": 200,
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
            },
            {
                "asset_id": "1809-4457-esa-s1413-41522020182506-gf3.thumbnail.jpg",
                "uri": (
                    "https://minio.scielo.br/documentstore/1809-4457/"
                    "BrT6FWNFFR3KBKHZVPN8Y9N/"
                    "df14e57dc001993fd7f3fbcefa642e40e6964224.jpg"
                ),
                "available": True,
                "status code": 200,
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
            },
        ]
        expected = {
            "summary": {
                "web html": {
                    "total": 1, "total unavailable": 0, "total incomplete": 1},
                "web pdf": {
                    "total": 1, "total unavailable": 0},
                "renditions": {
                    "total": 1, "total unavailable": 0},
                "assets": {
                    "total": 6, "total unavailable": 0},
                "processing":
                    {"start": T0, "end": T5, "duration": T0_to_T5}
            },
            "detail": {
                "web html": web_html_availability,
                "web pdf": web_pdf_availability,
                "renditions": renditions_availability,
                "assets": assets_availability,
            },
        }
        result = check_document_availability(doc_id, website_url, object_store_url)
        self.assertDictEqual(expected["summary"], result["summary"])

        self.assertListEqual(
            expected["detail"]["renditions"],
            result["detail"]["renditions"]
        )
        self.assertListEqual(
            expected["detail"]["assets"],
            result["detail"]["assets"]
        )

        _expected = expected["detail"]["web html"][0]
        _result = result["detail"]["web html"][0]

        for name in ("total missing components", "total expected components", "existing_uri_items_in_html"):
            with self.subTest(name):
                self.assertEqual(
                    _expected[name],
                    _result[name]
                )

        for i in range(4):
            with self.subTest(i):
                self.assertDictEqual(
                    _expected["components"][i],
                    _result["components"][i]
                )


class TestGetUriListFromGroupedPIDs(TestCase):

    def test_get_uri_list_from_pid_dict_returns_classic_website_uri_list(self):
        expected = [
            "/scielo.php?script=sci_serial&pid=2234-5679",
            "/scielo.php?script=sci_issues&pid=2234-5679",
            "/scielo.php?script=sci_issuetoc&pid=2234-567919970010",
            "/scielo.php?script=sci_arttext&pid=S2234-56791997001012305",
            "/scielo.php?script=sci_pdf&pid=S2234-56791997001012305",
            "/scielo.php?script=sci_arttext&pid=S2234-56791997001002315",
            "/scielo.php?script=sci_pdf&pid=S2234-56791997001002315",
            "/scielo.php?script=sci_serial&pid=1234-5678",
            "/scielo.php?script=sci_issues&pid=1234-5678",
            "/scielo.php?script=sci_issuetoc&pid=1234-567819870001",
            "/scielo.php?script=sci_arttext&pid=S1234-56781987000112305",
            "/scielo.php?script=sci_pdf&pid=S1234-56781987000112305",
            "/scielo.php?script=sci_arttext&pid=S1234-56781987000102315",
            "/scielo.php?script=sci_pdf&pid=S1234-56781987000102315",
            "/scielo.php?script=sci_arttext&pid=S1234-56781987000112345",
            "/scielo.php?script=sci_pdf&pid=S1234-56781987000112345",
            "/scielo.php?script=sci_arttext&pid=S1234-56781987000102345",
            "/scielo.php?script=sci_pdf&pid=S1234-56781987000102345",
        ]
        grouped_pids = {
            "2234-5679":
                {
                    "2234-567919970010": [
                        "S2234-56791997001012305",
                        "S2234-56791997001002315",
                    ]
                },
            "1234-5678":
                {
                    "1234-567819870001": [
                        "S1234-56781987000112305",
                        "S1234-56781987000102315",
                        "S1234-56781987000112345",
                        "S1234-56781987000102345",
                    ]
                },
        }
        result = get_uri_list_from_pid_dict(grouped_pids)
        self.assertListEqual(expected, result)


class TestGetPIDv3List(TestCase):

    @patch("operations.check_website_operations.requests.head")
    def test_get_pid_v3_list_returns_pid_v3_list_and_website_url_for_all_items(self, mock_head):
        responses = []
        for doc_id in (None, "S1Y3X-5678198700010Y3X5", "S123X-56k8198k0001023X5", "S1PLX-5678198700010PLX5"):
            status_code = 301
            if not doc_id:
                status_code = 404
            responses.append(MockResponse(status_code, loc_doc_id=doc_id))

        mock_head.side_effect = responses
        uri_items = [
            "/scielo.php?script=sci_arttext&pid=S1234-56781987000112305",
            "/scielo.php?script=sci_arttext&pid=S1234-56781987000102315",
            "/scielo.php?script=sci_arttext&pid=S1234-56781987000112345",
        ]
        website_url_list = ["https://old.scielo.br", "https://www.scielo.br", ]
        pid_list, website_url = get_pid_v3_list(uri_items, website_url_list)
        expected = [
            "S1Y3X-5678198700010Y3X5",
            "S123X-56k8198k0001023X5",
            "S1PLX-5678198700010PLX5",
        ]
        self.assertListEqual(expected, pid_list)
        self.assertEqual("https://www.scielo.br", website_url)

    @patch("operations.check_website_operations.requests.head")
    def test_get_pid_v3_list_returns_pid_v3_list_and_website_url_for_two_of_three_items(self, mock_head):
        responses = []
        for doc_id in (None, None, None, "S123X-56k8198k0001023X5", "S1PLX-5678198700010PLX5"):
            status_code = 301
            if not doc_id:
                status_code = 404
            responses.append(MockResponse(status_code, loc_doc_id=doc_id))

        mock_head.side_effect = responses
        uri_items = [
            "/scielo.php?script=sci_arttext&pid=S1234-56781987000112305",
            "/scielo.php?script=sci_arttext&pid=S1234-56781987000102315",
            "/scielo.php?script=sci_arttext&pid=S1234-56781987000112345",
        ]
        website_url_list = ["https://old.scielo.br", "https://www.scielo.br", ]
        pid_list, website_url = get_pid_v3_list(uri_items, website_url_list)
        expected = [
            "S123X-56k8198k0001023X5",
            "S1PLX-5678198700010PLX5",
        ]
        self.assertListEqual(expected, pid_list)
        self.assertEqual("https://www.scielo.br", website_url)


class TestGetPIDListFromCSV(TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.dir)

    def test_get_pid_list_from_csv_returns_previous_pid(self):
        data = [
            ("S1234-56781987000112305", "S1234-56781986005012305"),
            ("S1234-56781987000112306", ""),
            ("S1234-56781987000112308", "S1234-56781986005012307"),
            ("S1234-56781987000112309", "S1234-56781986005012313"),
        ]
        csv_file_path = os.path.join(self.dir, 'docs.csv')
        with open(csv_file_path, 'w', newline='') as fp:
            w = writer(fp)
            for row in data:
                w.writerow(row)
        expected = [
            "S1234-56781987000112305",
            "S1234-56781986005012305",
            "S1234-56781987000112306",
            "S1234-56781987000112308",
            "S1234-56781986005012307",
            "S1234-56781987000112309",
            "S1234-56781986005012313",
        ]

        result = get_pid_list_from_csv(csv_file_path)
        self.assertListEqual(expected, result)

    def test_get_pid_list_from_csv_reads_one_column_csv_file(self):
        data = [
            ("S1234-56781987000112305", ),
            ("S1234-56781987000112306", ),
            ("S1234-56781987000112308", ),
            ("S1234-56781987000112309", ),
        ]
        csv_file_path = os.path.join(self.dir, 'docs.csv')
        with open(csv_file_path, 'w', newline='') as fp:
            w = writer(fp)
            for row in data:
                w.writerow(row)
        expected = [
            "S1234-56781987000112305",
            "S1234-56781987000112306",
            "S1234-56781987000112308",
            "S1234-56781987000112309",
        ]

        result = get_pid_list_from_csv(csv_file_path)
        self.assertListEqual(expected, result)

    def test_get_pid_list_from_csv_reads_more_than_one_column_csv_file(self):
        data = [
            ("S1234-56781987000112305", "bla", "xyo", "98765-02"),
            ("S1234-56781987000112306", "bla", "xyo", "98765-02"),
            ("S1234-56781987000112308", "bla", "xyo", "98765-02"),
            ("S1234-56781987000112309", "bla", "xyo", "98765-02"),
        ]
        csv_file_path = os.path.join(self.dir, 'docs.csv')
        with open(csv_file_path, 'w', newline='') as fp:
            w = writer(fp)
            for row in data:
                w.writerow(row)
        expected = [
            "S1234-56781987000112305",
            "S1234-56781987000112306",
            "S1234-56781987000112308",
            "S1234-56781987000112309",
        ]

        result = get_pid_list_from_csv(csv_file_path)
        self.assertListEqual(expected, result)


class Testgroup_items_by_script_name(TestCase):

    def test_group_items_by_script_name(self):
        uri_items = [
            "/scielo.php?script=sci_serial&pid=2234-5679",
            "/scielo.php?script=sci_issues&pid=2234-5679",
            "/scielo.php?script=sci_issuetoc&pid=2234-567919970010",
            "/scielo.php?script=sci_arttext&pid=S2234-56791997001012305",
            "/scielo.php?script=sci_pdf&pid=S2234-56791997001012305",
            "/scielo.php?script=sci_arttext&pid=S2234-56791997001002315",
            "/scielo.php?script=sci_pdf&pid=S2234-56791997001002315",
            "/scielo.php?script=sci_serial&pid=1234-5678",
            "/scielo.php?script=sci_issues&pid=1234-5678",
            "/scielo.php?script=sci_issuetoc&pid=1234-567819870001",
            "/scielo.php?script=sci_arttext&pid=S1234-56781987000112305",
            "/scielo.php?script=sci_pdf&pid=S1234-56781987000112305",
            "/scielo.php?script=sci_arttext&pid=S1234-56781987000102315",
            "/scielo.php?script=sci_pdf&pid=S1234-56781987000102315",
            "/scielo.php?script=sci_arttext&pid=S1234-56781987000112345",
            "/scielo.php?script=sci_pdf&pid=S1234-56781987000112345",
            "/scielo.php?script=sci_arttext&pid=S1234-56781987000102345",
            "/scielo.php?script=sci_pdf&pid=S1234-56781987000102345",
        ]
        expected = {
            "sci_serial":
            [
                "/scielo.php?script=sci_serial&pid=2234-5679",
                "/scielo.php?script=sci_serial&pid=1234-5678",
            ],
            "sci_issues":
            [
                "/scielo.php?script=sci_issues&pid=2234-5679",
                "/scielo.php?script=sci_issues&pid=1234-5678",
            ],
            "sci_issuetoc":
            [
                "/scielo.php?script=sci_issuetoc&pid=2234-567919970010",
                "/scielo.php?script=sci_issuetoc&pid=1234-567819870001",
            ],
            "sci_arttext":
            [
                "/scielo.php?script=sci_arttext&pid=S2234-56791997001012305",
                "/scielo.php?script=sci_arttext&pid=S2234-56791997001002315",
                "/scielo.php?script=sci_arttext&pid=S1234-56781987000112305",
                "/scielo.php?script=sci_arttext&pid=S1234-56781987000102315",
                "/scielo.php?script=sci_arttext&pid=S1234-56781987000112345",
                "/scielo.php?script=sci_arttext&pid=S1234-56781987000102345",
            ],
            "sci_pdf":
            [
                "/scielo.php?script=sci_pdf&pid=S2234-56791997001012305",
                "/scielo.php?script=sci_pdf&pid=S2234-56791997001002315",
                "/scielo.php?script=sci_pdf&pid=S1234-56781987000112305",
                "/scielo.php?script=sci_pdf&pid=S1234-56781987000102315",
                "/scielo.php?script=sci_pdf&pid=S1234-56781987000112345",
                "/scielo.php?script=sci_pdf&pid=S1234-56781987000102345",
            ],
        }
        result = group_items_by_script_name(uri_items)
        self.assertDictEqual(expected, result)


class TestGetJournalIssueDocPids(TestCase):

    def test_get_journal_issue_doc_pids_returns_all_pids(self):
        expected = "2234-5679", "2234-567919970010", "S2234-56791997001012305"
        data = "/scielo.php?script=sci_pdf&pid=S2234-56791997001012305"
        result = get_journal_issue_doc_pids(data)
        self.assertEqual(expected, result)

    def test_get_journal_issue_doc_pids_returns_no_pids(self):
        expected = None, None, None
        data = "/scielo.php?script=sci_pdf&pid="
        result = get_journal_issue_doc_pids(data)
        self.assertEqual(expected, result)

    def test_get_journal_issue_doc_pids_returns_journal_pid(self):
        expected = "2234-5679", None, None
        data = "/scielo.php?script=sci_xxx&pid=2234-5679"
        result = get_journal_issue_doc_pids(data)
        self.assertEqual(expected, result)

    def test_get_journal_issue_doc_pids_returns_journal_and_issue_pids(self):
        expected = "2234-5679", "2234-567919970010", None
        data = "/scielo.php?script=sci_xxx&pid=2234-567919970010"
        result = get_journal_issue_doc_pids(data)
        self.assertEqual(expected, result)


class TestFormatSciPageAvailabilityResultToRegister(TestCase):

    def test_format_sci_page_availability_result_to_register_returns_data(self):
        data = {
            "available": True,
            "status code": 200,
            "start time": 1,
            "end time": 2,
            "duration": 1,
            "uri":  (
                "https://www.scielo.br/scielo.php?"
                "script=sci_xxx&pid=2234-567919970010"
            ),
        }
        dag_data = {"run_id": "RUN ID", "input_file_name": "a.csv"}
        expected = {
            "dag_run": "RUN ID",
            "input_file_name": "a.csv",
            "uri": (
                "https://www.scielo.br/scielo.php?"
                "script=sci_xxx&pid=2234-567919970010"
            ),
            "failed": False,
            "detail": '{"available": true, "status code": 200, '
                      '"start time": 1, "end time": 2, "duration": 1, '
                      '"uri": "https://www.scielo.br/scielo.php?script='
                      'sci_xxx&pid=2234-567919970010"}',
            "pid_v2_journal": "2234-5679",
            "pid_v2_issue": "2234-567919970010",
            "pid_v2_doc": None,
        }
        result = format_sci_page_availability_result_to_register(
            data, dag_data)
        self.assertEqual(expected, result)


class TestCheckDocWebpageUriItemsExpectedInWebpage(TestCase):

    def test_check_doc_webpage_uri_items_expected_in_webpage_returns_success(self):
        uri_items_in_html = [
            "asset_uri_1.jpg",
            "asset_uri_2.jpg",
            "/j/xyz/a/lokiujyht?format=html&lang=en",
            "/j/xyz/a/lokiujyht?format=pdf&lang=es",
            "/j/xyz/a/lokiujyht?format=pdf&lang=en",
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
        expected_formats_result = {
            "html": {"total": 1, "missing": 0},
            "pdf": {"total": 2, "missing": 0},
        }
        result, formats_result = check_doc_webpage_uri_items_expected_in_webpage(
            uri_items_in_html, other_webpages_uri_data)
        self.assertEqual(expected, result)
        self.assertEqual(expected_formats_result, formats_result)

    def test_check_doc_webpage_uri_items_expected_in_webpage_returns_not_found_pdf(self):
        uri_items_in_html = [
            "/j/xyz/a/lokiujyht?format=html&lang=en",
            "/j/xyz/a/lokiujyht?format=pdf&lang=en",
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
        expected_formats_result = {
            "html": {"total": 1, "missing": 0},
            "pdf": {"total": 2, "missing": 1},
        }
        result, formats_result = check_doc_webpage_uri_items_expected_in_webpage(
            uri_items_in_html, other_webpages_uri_data)
        self.assertEqual(expected, result)
        self.assertEqual(expected_formats_result, formats_result)


    def test_check_doc_webpage_uri_items_expected_in_webpage_returns_not_found_html(self):
        uri_items_in_html = [
            "/j/xyz/a/lokiujyht?format=pdf&lang=es",
            "/j/xyz/a/lokiujyht?format=pdf&lang=en",
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
        expected_formats_result = {
            "html": {"total": 1, "missing": 1},
            "pdf": {"total": 2, "missing": 0},
        }
        result, formats_result = check_doc_webpage_uri_items_expected_in_webpage(
            uri_items_in_html, other_webpages_uri_data)
        self.assertEqual(expected, result)
        self.assertEqual(expected_formats_result, formats_result)


class TestCheckAssetUriItemsExpectedInWebpage(TestCase):

    def test_check_asset_uri_items_expected_in_webpage_returns_success(self):
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
        ]
        expected_summary = {
            "total expected": 2,
            "total missing": 0,
            "total alternatives": 6,
            "total alternatives present in html": 2,
        }
        result, summary = check_asset_uri_items_expected_in_webpage(
                    uri_items_in_html,
                    assets_data)
        self.assertEqual(expected, result)
        self.assertEqual(expected_summary, summary)

    def test_check_asset_uri_items_expected_in_webpage_returns_not_found_asset_uri(self):
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
        expected_summary = {
            "total expected": 2,
            "total missing": 1,
            "total alternatives": 6,
            "total alternatives present in html": 1,
        }
        result, summary = check_asset_uri_items_expected_in_webpage(
                    uri_items_in_html,
                    assets_data)
        self.assertEqual(expected, result)
        self.assertEqual(expected_summary, summary)


class TestCalculateMissingAndTotalItems(TestCase):
    def test_calculate_missing_and_total_returns_no_missing(self):
        expected = {
            "html": {
                "total": 1,
                "missing": 0,
            },
            "pdf": {"total": 2, "missing": 0},
        }
        data = {
            "html": [True],
            "pdf": [True, True],
        }
        result = calculate_missing_and_total(data)
        self.assertDictEqual(expected, result)

    def test_calculate_missing_and_total_returns_all_missing(self):
        expected = {
            "html": {
                "total": 1,
                "missing": 1,
            },
            "pdf": {"total": 2, "missing": 2},
        }
        data = {
            "html": [False],
            "pdf": [False, False],
        }
        result = calculate_missing_and_total(data)
        self.assertDictEqual(expected, result)

    def test_calculate_missing_and_total_returns_some_missing(self):
        expected = {
            "html": {
                "total": 2,
                "missing": 1,
            },
            "pdf": {"total": 1, "missing": 1},
        }
        data = {
            "html": [False, True],
            "pdf": [False],
        }
        result = calculate_missing_and_total(data)
        self.assertDictEqual(expected, result)


class TestFixesForJSON(TestCase):

    def test_fixes_for_json_converts_datetime_to_isoformat(self):
        dt = datetime.utcnow()
        dt_iso = dt.isoformat() + "Z"
        expected = (
            '{"date": "%s", "s": "bla", "number": 9, "float": 8.5}' % dt_iso
        )
        j = {"date": dt, "s": "bla", "number": 9, "float": 8.5}
        r = json.dumps(j, default=fixes_for_json)
        self.assertEqual(expected, r)


class TestFormatDocumentAvailabilityResultToRegister(TestCase):

    def test_format_document_availability_result_to_register_executed_with_success(self):
        dt = datetime.utcnow()
        dt_iso = dt.isoformat() + "Z"
        doc_id = "1234"
        doc_checkup_result = {
            "date": dt, "s": "bla", "number": 9, "float": 8.5
        }
        dag_info = {"run_id": "988", "input_file_name": "filename.csv"}
        expected = {
            "dag_run": "988",
            "input_file_name": "filename.csv",
            "pid_v3": "1234",
            "status": "missing",
            "detail": (
                '{"date": "%s", "s": "bla", "number": 9, "float": 8.5}' %
                dt_iso),
        }

        result = format_document_availability_result_to_register(
            doc_id, doc_checkup_result, dag_info)
        self.assertEqual(expected, result)


class TestGetStatus(TestCase):

    def test_get_status_returns_partial_because_total_incomplete_is_not_zero(self):
        summary = {
          "web html": {
             "total": 1,
             "total unavailable": 0,
             "total incomplete": 1
          },
          "web pdf": {
             "total": 1,
             "total unavailable": 1
          },
          "renditions": {
             "total": 1,
             "total unavailable": 0
          },
          "assets": {
             "total": 6,
             "total unavailable": 0
          },
          "processing": {
             "start": "t0",
             "end": "t3",
             "duration": 5
          }
        }
        result = get_status(summary)
        self.assertEqual("partial", result)

    def test_get_status_returns_partial_because_total_unavailable_is_not_zero(self):
        summary = {
          "web html": {
             "total": 1,
             "total unavailable": 0,
             "total incomplete": 0
          },
          "web pdf": {
             "total": 1,
             "total unavailable": 1
          },
          "renditions": {
             "total": 1,
             "total unavailable": 0
          },
          "assets": {
             "total": 6,
             "total unavailable": 1
          },
          "processing": {
             "start": "t0",
             "end": "t3",
             "duration": 5
          }
        }
        result = get_status(summary)
        self.assertEqual("partial", result)

    def test_get_status_returns_complete_because_total_unavailable_and_total_incomplete_are_zero(self):
        summary = {
          "web html": {
             "total": 1,
             "total unavailable": 0,
             "total incomplete": 0
          },
          "web pdf": {
             "total": 1,
             "total unavailable": 0
          },
          "renditions": {
             "total": 1,
             "total unavailable": 0
          },
          "assets": {
             "total": 6,
             "total unavailable": 0
          },
          "processing": {
             "start": "t0",
             "end": "t3",
             "duration": 5
          }
        }
        result = get_status(summary)
        self.assertEqual("complete", result)

    def test_get_status_returns_missing_because_total_is_zero(self):
        summary = {
          "web html": {
             "total": 0,
             "total unavailable": 0,
             "total incomplete": 0
          },
          "web pdf": {
             "total": 0,
             "total unavailable": 0
          },
          "renditions": {
             "total": 0,
             "total unavailable": 0
          },
          "assets": {
             "total": 0,
             "total unavailable": 0
          },
          "processing": {
             "start": "t0",
             "end": "t3",
             "duration": 5
          }
        }
        result = get_status(summary)
        self.assertEqual("missing", result)

    def test_get_status_returns_missing_because_total_is_equal_to_total_unavailable(self):
        summary = {
          "web html": {
             "total": 1,
             "total unavailable": 1,
             "total incomplete": 0
          },
          "web pdf": {
             "total": 1,
             "total unavailable": 1
          },
          "renditions": {
             "total": 1,
             "total unavailable": 1
          },
          "assets": {
             "total": 1,
             "total unavailable": 1
          },
          "processing": {
             "start": "t0",
             "end": "t3",
             "duration": 5
          }
        }
        result = get_status(summary)
        self.assertEqual("missing", result)

    def test_get_status_returns_missing_because_summary_is_empty(self):
        summary = {}
        result = get_status(summary)
        self.assertEqual("missing", result)


class TestCheckWebsiteUriListDeeply(TestCase):

    @patch("operations.check_website_operations.async_requests.parallel_requests")
    @patch("operations.check_website_operations.add_execution_in_database")
    @patch("operations.check_website_operations.datetime")
    @patch("operations.check_website_operations.requests.head")
    @patch("operations.check_website_operations.requests.get")
    @patch("operations.docs_utils.hooks.kernel_connect")
    def test_check_website_uri_list_deeply_calls(self, mock_doc_manifest,
            mock_get, mock_head, mock_dt, mock_add_execution_in_database,
            mock_parallel_req):
        doc_id = "BrT6FWNFFR3KBKHZVPN8Y9N"
        website_url = "https://www.scielo.br"
        object_store_url = "https://minio.scielo.br"
        mock_doc_manifest.return_value = read_file(
            "./tests/fixtures/BrT6FWNFFR3KBKHZVPN8Y9N.manifest.json")
        mock_get.return_value = MockResponse(
                200,
                read_file(
                    "./tests/fixtures/BrT6FWNFFR3KBKHZVPN8Y9N.xml"))

        mock_parallel_req.side_effect = [
            [
                MockClientResponse(
                    200,
                    "https://www.scielo.br/j/esa/a/BrT6FWNFFR3KBKHZVPN8Y9N"
                    "?format=html&lang=pt",
                    read_file(
                        "./tests/fixtures/BrT6FWNFFR3KBKHZVPN8Y9N_pt.html")
                )
            ],
            [
                MockClientResponse(
                    200,
                    "https://www.scielo.br/j/esa/a/BrT6FWNFFR3KBKHZVPN8Y9N"
                    "?format=pdf&lang=pt"
                )
            ],
        ]
        mock_head.side_effect = [MockResponse(200)] * 8
        mock_dt.utcnow.side_effect = [T0] + ([START_TIME, END_TIME] * 7) + [T5]

        doc_id_list = [doc_id]
        dag_info = {"run_id": "xxxx"}

        t0 = T0.isoformat() + "Z"
        t1 = START_TIME.isoformat() + "Z"
        t2 = END_TIME.isoformat() + "Z"
        t3 = T5.isoformat() + "Z"

        detail = read_file(
                    "./tests/fixtures/BrT6FWNFFR3KBKHZVPN8Y9N.inline.json")
        expected_calls = [
            call("doc_deep_checkup", {
                    "dag_run": "xxxx",
                    "input_file_name": None,
                    "pid_v3": "BrT6FWNFFR3KBKHZVPN8Y9N",
                    "pid_v2_journal": "1413-4152",
                    "pid_v2_issue": "1413-415220200050",
                    "pid_v2_doc": "S1413-41522020005004201",
                    "previous_pid_v2_doc": None,
                    "status": "partial",
                    "detail": detail.replace("t0", t0).replace(
                        "t1", t1).replace("t2", t2).replace("t3", t3)
                    }),
        ]
        check_website_uri_list_deeply(
            doc_id_list, website_url, object_store_url, dag_info)
        self.assertEqual(mock_get.call_count, 1)
        self.assertEqual(mock_head.call_count, 7)
        self.assertEqual(mock_dt.utcnow.call_count, 16)
        self.assertListEqual(
            expected_calls,
            mock_add_execution_in_database.call_args_list
        )


class TestGroupDocDataByWebpageType(TestCase):

    def test_group_doc_data_by_webpage_type(self):
        doc_webpages_data = [
            {"doc_id": "x", "data": "2", "format": "html"},
            {"doc_id": "x", "data": "1", "format": "pdf"},
            {"doc_id": "x", "data": "b", "format": "html"},
            {"doc_id": "x", "data": "a", "format": "pdf"},
        ]
        expected = {
            "web html": [
                {"doc_id": "x", "data": "2", "format": "html"},
                {"doc_id": "x", "data": "b", "format": "html"},
            ],
            "web pdf": [
                {"doc_id": "x", "data": "1", "format": "pdf"},
                {"doc_id": "x", "data": "a", "format": "pdf"},
            ],
        }
        result = group_doc_data_by_webpage_type(doc_webpages_data)
        self.assertDictEqual(expected, result)


class TestCheckHtmlWebpagesAvailability(TestCase):

    def test_check_html_webpages_availability_returns_success(self):
        responses = [
            MockClientResponse(
                200,
                "https://www.scielo.br/j/xjk/a/ldld?format=html&lang=en"
            ),
            MockClientResponse(
                200,
                "https://www.scielo.br/j/xjk/a/ldld?format=html&lang=es"
            ),
        ]
        responses[0].text = """
            <a href="asset_uri_2.jpg"/>
            <a href="asset_uri_1.jpg"/>
            <a href="/j/xjk/a/ldld?format=pdf&lang=es"/>
            <a href="/j/xjk/a/ldld?format=pdf&lang=en"/>
            <a href="/j/xjk/a/ldld?format=html&lang=es"/>
            """
        responses[1].text = """
            <a href="asset_uri_2.jpg"/>
            <a href="asset_uri_1.jpg"/>
            <a href="/j/xjk/a/ldld?format=pdf&lang=es"/>
            <a href="/j/xjk/a/ldld?format=pdf&lang=en"/>
            <a href="/j/xjk/a/ldld?format=html&lang=en"/>
            """
        doc_data_list = [
            {
                "lang": "en",
                "format": "html",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "https://www.scielo.br/j/xjk/a/ldld?format=html&lang=en",
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
                "response": responses[0]
            },
            {
                "lang": "en",
                "format": "pdf",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "https://www.scielo.br/j/xjk/a/ldld?format=pdf&lang=en",
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
                "uri": "https://www.scielo.br/j/xjk/a/ldld?format=html&lang=es",
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
                "response": responses[1]
            },
            {
                "lang": "es",
                "format": "pdf",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "https://www.scielo.br/j/xjk/a/ldld?format=pdf&lang=es",
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
        html_data_list = [
            doc_data_list[0],
            doc_data_list[2],
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
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
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
                "total missing components": 0,
                "total expected components": 5,
                "pdf": {"total": 2, "missing": 0},
                "html": {"total": 1, "missing": 0},
                "assets": {
                    "total expected": 2,
                    "total missing": 0,
                    "total alternatives": 6,
                    "total alternatives present in html": 2,
                },
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
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
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
                "total expected components": 5,
                "total missing components": 0,
                "html": {"total": 1, "missing": 0},
                "pdf": {"total": 2, "missing": 0},
                "assets": {
                    "total expected": 2,
                    "total missing": 0,
                    "total alternatives": 6,
                    "total alternatives present in html": 2,
                },
            },
        ]
        object_store_url = None
        result, summary = check_html_webpages_availability(
            html_data_list, assets_data, doc_data_list,
            object_store_url)
        self.assertDictEqual(
            {"total": 2, "total unavailable": 0, "total incomplete": 0, },
            summary
        )
        self.assertListEqual(expected, result)

    def test_check_html_webpages_availability_returns_pdf_is_present_in_html(self):
        response = MockClientResponse(
            200, "https://www.scielo.br/j/xjk/a/ldld?format=pdf&lang=en")
        response.text = """
        <a href="/j/xjk/a/ldld?format=pdf&lang=en"/>
        """
        doc_data_list = [
            {
                "lang": "en",
                "format": "html",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "https://www.scielo.br/j/xjk/a/ldld?format=html&lang=en",
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
                "response": response,
            },
            {
                "lang": "en",
                "format": "pdf",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "https://www.scielo.br/j/xjk/a/ldld?format=pdf&lang=en",
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
        html_data_list = [doc_data_list[0]]
        assets_data = [
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
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
                "components": [
                    {
                        "type": "pdf",
                        "id": "en",
                        "present_in_html": [
                            "/j/xjk/a/ldld?format=pdf&lang=en",
                        ],
                    },
                ],
                "total missing components": 0,
                "total expected components": 1,
                "pdf": {"total": 1, "missing": 0},
                "assets": {
                    "total expected": 0,
                    "total missing": 0,
                    "total alternatives": 0,
                    "total alternatives present in html": 0,
                },
            },
        ]
        object_store_url = None
        result, summary = check_html_webpages_availability(
            html_data_list, assets_data, doc_data_list, object_store_url)
        self.assertDictEqual(
            {"total": 1, "total unavailable": 0, "total incomplete": 0},
            summary
        )
        self.assertListEqual(expected, result)

    def test_check_html_webpages_availability_returns_pdf_it_is_not_present_in_html(self):
        response = MockClientResponse(
                200, "https://www.scielo.br/j/xjk/a/ldld?format=html&lang=en",
                "")
        doc_data_list = [
            {
                "lang": "en",
                "format": "html",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "https://www.scielo.br/j/xjk/a/ldld?format=html&lang=en",
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
                "response": response,
            },
            {
                "lang": "en",
                "format": "pdf",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "https://www.scielo.br/j/xjk/a/ldld?format=pdf&lang=en",
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
        html_data_list = [doc_data_list[0]]
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
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
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
                "total expected components": 1,
                "total missing components": 1,
                "pdf": {"total": 1, "missing": 1},
                "assets": {
                    "total expected": 0,
                    "total missing": 0,
                    "total alternatives": 0,
                    "total alternatives present in html": 0,
                },
                "existing_uri_items_in_html": [],
            },
        ]
        object_store_url = None
        result, summary = check_html_webpages_availability(
            html_data_list, assets_data, doc_data_list, object_store_url)
        self.assertDictEqual({
                "total": 1, "total unavailable": 0, "total incomplete": 1
            },
            summary
        )
        self.assertListEqual(expected, result)

    def test_check_html_webpages_availability_returns_html_es_is_not_available_but_it_is_present_in_html_en(self):
        responses = [
            MockClientResponse(200,
                "https://www.scielo.br/j/xjk/a/ldld?format=html&lang=en",
                """
                Versão ingles no formato html
                <a href="/j/xjk/a/ldld?format=html&lang=es"/>
                """
            ),
            MockClientResponse(
                None, "https://www.scielo.br/j/xjk/a/ldld?format=html&lang=es")
        ]
        html_data_list = [
            {
                "lang": "en",
                "format": "html",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "https://www.scielo.br/j/xjk/a/ldld?format=html&lang=en",
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
                "response": responses[0]
            },
            {
                "lang": "es",
                "format": "html",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "https://www.scielo.br/j/xjk/a/ldld?format=html&lang=es",
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
                "response": responses[1]
            },
        ]
        doc_data_list = html_data_list.copy()
        assets_data = [
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
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
                "components": [
                    {
                        "type": "html",
                        "id": "es",
                        "present_in_html": [
                            "/j/xjk/a/ldld?format=html&lang=es",
                        ],
                    },
                ],
                "total missing components": 0,
                "total expected components": 1,
                "html": {"total": 1, "missing": 0},
                "assets": {
                    "total expected": 0,
                    "total missing": 0,
                    "total alternatives": 0,
                    "total alternatives present in html": 0,
                },
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
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
                "total missing components": 1,
                "total expected components": 1,
            },
        ]
        object_store_url = None
        result, summary = check_html_webpages_availability(
            html_data_list, assets_data, doc_data_list, object_store_url)
        self.assertDictEqual(
            {"total": 2, "total unavailable": 1, "total incomplete": 0},
            summary
        )
        self.assertListEqual(expected, result)

    def test_check_html_webpages_availability_returns_html_es_is_available_but_it_is_not_present_in_html_en(self):
        responses = [
            MockClientResponse(
                200,
                "https://www.scielo.br/j/xjk/a/ldld?format=html&lang=en",
                "documento sem links, conteúdo do html em Ingles"),
            MockClientResponse(
                200,
                "https://www.scielo.br/j/xjk/a/ldld?format=html&lang=es",
                """
                conteúdo do documento em espanhol com link para a versão ingles
                    <a href="/j/xjk/a/ldld?format=html"/>
                """
            ),
        ]
        html_data_list = [
            {
                "lang": "en",
                "format": "html",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "https://www.scielo.br/j/xjk/a/ldld?format=html&lang=en",
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
                "response": responses[0],
            },
            {
                "lang": "es",
                "format": "html",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "https://www.scielo.br/j/xjk/a/ldld?format=html&lang=es",
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
                "response": responses[1],
            },
        ]
        doc_data_list = html_data_list.copy()
        assets_data = [
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
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
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
                "total expected components": 1,
                "total missing components": 1,
                "html": {"total": 1, "missing": 1},
                "assets": {
                    "total expected": 0,
                    "total missing": 0,
                    "total alternatives": 0,
                    "total alternatives present in html": 0,
                },
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
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
                "components": [
                    {
                        "type": "html",
                        "id": "en",
                        "present_in_html": [
                            "/j/xjk/a/ldld?format=html",
                        ],
                    },
                ],
                "total expected components": 1,
                "total missing components": 0,
                "html": {"total": 1, "missing": 0},
                "assets": {
                    "total expected": 0,
                    "total missing": 0,
                    "total alternatives": 0,
                    "total alternatives present in html": 0,
                },
            },
        ]
        object_store_url = None
        result, summary = check_html_webpages_availability(
            html_data_list, assets_data, doc_data_list, object_store_url)
        self.assertDictEqual(
            {"total": 2, "total unavailable": 0, "total incomplete": 1, },
            summary
        )
        self.assertListEqual(expected, result)


class TestCheckPdfWebpagesAvailability(TestCase):

    def test_check_pdf_webpages_availability_returns_success(self):
        responses = [
            MockClientResponse(
                200, "https://www.scielo.br/j/xjk/a/ldld?format=pdf&lang=en"),
            MockClientResponse(
                200, "https://www.scielo.br/j/xjk/a/ldld?format=pdf&lang=es"),
        ]
        doc_data_list = [
            {
                "lang": "en",
                "format": "pdf",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "https://www.scielo.br/j/xjk/a/ldld?format=pdf&lang=en",
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
                "response": responses[0],
            },
            {
                "lang": "es",
                "format": "pdf",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "https://www.scielo.br/j/xjk/a/ldld?format=pdf&lang=es",
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
                "response": responses[1],
            },
        ]
        expected = [
            {
                "lang": "en",
                "format": "pdf",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "https://www.scielo.br/j/xjk/a/ldld?format=pdf&lang=en",
                "available": True,
                "status code": 200,
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
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
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
            },
        ]
        result, summary = check_pdf_webpages_availability(doc_data_list)
        self.assertDictEqual(
            {"total": 2, "total unavailable": 0},
            summary
        )
        self.assertListEqual(expected, result)

    def test_check_pdf_webpages_availability_returns_pdf_is_not_available(self):
        doc_data_list = [
            {
                "lang": "en",
                "format": "pdf",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "https://www.scielo.br/j/xjk/a/ldld?format=pdf&lang=en",
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
                "response": MockClientResponse(
                    None,
                    "https://www.scielo.br/j/xjk/a/ldld?format=pdf&lang=en")
            },
        ]
        expected = [
            {
                "lang": "en",
                "format": "pdf",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "https://www.scielo.br/j/xjk/a/ldld?format=pdf&lang=en",
                "available": False,
                "status code": None,
                "start time": START_TIME,
                "end time": END_TIME,
                "duration": DURATION,
            },
        ]
        result, summary = check_pdf_webpages_availability(doc_data_list)
        self.assertDictEqual(
            {"total": 1, "total unavailable": 1},
            summary
        )
        self.assertListEqual(expected, result)


class TestAddResponses(TestCase):

    @patch('operations.check_website_operations.async_requests.parallel_requests')
    def test_add_responses_returns_data_with_complete_uri_and_responses(self, mock_parallel_requests):
        doc_data_list = [
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
                "format": "pdf",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "/j/xjk/a/ldld?format=pdf&lang=es",
            },
        ]
        website_url = "https://www.scielo.br"
        response1 = MockClientResponse(
                200, "https://www.scielo.br/j/xjk/a/ldld?format=pdf&lang=en")
        response2 = MockClientResponse(
                200, "https://www.scielo.br/j/xjk/a/ldld?format=pdf&lang=es")

        mock_parallel_requests.return_value = [
            response1, response2
        ]
        expected = [
            {
                "lang": "en",
                "format": "pdf",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "https://www.scielo.br/j/xjk/a/ldld?format=pdf&lang=en",
                "response": response1,
            },
            {
                "lang": "es",
                "format": "pdf",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "https://www.scielo.br/j/xjk/a/ldld?format=pdf&lang=es",
                "response": response2,
            },
        ]
        result = add_responses(doc_data_list, website_url)
        self.assertEqual(expected, result)

    @patch('operations.check_website_operations.async_requests.parallel_requests')
    def test_add_responses_returns_data_with_responses(self, mock_parallel_requests):
        doc_data_list = [
           {
                "lang": "en",
                "format": "pdf",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "https://www.scielo.br/j/xjk/a/ldld?format=pdf&lang=en",
            },
            {
                "lang": "es",
                "format": "pdf",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "https://www.scielo.br/j/xjk/a/ldld?format=pdf&lang=es",
            },
        ]
        response1 = MockClientResponse(
                200, "https://www.scielo.br/j/xjk/a/ldld?format=pdf&lang=en")
        response2 = MockClientResponse(
                200, "https://www.scielo.br/j/xjk/a/ldld?format=pdf&lang=es")

        mock_parallel_requests.return_value = [
            response1, response2
        ]
        expected = [
            {
                "lang": "en",
                "format": "pdf",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "https://www.scielo.br/j/xjk/a/ldld?format=pdf&lang=en",
                "response": response1,
            },
            {
                "lang": "es",
                "format": "pdf",
                "pid_v2": "pid-v2", "acron": "xjk",
                "doc_id_for_human": "artigo-1234",
                "doc_id": "ldld",
                "uri": "https://www.scielo.br/j/xjk/a/ldld?format=pdf&lang=es",
                "response": response2,
            },
        ]
        result = add_responses(doc_data_list)
        self.assertEqual(expected, result)


class TestFormatDocumentAvailabilityResultToRegister(TestCase):

    def test_format_document_availability_result_to_register(self):
        doc_id = "DOC123"
        file_content = read_file(
            "./tests/fixtures/BrT6FWNFFR3KBKHZVPN8Y9N.inline.json")
        doc_checkup_result = json.loads(file_content)
        dag_info = {"run_id": "RUN ID", "whatever": "whatever"}
        expected = {
            "dag_run": "RUN ID",
            "input_file_name": None,
            "pid_v3": "DOC123",
            "pid_v2_journal": "1413-4152",
            "pid_v2_issue": "1413-415220200050",
            "pid_v2_doc": "S1413-41522020005004201",
            "previous_pid_v2_doc": None,
            "status": "partial",
            "detail": file_content,
        }
        result = format_document_availability_result_to_register(
            doc_id, doc_checkup_result, dag_info)
        self.assertDictEqual(expected, result)
