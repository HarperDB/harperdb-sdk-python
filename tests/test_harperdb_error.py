import responses
import requests

import harperdb
import harperdb_testcase


class TestHarperDBError(harperdb_testcase.HarperDBTestCase):

    @responses.activate
    def test_harperdb_error_messages(self):
        """ HarperDBErrors contain the original error message.
        """
        # mock server error to create_schema request
        responses.add(
            'POST',
            self.URL,
            json=self.LOGIN_FAILED,
            status=401)

        db = harperdb.HarperDB(self.URL)
        with self.assertRaises(harperdb.HarperDBError) as assertion_error:
            db.create_schema('test_schema_1')
        self.assertEqual(
            assertion_error.exception.args[0],
            self.LOGIN_FAILED['error'])
        self.assertEqual(len(responses.calls), 1)

    def test_request_exceptions_are_raised(self):
        """ RequestException is raised for connection errors.
        """
        db = harperdb.HarperDB(self.URL)
        # without activating responses, the connection will be refused
        with self.assertRaises(requests.exceptions.RequestException):
            db.create_schema('test_schema_1')
