import json
import responses
import requests
import unittest

import harperdb
import harperdb_testcase


class TestHarperDB(harperdb_testcase.HarperDBTestCase):

    def setUp(self):
        """ This method is called before each test.
        """
        self.db = harperdb.HarperDB(self.URL)

    @unittest.mock.patch('base64.b64encode')
    def test_create_harperdb_with_kwargs(self, mock_b64encode):
        """ Create an instance of HarperDB with keyword args.
        """
        # by mocking the base64 module we can define what it returns,
        # so it's very easy to check the value stored in db.token
        mock_b64encode.return_value = b'anArbitraryBase64String'

        db = harperdb.HarperDB(
            self.URL,
            self.USERNAME,
            self.PASSWORD,
            timeout=3)

        mock_b64encode.assert_called_once_with(
            '{}:{}'.format(self.USERNAME, self.PASSWORD).encode('utf-8'))
        self.assertEqual(db.url, self.URL)
        self.assertEqual(
            db.token,
            'Basic {}'.format(mock_b64encode.return_value.decode('utf-8')))
        self.assertEqual(db.timeout, 3)

    @responses.activate
    def test_create_schema(self):
        # define the expected JSON body in POST request
        spec = {
            'operation': 'create_schema',
            'schema': 'test_schema',
        }
        # mock the server response to create_schema
        responses.add(
            'POST',
            self.URL,
            json={
                'message': 'successfully created schema',
            },
            status=200)

        self.db.create_schema(spec['schema'])

        self.assertLastRequestMatchesSpec(spec)
        self.assertEqual(len(responses.calls), 1)

    @responses.activate
    def test_drop_schema(self):
        # define the expected JSON body in POST request
        spec = {
            'operation': 'drop_schema',
            'schema': 'test_schema',
        }
        # mock the server response to drop_schema
        responses.add(
            'POST',
            self.URL,
            json={
                'message': 'successfully deleted schema',
            },
            status=200)

        self.db.drop_schema(spec['schema'])

        self.assertLastRequestMatchesSpec(spec)
        self.assertEqual(len(responses.calls), 1)

    @responses.activate
    def test_describe_schema(self):
        # define the expected JSON body in POST request
        spec = {
            'operation': 'describe_schema',
            'schema': 'test_schema',
        }
        # mock the server response to describe_schema
        responses.add(
            'POST',
            self.URL,
            json=[
                {
                    '__createdtime__': 1234567890000,
                    '__updatedtime__': 1234567890001,
                    'hash_attribute': 'id',
                    'id': 'assignedUUID',
                    'name': 'test_table',
                    'residence': None,
                    'schema': 'test_schema',
                    'attributes': [
                        {
                            'attribute': '__createdtime__'
                        },
                        {
                            'attribute': '__updatedtime__'
                        },
                        {
                            'attribute': 'id'
                        }
                    ],
                    'record_count': 0
                }
            ],
            status=200)

        self.db.describe_schema(spec['schema'])

        self.assertLastRequestMatchesSpec(spec)
        self.assertEqual(len(responses.calls), 1)

    @responses.activate
    def test_create_table(self):
        # define the expected JSON body in POST request
        spec = {
            'operation': 'create_table',
            'schema': 'test_schema',
            'table': 'test_table',
            'hash_attribute': 'id',
        }
        # mock the server response to drop_schema
        responses.add(
            'POST',
            self.URL,
            json={
                'message': 'successfully created table',
            },
            status=200)

        self.db.create_table(
            schema=spec['schema'],
            table=spec['table'],
            hash_attribute=spec['hash_attribute'])

        self.assertLastRequestMatchesSpec(spec)
        self.assertEqual(len(responses.calls), 1)

    @responses.activate
    def test_describe_table(self):
        # define the expected JSON body in POST request
        spec = {
            'operation': 'describe_table',
            'schema': 'test_schema',
            'table': 'test_table',
        }
        # mock the server response to describe_table
        responses.add(
            'POST',
            self.URL,
            json={
                '__createdtime__': 1234567890000,
                '__updatedtime__': 1234567890001,
                'hash_attribute': 'id',
                'id': 'assignedUUID',
                'name': 'test_table',
                'residence': None,
                'schema': 'test_schema',
                'attributes': [
                    {
                        'attribute': '__createdtime__'
                    },
                    {
                        'attribute': '__updatedtime__'
                    },
                    {
                        'attribute': 'id'
                    }
                ],
                'record_count': 0
            },
            status=200)

        self.db.describe_table(spec['schema'], spec['table'])
        self.assertLastRequestMatchesSpec(spec)
        self.assertEqual(len(responses.calls), 1)

    @responses.activate
    def test_describe_all(self):
        # define the expected JSON body in POST request
        spec = {
            'operation': 'describe_all'
        }
        # mock the server response to describe_all
        responses.add(
            'POST',
            self.URL,
            json={
                'test_schema': {
                    'test_table': {
                        '__createdtime__': 1234567890000,
                        '__updatedtime__': 1234567890001,
                        'hash_attribute': 'id',
                        'id': 'assignedUUID',
                        'name': 'test_table',
                        'residence': None,
                        'schema': 'test_schema',
                        'attributes': [
                            {
                                'attribute': '__createdtime__'
                            },
                            {
                                'attribute': '__updatedtime__'
                            },
                            {
                                'attribute': 'id'
                            }
                        ],
                        'record_count': 0
                    }
                }
            },
            status=200)

        self.db.describe_all()

        self.assertLastRequestMatchesSpec(spec)
        self.assertEqual(len(responses.calls), 1)

    @responses.activate
    def test_drop_table(self):
        # define the expected JSON body in POST request
        spec = {
            'operation': 'drop_table',
            'schema': 'test_schema',
            'table': 'test_table',
        }
        # mock the server response
        responses.add(
            'POST',
            self.URL,
            json={
                'message': 'successfully deleted table',
            },
            status=200)

        self.db.drop_table(schema=spec['schema'], table=spec['table'])

        self.assertLastRequestMatchesSpec(spec)
        self.assertEqual(len(responses.calls), 1)

    @responses.activate
    def test_drop_attribute(self):
        """ Drop an attribute from a table.
        """
        # define the expected JSON body in POST request
        spec = {
            'operation': 'drop_attribute',
            'schema': 'test_schema',
            'table': 'test_table',
            'attribute': 'test_attribute',
        }
        # mock the server response
        responses.add(
            'POST',
            self.URL,
            json={
                'message': 'successfully deleted table',
            },
            status=200)

        self.db.drop_attribute(
            schema=spec['schema'],
            table=spec['table'],
            attribute=spec['attribute'])

        self.assertLastRequestMatchesSpec(spec)
        self.assertEqual(len(responses.calls), 1)

    @responses.activate
    def test_insert(self):
        """ Insert a list of records.
        """
        # define the expected JSON body in POST request
        spec = {
            'operation': 'insert',
            'schema': 'test_schema',
            'table': 'test_table',
            'records': [
                {
                    'id': 1,
                    'name': 'foo',
                },
            ],
        }
        # mock the server response
        responses.add(
            'POST',
            self.URL,
            json={
                'message': 'inserted 1 of 1 records',
                'skipped_hashes': [],
                'inserted_hashes': [
                    1,
                ],
            },
            status=200)

        self.db.insert(
            schema=spec['schema'],
            table=spec['table'],
            records=spec['records'])

        self.assertLastRequestMatchesSpec(spec)
        self.assertEqual(len(responses.calls), 1)

    @responses.activate
    def test_update(self):
        """ Update a list of records by hash.
        """
        # define the expected JSON body in POST request
        spec = {
            'operation': 'update',
            'schema': 'test_schema',
            'table': 'test_table',
            'records': [
                {
                    'id': 1,
                    'name': 'foo',
                },
            ],
        }
        # mock the server response
        responses.add(
            'POST',
            self.URL,
            json={
                'message': 'updated 1 of 1 records',
                'skipped_hashes': [],
                'updated_hashes': [
                    1,
                ],
            },
            status=200)

        self.db.update(
            schema=spec['schema'],
            table=spec['table'],
            records=spec['records'])

        self.assertLastRequestMatchesSpec(spec)
        self.assertEqual(len(responses.calls), 1)

    @responses.activate
    def test_delete(self):
        """ Delete a list of records by hash.
        """
        # define the expected JSON body in POST request
        spec = {
            'operation': 'delete',
            'schema': 'test_schema',
            'table': 'test_table',
            'hash_values': ['1'],
        }
        # mock the server response
        responses.add(
            'POST',
            self.URL,
            json={
                'message': 'deleted 1 of 1 records',
                'skipped_hashes': [],
                'deleted_hashes': [
                    1,
                ],
            },
            status=200)

        self.db.delete(
            schema=spec['schema'],
            table=spec['table'],
            hash_values=spec['hash_values'])

        self.assertLastRequestMatchesSpec(spec)
        self.assertEqual(len(responses.calls), 1)

    @responses.activate
    def test_search_by_hash(self):
        """ Search records by hash value.
        """
        # define the expected JSON body in POST request
        spec = {
            'operation': 'search_by_hash',
            'schema': 'test_schema',
            'table': 'test_table',
            'hash_values': ['1'],
            'get_attributes': ['*'],
        }
        # mock the server response
        responses.add(
            'POST',
            self.URL,
            json=[
                {
                    'id': 1,
                    'name': 'bar',
                },
            ],
            status=200)

        self.db.search_by_hash(
            schema=spec['schema'],
            table=spec['table'],
            hash_values=spec['hash_values'])

        self.assertLastRequestMatchesSpec(spec)
        self.assertEqual(len(responses.calls), 1)

        # test optional get_attributes
        spec['get_attributes'] = ['foo', 'bar']
        self.db.search_by_hash(
            schema=spec['schema'],
            table=spec['table'],
            hash_values=spec['hash_values'],
            get_attributes=spec['get_attributes'])

        self.assertLastRequestMatchesSpec(spec)
        self.assertEqual(len(responses.calls), 2)

    @responses.activate
    def test_search_by_value(self):
        """ Search records by value.
        """
        # define the expected JSON body in POST request
        spec = {
            'operation': 'search_by_value',
            'schema': 'test_schema',
            'table': 'test_table',
            'search_attribute': 'name',
            'search_value': 'foo',
            'get_attributes': ['*'],
        }
        # mock the server response
        responses.add(
            'POST',
            self.URL,
            json=[
                {
                    'id': 1,
                    'name': 'foo',
                },
            ],
            status=200)

        self.db.search_by_value(
            schema=spec['schema'],
            table=spec['table'],
            search_attribute=spec['search_attribute'],
            search_value=spec['search_value'])

        self.assertLastRequestMatchesSpec(spec)
        self.assertEqual(len(responses.calls), 1)

        # test optional get_attributes
        spec['get_attributes'] = ['foo', 'bar']
        self.db.search_by_value(
            schema=spec['schema'],
            table=spec['table'],
            search_attribute=spec['search_attribute'],
            search_value=spec['search_value'],
            get_attributes=spec['get_attributes'])

        self.assertLastRequestMatchesSpec(spec)
        self.assertEqual(len(responses.calls), 2)

    @responses.activate
    def test_sql(self):
        """ Accept SQL strings.
        """
        sql_string = \
            'INSERT INTO test_schema.test_table (id, name) VALUES(1, \"foo\")'
        # define the expected JSON body in POST request
        spec = {
            'operation': 'sql',
            'sql': sql_string,
        }
        # mock the server response
        responses.add(
            'POST',
            self.URL,
            json={
                'message': 'inserted 1 of 1 records',
                'skipped_hashes': [],
                'inserted_hashes': [
                    1,
                ],
            },
            status=200)

        self.db.sql(sql_string)
        self.assertLastRequestMatchesSpec(spec)
        self.assertEqual(len(responses.calls), 1)

    @responses.activate
    def test_csv_data_load(self):
        """ Records are inserted from a CSV file path.
        """
        # define the expected JSON body in POST request
        with open('tests/test.csv') as csv_file:
            csv_string = csv_file.read()
        spec = {
            'operation': 'csv_data_load',
            'action': 'insert',
            'schema': 'test_schema',
            'table': 'test_table',
            'data': csv_string,
        }
        # mock the server response
        responses.add(
            'POST',
            self.URL,
            json={
                'message': 'Starting job with id aUniqueID'
            },
            status=200)

        self.db.csv_data_load(
            schema=spec['schema'],
            table=spec['table'],
            path='tests/test.csv')
        self.assertLastRequestMatchesSpec(spec)
        self.assertEqual(len(responses.calls), 1)

    @responses.activate
    def test_csv_data_load_update(self):
        """ Records are updated from a CSV file path.
        """
        # define the expected JSON body in POST request
        with open('tests/test.csv') as csv_file:
            csv_string = csv_file.read()
        spec = {
            'operation': 'csv_data_load',
            'action': 'update',
            'schema': 'test_schema',
            'table': 'test_table',
            'data': csv_string,
        }
        # mock the server response
        responses.add(
            'POST',
            self.URL,
            json={
                'message': 'Starting job with id aUniqueID'
            },
            status=200)

        self.db.csv_data_load(
            schema=spec['schema'],
            table=spec['table'],
            path='tests/test.csv',
            action='update')
        self.assertLastRequestMatchesSpec(spec)
        self.assertEqual(len(responses.calls), 1)

    @responses.activate
    def test_csv_file_load(self):
        """ Records are inserted from a CSV file path on the HarperDB host.
        """
        spec = {
            'operation': 'csv_file_load',
            'action': 'insert',
            'schema': 'test_schema',
            'table': 'test_table',
            'file_path': 'path/to/file/on/host.csv',
        }
        # mock the server response
        responses.add(
            'POST',
            self.URL,
            json={
                'message': 'Starting job with id aUniqueID'
            },
            status=200)

        self.db.csv_file_load(
            schema=spec['schema'],
            table=spec['table'],
            file_path='path/to/file/on/host.csv')
        self.assertLastRequestMatchesSpec(spec)
        self.assertEqual(len(responses.calls), 1)

    @responses.activate
    def test_csv_file_load_update(self):
        """ Records are updated from a CSV file path on the HarperDB host.
        """
        spec = {
            'operation': 'csv_file_load',
            'action': 'update',
            'schema': 'test_schema',
            'table': 'test_table',
            'file_path': 'path/to/file/on/host.csv',
        }
        # mock the server response
        responses.add(
            'POST',
            self.URL,
            json={
                'message': 'Starting job with id aUniqueID'
            },
            status=200)

        self.db.csv_file_load(
            schema=spec['schema'],
            table=spec['table'],
            file_path='path/to/file/on/host.csv',
            action='update')
        self.assertLastRequestMatchesSpec(spec)
        self.assertEqual(len(responses.calls), 1)

    @responses.activate
    def test_csv_url_load(self):
        """ Records are inserted from a CSV file at a URL.
        """
        spec = {
            'operation': 'csv_url_load',
            'action': 'insert',
            'schema': 'test_schema',
            'table': 'test_table',
            'csv_url': 'example.com/test.csv',
        }
        # mock the server response
        responses.add(
            'POST',
            self.URL,
            json={
                'message': 'Starting job with id aUniqueID'
            },
            status=200)

        self.db.csv_url_load(
            schema=spec['schema'],
            table=spec['table'],
            csv_url='example.com/test.csv')
        self.assertLastRequestMatchesSpec(spec)
        self.assertEqual(len(responses.calls), 1)

    @responses.activate
    def test_csv_url_load_update(self):
        """ Records are updated from a CSV file at a URL.
        """
        spec = {
            'operation': 'csv_url_load',
            'action': 'update',
            'schema': 'test_schema',
            'table': 'test_table',
            'csv_url': 'example.com/test.csv',
        }
        # mock the server response
        responses.add(
            'POST',
            self.URL,
            json={
                'message': 'Starting job with id aUniqueID'
            },
            status=200)

        self.db.csv_url_load(
            schema=spec['schema'],
            table=spec['table'],
            csv_url='example.com/test.csv',
            action='update')
        self.assertLastRequestMatchesSpec(spec)
        self.assertEqual(len(responses.calls), 1)

    @responses.activate
    def test_get_job(self):
        """ Returns a job dictionary from an id.
        """
        spec = {
            'operation': 'get_job',
            'id': 'aUniqueID',
        }
        # mock the server response
        responses.add(
            'POST',
            self.URL,
            json=[
                {
                    '__createdtime__': 1234567890000,
                    '__updatedtime__': 1234567890002,
                    'created_datetime': 1234567890004,
                    'end_datetime': 1234567890008,
                    'id': 'aUniqueID',
                    'job_body': None,
                    'message': 'successfully loaded 2 of 2 records',
                    'start_datetime': 1234567890006,
                    'status': 'COMPLETE',
                    'type': 'csv_data_load',
                    'user': None,
                    'start_datetime_converted': 'ISO 8601',
                    'end_datetime_converted': 'ISO 8601'
                }
            ],
            status=200)

        self.db.get_job(spec['id'])
        self.assertLastRequestMatchesSpec(spec)
        self.assertEqual(len(responses.calls), 1)


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
