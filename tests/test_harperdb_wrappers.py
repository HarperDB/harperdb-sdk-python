import datetime
import json
import responses
import requests
import unittest

import harperdb
import harperdb_testcase


class TestHarperDBWrapper(harperdb_testcase.HarperDBTestCase):

    def setUp(self):
        """ This method is called before each test.
        """
        self.db = harperdb.wrappers.HarperDBWrapper(self.URL)

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

    def test_harperdb_is_subscriptable_by_schema_name(self):
        """ HarperDB is subscriptable by schema name.
        """
        self.assertIsInstance(
            self.db['test_schema_1'],
            harperdb.HarperDBSchema)
        self.assertIsInstance(
            self.db['test_schema_2'],
            harperdb.HarperDBSchema)

    @responses.activate
    def test_harperdb_is_iterable_and_has_length(self):
        """ HarperDB yields HarperDBSchema instances, and has length.
        """
        # mock server response to describe_all request
        responses.add(
            'POST',
            self.URL,
            json=self.DESCRIBE_ALL,
            status=200)
        responses.add(
            'POST',
            self.URL,
            json=self.DESCRIBE_ALL,
            status=200)
        responses.add(
            'POST',
            self.URL,
            json=self.DESCRIBE_ALL_UPDATED,
            status=200)

        for schema in self.db:
            self.assertIsInstance(schema, harperdb.HarperDBSchema)
        self.assertEqual(len(self.db), len(self.DESCRIBE_ALL))
        # length is a property read from the server
        self.assertEqual(len(self.db), len(self.DESCRIBE_ALL_UPDATED))
        self.assertEqual(len(responses.calls), 3)

    @responses.activate
    def test_create_schema(self):
        """ create_schema() returns an instance of HarperDBSchema.
        """
        # mock server response to create_schema request
        responses.add(
            'POST',
            self.URL,
            json=self.SCHEMA_CREATED,
            status=200)
        # mock server error to create_schema request
        responses.add(
            'POST',
            self.URL,
            json=self.SCHEMA_EXISTS,
            status=500)

        self.assertIsInstance(
            self.db.create_schema('test_schema_1'),
            harperdb.HarperDBSchema)
        with self.assertRaises(harperdb.exceptions.HarperDBError):
            self.db.create_schema('test_schema_1')
        self.assertEqual(len(responses.calls), 2)

    @responses.activate
    def test_drop_schema(self):
        """ drop_schema() returns None.
        """
        # mock server response to drop_schema request
        responses.add(
            'POST',
            self.URL,
            json=self.SCHEMA_DROPPED,
            status=200)
        # mock server error to drop_schema request
        responses.add(
            'POST',
            self.URL,
            json=self.SCHEMA_NOT_EXISTS,
            status=500)

        self.assertIsNone(self.db.drop_schema('test_schema_1'))
        with self.assertRaises(harperdb.exceptions.HarperDBError):
            self.db.drop_schema('test_schema_1')
        self.assertEqual(len(responses.calls), 2)

    @responses.activate
    def test_del_schema(self):
        """ Schemas can be dropped using the del keyword.
        """
        # mock server response to drop_schema request
        responses.add(
            'POST',
            self.URL,
            json=self.SCHEMA_DROPPED,
            status=200)

        del self.db['test_schema_1']
        self.assertEqual(len(responses.calls), 1)


class TestHarperDBWrapperLowLevelMethods(harperdb_testcase.HarperDBTestCase):

    def setUp(self):
        """ This method is called before each test.
        """
        self.db = harperdb.wrappers.HarperDBWrapper(self.URL)

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

        self.db._create_schema(spec['schema'])

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

        self.db._drop_schema(spec['schema'])

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

        self.db._describe_schema(spec['schema'])

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

        self.db._create_table(
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

        self.db._describe_table(spec['schema'], spec['table'])
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

        self.db._describe_all()

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

        self.db._drop_table(schema=spec['schema'], table=spec['table'])

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

        self.db._drop_attribute(
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

        self.db._insert(
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

        self.db._update(
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

        self.db._delete(
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

        self.db._search_by_hash(
            schema=spec['schema'],
            table=spec['table'],
            hash_values=spec['hash_values'])

        self.assertLastRequestMatchesSpec(spec)
        self.assertEqual(len(responses.calls), 1)

        # test optional get_attributes
        spec['get_attributes'] = ['foo', 'bar']
        self.db._search_by_hash(
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

        self.db._search_by_value(
            schema=spec['schema'],
            table=spec['table'],
            search_attribute=spec['search_attribute'],
            search_value=spec['search_value'])

        self.assertLastRequestMatchesSpec(spec)
        self.assertEqual(len(responses.calls), 1)

        # test optional get_attributes
        spec['get_attributes'] = ['foo', 'bar']
        self.db._search_by_value(
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

        self.db._sql(sql_string)
        self.assertLastRequestMatchesSpec(spec)
        self.assertEqual(len(responses.calls), 1)


class TestHarperDBRecord(harperdb_testcase.HarperDBTestCase):

    def setUp(self):
        """ This method is called before each test.
        """
        self.db = harperdb.wrappers.HarperDBWrapper(self.URL)
        self.schema = harperdb.wrappers.HarperDBSchema(
            name='test_schema_1',
            database=self.db)
        self.table = harperdb.wrappers.HarperDBTable(
            name=self.DESCRIBE_TABLE['name'],
            schema=self.schema)
        self.record = harperdb.wrappers.HarperDBRecord(
            table=self.table,
            hash_value=self.RECORDS[0]['id'])

    def test_create_harperdbrecord_with_kwargs(self):
        """ Create an instance of HarperDBTable with keyowrd args.
        """
        # record object created in self.setUp()
        self.assertEqual(self.record.table, self.table)
        self.assertEqual(self.record._hash_value, self.RECORDS[0]['id'])

    @responses.activate
    def test_harperdbrecord_is_subscriptable(self):
        """ HarperDBRecord is subscriptable.
        """
        # mock the server responses to search_by_hash requests
        responses.add(
            'POST',
            self.URL,
            json=self.RECORDS,
            status=200)
        responses.add(
            'POST',
            self.URL,
            json=self.RECORDS,
            status=200)
        # this record doesnt exist in the database
        responses.add(
            'POST',
            self.URL,
            json=self.NO_RECORDS,
            status=200)

        self.assertEqual(self.record['pi'], self.RECORDS[0]['pi'])
        with self.assertRaises(KeyError):
            self.record['invalid_key']
        self.assertEqual(len(responses.calls), 2)
        with self.assertRaises(harperdb.exceptions.HarperDBError):
            self.record['pi']
        self.assertEqual(len(responses.calls), 3)

    @responses.activate
    def test_time_properties(self):
        """ HarperDBRecord time metadata is read from the server.
        """
        # mock server responses to search_by_hash requests
        responses.add(
            'POST',
            self.URL,
            json=self.RECORDS,
            status=200)
        responses.add(
            'POST',
            self.URL,
            json=self.RECORDS,
            status=200)
        responses.add(
            'POST',
            self.URL,
            json=self.RECORDS_UPDATED,
            status=200)

        self.assertEqual(
            self.record.__createdtime__,
            self.RECORDS[0]['__createdtime__'])
        self.assertEqual(
            self.record.__updatedtime__,
            self.RECORDS[0]['__updatedtime__'])
        self.assertEqual(
            self.record.__updatedtime__,
            self.RECORDS_UPDATED[0]['__updatedtime__'])
        self.assertEqual(len(responses.calls), 3)

    @responses.activate
    def test_delete(self):
        """ A record can be deleted using HarperDBrecord.delete()
        """
        # mock server response to delete request
        responses.add(
            'POST',
            self.URL,
            json=self.RECORDS_DELETED,
            status=200)

        self.assertIsNone(self.record.delete())
        self.assertEqual(len(responses.calls), 1)

    @responses.activate
    def test_item_assignment_updates_record_values(self):
        """ Item assignment updates record values.
        """
        # mock server response to update
        responses.add(
            'POST',
            self.URL,
            json=self.RECORD_UPSERTED,
            status=200)
        # mock server repsonse to search_by_hash in __getitem__
        responses.add(
            'POST',
            self.URL,
            json=self.RECORDS_UPDATED,
            status=200)

        self.record['foo'] = 'bar'
        self.assertEqual(self.record['foo'], 'bar')
        self.assertEqual(len(responses.calls), 2)

    @responses.activate
    def test_datetime_helpers(self):
        """ Helpers are implemented which return datetime objects.
        """
        # mock server responses to search_by_hash requests
        responses.add(
            'POST',
            self.URL,
            json=self.RECORDS,
            status=200)
        responses.add(
            'POST',
            self.URL,
            json=self.RECORDS,
            status=200)

        created_time = self.record.created_time
        updated_time = self.record.updated_time

        self.assertEqual(len(responses.calls), 2)
        self.assertIsInstance(created_time, datetime.datetime)
        self.assertIsInstance(updated_time, datetime.datetime)
        self.assertEqual(
            created_time.timestamp(),
            self.RECORDS[0]['__createdtime__'] / 1000)
        self.assertEqual(
            updated_time.timestamp(),
            self.RECORDS[0]['__updatedtime__'] / 1000)

    @responses.activate
    def test_to_dict_method(self):
        """ Record data can be dumped to a dictionary.
        """
        # mock server responses to search_by_hash requests
        responses.add(
            'POST',
            self.URL,
            json=self.RECORDS,
            status=200)

        record_dict = self.record.to_dict()
        self.assertDictEqual(
            record_dict,
            {
                'id': 'uniqueHash',
                'pi': 3.14159,
            })


class TestHarperDBSchema(harperdb_testcase.HarperDBTestCase):

    def setUp(self):
        """ This method is called before each test.
        """
        self.db = harperdb.wrappers.HarperDBWrapper(self.URL)
        self.schema = harperdb.wrappers.HarperDBSchema(
            name='test_schema_1',
            database=self.db)

    def test_create_harperdbschema_with_kwargs(self):
        """ Create an instance of HarperDBSchema with keyword args.
        """
        self.assertEqual(self.schema.name, 'test_schema_1')
        self.assertEqual(self.schema.database, self.db)

    def test_harperdbschema_is_subscriptable_by_table_name(self):
        """ HarperDBSchema is subscriptable by table name.
        """
        self.assertIsInstance(
            self.schema['test_table_1'],
            harperdb.HarperDBTable)

    @responses.activate
    def test_harperdbschema_is_iterable_and_has_length(self):
        """ HarperDBSchema yields instances of HarperDBTable, and has length.
        """
        # mock server responses to describe_table requests
        responses.add(
            'POST',
            self.URL,
            json=self.DESCRIBE_SCHEMA_1,
            status=200)
        responses.add(
            'POST',
            self.URL,
            json=self.DESCRIBE_SCHEMA_1,
            status=200)
        responses.add(
            'POST',
            self.URL,
            json=self.DESCRIBE_SCHEMA_2,
            status=200)
        responses.add(
            'POST',
            self.URL,
            json=self.DESCRIBE_SCHEMA_2_UPDATED,
            status=200)

        schema_1 = harperdb.HarperDBSchema(
            name='test_schema_1',
            database=self.db)
        schema_2 = harperdb.HarperDBSchema(
            name='test_schema_2',
            database=self.db)

        self.assertEqual(len(schema_1), 1)
        for table in schema_1:
            self.assertIsInstance(table, harperdb.HarperDBTable)
        self.assertEqual(len(schema_2), len(self.DESCRIBE_SCHEMA_2))
        # length is a property read from the server
        self.assertEqual(
            len(schema_2),
            len(self.DESCRIBE_SCHEMA_2_UPDATED))
        self.assertEqual(len(responses.calls), 4)

    @responses.activate
    def test_harperdbschema_is_iterable_and_has_length_legacy(self):
        """ HarperDBSchema yields instances of HarperDBTable, and has length.
        """
        # mock server responses to describe_table requests
        responses.add(
            'POST',
            self.URL,
            json=self.DESCRIBE_SCHEMA_1__LEGACY,
            status=200)
        responses.add(
            'POST',
            self.URL,
            json=self.DESCRIBE_SCHEMA_1__LEGACY,
            status=200)
        responses.add(
            'POST',
            self.URL,
            json=self.DESCRIBE_SCHEMA_2,
            status=200)
        responses.add(
            'POST',
            self.URL,
            json=self.DESCRIBE_SCHEMA_2_UPDATED__LEGACY,
            status=200)

        schema_1 = harperdb.HarperDBSchema(
            name='test_schema_1',
            database=self.db)
        schema_2 = harperdb.HarperDBSchema(
            name='test_schema_2',
            database=self.db)

        self.assertEqual(len(schema_1), 1)
        for table in schema_1:
            self.assertIsInstance(table, harperdb.HarperDBTable)
        self.assertEqual(len(schema_2), len(self.DESCRIBE_SCHEMA_2))
        # length is a property read from the server
        self.assertEqual(
            len(schema_2),
            len(self.DESCRIBE_SCHEMA_2_UPDATED__LEGACY))
        self.assertEqual(len(responses.calls), 4)

    @responses.activate
    def test_create_table(self):
        """ create_table() returns an instance of HarperTable.
        """
        # mock server response to create_schema request
        responses.add(
            'POST',
            self.URL,
            json=self.TABLE_CREATED,
            status=200)
        # mock server error to create_schema request
        responses.add(
            'POST',
            self.URL,
            json=self.TABLE_EXISTS,
            status=500)

        self.assertIsInstance(
            self.schema.create_table(name='test_table_1', hash_attribute='id'),
            harperdb.HarperDBTable)
        with self.assertRaises(harperdb.HarperDBError):
            self.schema.create_table(name='test_table_1', hash_attribute='id')
        self.assertEqual(len(responses.calls), 2)

    @responses.activate
    def test_drop_table(self):
        """ drop_table() returns None.
        """
        # mock server response to drop_schema request
        responses.add(
            'POST',
            self.URL,
            json=self.TABLE_DROPPED,
            status=200)
        # mock server error to drop_schema request
        responses.add(
            'POST',
            self.URL,
            json=self.TABLE_NOT_EXISTS,
            status=500)

        self.assertIsNone(self.schema.drop_table('test_table_1'))
        with self.assertRaises(harperdb.HarperDBError):
            self.schema.drop_table('test_table_1')
        self.assertEqual(len(responses.calls), 2)

    @responses.activate
    def test_del_table(self):
        """ Tables can be dropped using the del keyword.
        """
        # mock server response to drop_table request
        responses.add(
            'POST',
            self.URL,
            json=self.SCHEMA_DROPPED,
            status=200)

        del self.schema['test_schema_1']
        self.assertEqual(len(responses.calls), 1)

    @responses.activate
    def test_drop(self):
        """ A schema can be dropped using HarperDBSchema.drop()
        """
        # mock server response to drop_schema request
        responses.add(
            'POST',
            self.URL,
            json=self.SCHEMA_DROPPED,
            status=200)

        self.assertIsNone(self.schema.drop())
        self.assertEqual(len(responses.calls), 1)


class TestHarperDBTable(harperdb_testcase.HarperDBTestCase):

    def setUp(self):
        """ This method is called before each test.
        """
        self.db = harperdb.wrappers.HarperDBWrapper(self.URL)
        self.schema = harperdb.wrappers.HarperDBSchema(
            name='test_schema_1',
            database=self.db)
        self.table = harperdb.wrappers.HarperDBTable(
            name=self.DESCRIBE_TABLE['name'],
            schema=self.schema,
            hash_attribute='id')

    @responses.activate
    def test_create_harperdbtable_with_kwargs(self):
        """ Create an instance of HarperDBTable with keyowrd args.
        """
        # mock server responses to describe_table requests
        responses.add(
            'POST',
            self.URL,
            json=self.DESCRIBE_TABLE,
            status=200)
        responses.add(
            'POST',
            self.URL,
            json=self.DESCRIBE_TABLE,
            status=200)
        responses.add(
            'POST',
            self.URL,
            json=self.DESCRIBE_TABLE,
            status=200)

        self.assertEqual(
            self.table.hash_attribute,
            self.DESCRIBE_TABLE['hash_attribute'])
        self.assertEqual(len(responses.calls), 0)
        self.assertEqual(self.table.id, self.DESCRIBE_TABLE['id'])
        self.assertEqual(len(responses.calls), 1)
        self.assertEqual(self.table.name, self.DESCRIBE_TABLE['name'])
        self.assertEqual(self.table.schema, self.schema)
        self.assertEqual(len(responses.calls), 1)
        self.assertEqual(self.table.attributes, ['id'])
        self.assertEqual(len(responses.calls), 2)
        self.assertEqual(
            self.table.record_count,
            self.DESCRIBE_TABLE['record_count'])
        self.assertEqual(len(responses.calls), 3)

    def test_harperdbtable_is_subscriptable_by_hash_attribute(self):
        """ HarperDBTable is subscriptable by hash.
        """
        self.assertIsInstance(
            self.table['uniqueHash'],
            harperdb.HarperDBRecord)

    @responses.activate
    def test_harperdbtable_has_length(self):
        """ The Length of HarperDBTable equals the number of records.
        """
        # mock server responses to describe_table requests
        responses.add(
            'POST',
            self.URL,
            json=self.DESCRIBE_TABLE,
            status=200)
        responses.add(
            'POST',
            self.URL,
            json=self.DESCRIBE_UPDATED_TABLE,
            status=200)

        self.assertEqual(len(self.table), self.DESCRIBE_TABLE['record_count'])
        # length is a property read from the server
        self.assertEqual(
            len(self.table),
            self.DESCRIBE_UPDATED_TABLE['record_count'])
        self.assertEqual(len(responses.calls), 2)

    @responses.activate
    def test_time_properties(self):
        """ HarperDBTable time metadata is read from the server.
        """
        # mock server responses to describe_table requests
        responses.add(
            'POST',
            self.URL,
            json=self.DESCRIBE_TABLE,
            status=200)
        responses.add(
            'POST',
            self.URL,
            json=self.DESCRIBE_TABLE,
            status=200)
        responses.add(
            'POST',
            self.URL,
            json=self.DESCRIBE_UPDATED_TABLE,
            status=200)

        self.assertEqual(
            self.table.__createdtime__,
            self.DESCRIBE_TABLE['__createdtime__'])
        self.assertEqual(
            self.table.__updatedtime__,
            self.DESCRIBE_TABLE['__updatedtime__'])
        self.assertEqual(
            self.table.__updatedtime__,
            self.DESCRIBE_UPDATED_TABLE['__updatedtime__'])
        self.assertEqual(len(responses.calls), 3)

    @responses.activate
    def test_record_count_property(self):
        """ HarperDBTable record_count metadata is read from the server.
        """
        # mock server responses to describe_table requests
        responses.add(
            'POST',
            self.URL,
            json=self.DESCRIBE_TABLE,
            status=200)
        responses.add(
            'POST',
            self.URL,
            json=self.DESCRIBE_UPDATED_TABLE,
            status=200)

        self.assertEqual(
            self.table.record_count,
            self.DESCRIBE_TABLE['record_count'])
        self.assertEqual(
            self.table.record_count,
            self.DESCRIBE_UPDATED_TABLE['record_count'])
        self.assertEqual(len(responses.calls), 2)

    @responses.activate
    def test_attributes_property(self):
        """ HarperDBTable attributes metadata is read from the server.
        """
        # mock server responses to describe_table requests
        responses.add(
            'POST',
            self.URL,
            json=self.DESCRIBE_TABLE,
            status=200)
        responses.add(
            'POST',
            self.URL,
            json=self.DESCRIBE_UPDATED_TABLE,
            status=200)

        self.assertEqual(self.table.attributes, ['id'])
        self.assertEqual(self.table.attributes, ['id', 'name'])
        self.assertEqual(len(responses.calls), 2)

    @responses.activate
    def test_delete(self):
        """ Records can be deleted by hash value.
        """
        # mock server response to drop_record request
        responses.add(
            'POST',
            self.URL,
            json=self.RECORDS_DELETED,
            status=200)
        responses.add(
            'POST',
            self.URL,
            json=self.RECORDS_NOT_DELETED,
            status=200)

        self.assertIsNone(self.table.delete(1))
        with self.assertRaises(harperdb.HarperDBError):
            self.table.delete('invalid_hash')
        self.assertEqual(len(responses.calls), 2)

    @responses.activate
    def test_del_record(self):
        """ Records can be deleted using the del keyword and hash value.
        """
        # mock server response to drop_record request
        responses.add(
            'POST',
            self.URL,
            json=self.RECORDS_DELETED,
            status=200)
        responses.add(
            'POST',
            self.URL,
            json=self.RECORDS_NOT_DELETED,
            status=200)

        del self.table[1]
        with self.assertRaises(harperdb.HarperDBError):
            del self.table['invalid_hash']
        self.assertEqual(len(responses.calls), 2)

    @responses.activate
    def test_drop(self):
        """ A table can be dropped using HarperDBSchema.drop()
        """
        # mock server response to drop_table request
        responses.add(
            'POST',
            self.URL,
            json=self.RECORDS_DELETED,
            status=200)

        self.assertIsNone(self.table.drop())
        self.assertEqual(len(responses.calls), 1)

    @responses.activate
    def test_upsert(self):
        """ HarperDBTable.upsert accepts a list, or a single record.
        """
        # mock server response to insert requests
        responses.add(
            'POST',
            self.URL,
            json=self.RECORD_INSERTED,
            status=200)
        responses.add(
            'POST',
            self.URL,
            json=self.RECORDS_INSERTED,
            status=200)
        # mock server response to update request
        responses.add(
            'POST',
            self.URL,
            json=self.RECORD_NOT_INSERTED,
            status=200)
        responses.add(
            'POST',
            self.URL,
            json=self.RECORD_UPSERTED,
            status=200)

        # insert records without hash_attribute
        record = self.table.upsert({'name': 'foo'})
        self.assertLastRequestMatchesSpec({
            'operation': 'insert',
            'schema': self.schema.name,
            'table': self.table.name,
            'records': [
                {'name': 'foo'},
            ],
        })
        self.assertEqual(len(responses.calls), 1)
        records = self.table.upsert([
            {'name': 'bar'},
            {'name': 'baz'},
        ])
        self.assertLastRequestMatchesSpec({
            'operation': 'insert',
            'schema': self.schema.name,
            'table': self.table.name,
            'records': [
                {'name': 'bar'},
                {'name': 'baz'},
            ],
        })
        self.assertEqual(len(responses.calls), 2)
        # update a record using its hash value
        updated_record = self.table.upsert({
            self.table.hash_attribute: record._hash_value,
            'name': 'FOO',
        })
        self.assertLastRequestMatchesSpec({
            'operation': 'update',
            'schema': self.schema.name,
            'table': self.table.name,
            'records': [
                {
                    'id': 'assignedID_1',
                    'name': 'FOO',
                },
            ],
        })

        self.assertEqual(len(records), 2)
        self.assertEqual(len(responses.calls), 4)

    @responses.activate
    def test_search_by_value(self):
        """ HarperDBTable.search_by_value returns a list of HarperDBRecords
        """
        # mock server responses to search_by_value requests
        responses.add(
            'POST',
            self.URL,
            json=self.RECORDS,
            status=200)

        # search for records
        records = self.table.search_by_value(
            search_attribute='pi',
            search_value=3.14159)

        self.assertEqual(len(records), 1)
        self.assertEqual(len(responses.calls), 1)
        self.assertIsInstance(records[0], harperdb.HarperDBRecord)

    @responses.activate
    def test_datetime_helpers(self):
        """ Helpers are implemented which return datetime objects.
        """
        # mock server responses to describe_table requests
        responses.add(
            'POST',
            self.URL,
            json=self.DESCRIBE_TABLE,
            status=200)
        responses.add(
            'POST',
            self.URL,
            json=self.DESCRIBE_TABLE,
            status=200)

        created_time = self.table.created_time
        updated_time = self.table.updated_time

        self.assertEqual(len(responses.calls), 2)
        self.assertIsInstance(created_time, datetime.datetime)
        self.assertIsInstance(updated_time, datetime.datetime)
        self.assertEqual(
            created_time.timestamp(),
            self.DESCRIBE_TABLE['__createdtime__'] / 1000)
        self.assertEqual(
            updated_time.timestamp(),
            self.DESCRIBE_TABLE['__updatedtime__'] / 1000)

    @responses.activate
    def test_upsert_from_csv(self):
        """ Records can be upserted from a CSV file path.
        """
        # mock server response to insert request
        responses.add(
            'POST',
            self.URL,
            json=self.RECORDS_INSERTED,
            status=200)
        # mock server response to insert+update request
        responses.add(
            'POST',
            self.URL,
            json=self.RECORDS_NOT_INSERTED,
            status=200)
        responses.add(
            'POST',
            self.URL,
            json=self.RECORD_UPSERTED,
            status=200)

        records = self.table.upsert_from_csv('tests/test.csv')
        self.assertLastRequestMatchesSpec({
            'operation': 'insert',
            'schema': self.schema.name,
            'table': self.table.name,
            'records': [
                {
                    'id': '1',
                    'name': 'Duke',
                    'age': '5',
                    'color': 'Brown',
                },
                {
                    'id': '2',
                    'name': 'Dino',
                    'age': '3',
                    'color': 'Gray',
                },
            ],
        })
        self.assertEqual(len(records), 2)
        for record in records:
            self.assertIsInstance(record, harperdb.wrappers.HarperDBRecord)
        self.assertEqual(records[0]._hash_value, 'assignedID_2')
        self.assertEqual(records[1]._hash_value, 'assignedID_3')
        # all sample records have a valid hash value
        self.assertEqual(len(responses.calls), 1)

        # one record is inserted, one is updated
        records = self.table.upsert_from_csv('tests/test.csv')
        self.assertEqual(len(responses.calls), 3)
        self.assertLastRequestMatchesSpec({
            'operation': 'update',
            'schema': self.schema.name,
            'table': self.table.name,
            'records': [
                {
                    'id': '2',
                    'name': 'Dino',
                    'age': '3',
                    'color': 'Gray',
                },
            ],
        })
