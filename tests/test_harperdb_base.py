import responses
import unittest

import harperdb
import harperdb_testcase


class TestHarperDBBase(harperdb_testcase.HarperDBTestCase):

    def setUp(self):
        """ This method is called before each test.
        """
        self.db = harperdb.HarperDBBase(self.URL)

    def tearDown(self):
        """ This method is called after each test.
        """
        responses.reset()

    @unittest.mock.patch('base64.b64encode')
    def test_create_harperdb_base_with_kwargs(self, mock_b64encode):
        """ Create an instance of HarperDBBase with keyword args.
        """
        # by mocking the base64 module we can define what it returns,
        # so it's very easy to check the value stored in db.token
        mock_b64encode.return_value = b'anArbitraryBase64String'

        db = harperdb.HarperDBBase(
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
            json=self.SCHEMA_CREATED,
            status=200)

        self.assertEqual(
            self.db._create_schema(spec['schema']),
            self.SCHEMA_CREATED)
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
            json=self.SCHEMA_DROPPED,
            status=200)

        self.assertEqual(
            self.db._drop_schema(spec['schema']),
            self.SCHEMA_DROPPED)
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
            json=self.DESCRIBE_SCHEMA_1,
            status=200)

        self.assertEqual(
            self.db._describe_schema(spec['schema']),
            self.DESCRIBE_SCHEMA_1)
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
            json=self.TABLE_CREATED,
            status=200)

        self.assertEqual(
            self.db._create_table(
                schema=spec['schema'],
                table=spec['table'],
                hash_attribute=spec['hash_attribute']),
            self.TABLE_CREATED)
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
            json=self.DESCRIBE_TABLE,
            status=200)

        self.assertEqual(
            self.db._describe_table(spec['schema'], spec['table']),
            self.DESCRIBE_TABLE)
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
            json=self.DESCRIBE_ALL,
            status=200)

        self.assertEqual(self.db._describe_all(), self.DESCRIBE_ALL)
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
            json=self.TABLE_DROPPED,
            status=200)

        self.assertEqual(
            self.db._drop_table(schema=spec['schema'], table=spec['table']),
            self.TABLE_DROPPED)
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
            json=self.DROP_ATTRIBUTE,
            status=200)

        self.assertEqual(
            self.db._drop_attribute(
                schema=spec['schema'],
                table=spec['table'],
                attribute=spec['attribute']),
            self.DROP_ATTRIBUTE)
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
            json=self.RECORD_INSERTED,
            status=200)

        self.assertEqual(
            self.db._insert(
                schema=spec['schema'],
                table=spec['table'],
                records=spec['records']),
            self.RECORD_INSERTED)
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
            json=self.RECORD_UPSERTED,
            status=200)

        self.assertEqual(
            self.db._update(
                schema=spec['schema'],
                table=spec['table'],
                records=spec['records']),
            self.RECORD_UPSERTED)
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
            json=self.RECORDS_DELETED,
            status=200)

        self.assertEqual(
            self.db._delete(
                schema=spec['schema'],
                table=spec['table'],
                hash_values=spec['hash_values']),
            self.RECORDS_DELETED)
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
            json=self.RECORDS,
            status=200)

        self.assertEqual(
            self.db._search_by_hash(
                schema=spec['schema'],
                table=spec['table'],
                hash_values=spec['hash_values']),
            self.RECORDS)
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
            json=self.RECORDS,
            status=200)

        self.assertEqual(
            self.db._search_by_value(
                schema=spec['schema'],
                table=spec['table'],
                search_attribute=spec['search_attribute'],
                search_value=spec['search_value']),
            self.RECORDS)
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
            json=self.RECORD_INSERTED,
            status=200)

        self.assertEqual(self.db._sql(sql_string), self.RECORD_INSERTED)
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
            json=self.START_JOB,
            status=200)

        self.assertEqual(
            self.db._csv_data_load(
                schema=spec['schema'],
                table=spec['table'],
                path='tests/test.csv'),
            self.START_JOB)
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
            json=self.START_JOB,
            status=200)

        self.assertEqual(
            self.db._csv_data_load(
                schema=spec['schema'],
                table=spec['table'],
                path='tests/test.csv',
                action='update'),
            self.START_JOB)
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
            json=self.START_JOB,
            status=200)

        self.assertEqual(
            self.db._csv_file_load(
                schema=spec['schema'],
                table=spec['table'],
                file_path='path/to/file/on/host.csv'),
            self.START_JOB)
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
            json=self.START_JOB,
            status=200)

        self.assertEqual(
            self.db._csv_file_load(
                schema=spec['schema'],
                table=spec['table'],
                file_path='path/to/file/on/host.csv',
                action='update'),
            self.START_JOB)
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
            json=self.START_JOB,
            status=200)

        self.assertEqual(
            self.db._csv_url_load(
                schema=spec['schema'],
                table=spec['table'],
                csv_url='example.com/test.csv'),
            self.START_JOB)
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
            json=self.START_JOB,
            status=200)

        self.assertEqual(
            self.db._csv_url_load(
                schema=spec['schema'],
                table=spec['table'],
                csv_url='example.com/test.csv',
                action='update'),
            self.START_JOB)
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
            json=self.GET_JOB,
            status=200)

        self.assertEqual(
            self.db._get_job(spec['id']),
            self.GET_JOB)
        self.assertLastRequestMatchesSpec(spec)
        self.assertEqual(len(responses.calls), 1)

    @responses.activate
    def test_add_user(self):
        """ Add a user.
        """
        spec = {
            'operation': 'add_user',
            'role': 'aUniqueID',
            'username': 'user',
            'password': 'pass',
            'active': True,
        }
        # mock the server response
        responses.add(
            'POST',
            self.URL,
            json=self.USER_ADDED,
            status=200)

        self.assertEqual(
            self.db._add_user(
                role=spec['role'],
                username=spec['username'],
                password=spec['password']),
            self.USER_ADDED)
        self.assertLastRequestMatchesSpec(spec)
        self.assertEqual(len(responses.calls), 1)

    @responses.activate
    def test_alter_user(self):
        """ Alter a user.
        """
        spec = {
            'operation': 'alter_user',
            'role': 'aUniqueID',
            'username': 'user',
            'password': 'pass',
            'active': False,
        }
        # mock the server response
        responses.add(
            'POST',
            self.URL,
            json=self.USER_ALTERED,
            status=200)

        self.assertEqual(
            self.db._alter_user(
                role=spec['role'],
                username=spec['username'],
                password=spec['password'],
                active=False),
            self.USER_ALTERED)
        self.assertLastRequestMatchesSpec(spec)
        self.assertEqual(len(responses.calls), 1)

    @responses.activate
    def test_drop_user(self):
        """ Drop a user.
        """
        spec = {
            'operation': 'drop_user',
            'username': 'user',
        }
        # mock the server response
        responses.add(
            'POST',
            self.URL,
            json=self.USER_DROPPED,
            status=200)

        self.assertEqual(
            self.db._drop_user(username=spec['username']),
            self.USER_DROPPED)
        self.assertLastRequestMatchesSpec(spec)
        self.assertEqual(len(responses.calls), 1)

    @responses.activate
    def test_user_info(self):
        """ Get user info.
        """
        spec = {
            'operation': 'user_info',
            'username': 'user',
        }
        # mock the server response
        responses.add(
            'POST',
            self.URL,
            json=self.USER_INFO,
            status=200)

        self.assertEqual(
            self.db._user_info(username=spec['username']),
            self.USER_INFO)
        self.assertLastRequestMatchesSpec(spec)
        self.assertEqual(len(responses.calls), 1)

    @responses.activate
    def test_list_users(self):
        """ List users.
        """
        spec = {
            'operation': 'list_users',
        }
        # mock the server response
        responses.add(
            'POST',
            self.URL,
            json=self.LIST_USERS,
            status=200)

        self.assertEqual(
            self.db._list_users(),
            self.LIST_USERS)
        self.assertLastRequestMatchesSpec(spec)
        self.assertEqual(len(responses.calls), 1)

    @responses.activate
    def test_add_role(self):
        """ Add a role.
        """
        spec = {
            'operation': 'add_role',
            'role': 'developer',
            'permission': {
                'super_user': False,
                'dev':{
                    'tables': {
                            'dog': {
                            'read': True,
                            'insert': True,
                            'update': True,
                            'delete': False,
                            'attribute_restrictions':[]
                        }
                    }
                }
            }
        }
        # mock the server response
        responses.add(
            'POST',
            self.URL,
            json=self.ADD_ROLE,
            status=200)

        self.assertEqual(
            self.db._add_role(role='developer', permission=spec['permission']),
            self.ADD_ROLE)
        self.assertLastRequestMatchesSpec(spec)
        self.assertEqual(len(responses.calls), 1)

    @responses.activate
    def test_alter_role(self):
        """ Add a role.
        """
        spec = {
            'operation': 'alter_role',
            'id': 'aUniqueID',
            'permission': {
                'super_user': False,
                'dev':{
                    'tables': {
                            'dog': {
                            'read': True,
                            'insert': True,
                            'update': True,
                            'delete': False,
                            'attribute_restrictions':[]
                        }
                    }
                }
            }
        }
        # mock the server response
        responses.add(
            'POST',
            self.URL,
            json=self.ALTER_ROLE,
            status=200)

        self.assertEqual(
            self.db._alter_role(id='aUniqueID', permission=spec['permission']),
            self.ALTER_ROLE)
        self.assertLastRequestMatchesSpec(spec)
        self.assertEqual(len(responses.calls), 1)

    @responses.activate
    def test_drop_role(self):
        """ Drop a role.
        """
        spec = {
            'operation': 'drop_role',
            'id': 'aUniqueID',
        }
        # mock the server response
        responses.add(
            'POST',
            self.URL,
            json=self.DROP_ROLE,
            status=200)

        self.assertEqual(
            self.db._drop_role(id='aUniqueID'),
            self.DROP_ROLE)
        self.assertLastRequestMatchesSpec(spec)
        self.assertEqual(len(responses.calls), 1)

    @responses.activate
    def test_list_roles(self):
        """List Roles.
        """
        spec = {
            'operation': 'list_roles',
        }
        # mock the server response
        responses.add(
            'POST',
            self.URL,
            json=self.LIST_ROLES,
            status=200)

        self.assertEqual(
            self.db._list_roles(),
            self.LIST_ROLES)
        self.assertLastRequestMatchesSpec(spec)
        self.assertEqual(len(responses.calls), 1)

    @responses.activate
    def test_add_node(self):
        """ Add a node to a cluster.
        """
        spec = {
            'operation': 'add_node',
            'name': 'anotherNode',
            'host': 'hostname',
            'port': 31415,
            'subscriptions': [
                {
                    'channel': 'dev:dog',
                    'subscribe': False,
                    'publish': True,
                },
            ]
        }
        responses.add(
            'POST',
            self.URL,
            json=self.NODE_ADDED,
            status=200)

        self.assertEqual(
            self.db._add_node(
                name=spec['name'],
                host=spec['host'],
                port=spec['port'],
                subscriptions=spec['subscriptions'],),
            self.NODE_ADDED)
        self.assertLastRequestMatchesSpec(spec)
        self.assertEqual(len(responses.calls), 1)

    @responses.activate
    def test_update_node(self):
        """ Update a node in a cluster.
        """
        spec = {
            'operation': 'update_node',
            'name': 'anotherNode',
            'host': 'hostname',
            'port': 31415,
            'subscriptions': [
                {
                    'channel': 'dev:dog',
                    'subscribe': False,
                    'publish': True,
                },
            ]
        }
        responses.add(
            'POST',
            self.URL,
            json=self.NODE_ADDED,
            status=200)

        self.assertEqual(
            self.db._update_node(
                name=spec['name'],
                host=spec['host'],
                port=spec['port'],
                subscriptions=spec['subscriptions'],),
            self.NODE_ADDED)
        self.assertLastRequestMatchesSpec(spec)
        self.assertEqual(len(responses.calls), 1)

    @responses.activate
    def test_remove_node(self):
        """ Remove a node from a cluster.
        """
        spec = {
            'operation': 'remove_node',
            'name': 'anotherNode',
        }
        responses.add(
            'POST',
            self.URL,
            json=self.NODE_REMOVED,
            status=200)

        self.assertEqual(
            self.db._remove_node(name=spec['name']),
            self.NODE_REMOVED)
        self.assertLastRequestMatchesSpec(spec)
        self.assertEqual(len(responses.calls), 1)

    @responses.activate
    def test_cluster_status(self):
        """ Retrieve cluster status.
        """
        spec = {
            'operation': 'cluster_status',
        }
        responses.add(
            'POST',
            self.URL,
            json=self.CLUSTER_STATUS,
            status=200)

        self.assertEqual(self.db._cluster_status(), self.CLUSTER_STATUS)
        self.assertLastRequestMatchesSpec(spec)
        self.assertEqual(len(responses.calls), 1)

    @responses.activate
    def test_registration_info(self):
        """ Retrieve registration info.
        """
        spec = {
            'operation': 'registration_info',
        }
        responses.add(
            'POST',
            self.URL,
            json=self.REGISTRATION,
            status=200)

        self.assertEqual(self.db._registration_info(), self.REGISTRATION)
        self.assertLastRequestMatchesSpec(spec)
        self.assertEqual(len(responses.calls), 1)

    @responses.activate
    def test_get_fingerprint(self):
        """ Retrieve fingerprint.
        """
        spec = {
            'operation': 'get_fingerprint',
        }
        responses.add(
            'POST',
            self.URL,
            json=self.FINGERPRINT,
            status=200)

        self.assertEqual(self.db._get_fingerprint(), self.FINGERPRINT)
        self.assertLastRequestMatchesSpec(spec)
        self.assertEqual(len(responses.calls), 1)

    @responses.activate
    def test_set_license(self):
        """ Set license.
        """
        spec = {
            'operation': 'set_license',
            'key': 'myLicenseKey',
            'company': 'myCompany',
        }
        responses.add(
            'POST',
            self.URL,
            json=self.SET_LICENSE,
            status=200)

        self.assertEqual(
            self.db._set_license(spec['key'], spec['company']),
            self.SET_LICENSE)
        self.assertLastRequestMatchesSpec(spec)
        self.assertEqual(len(responses.calls), 1)
