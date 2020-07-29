import base64
import json
import requests

from .exceptions import HarperDBError


class HarperDBBase():

    """ Extensible base class implements HarperDB API functions.
    """

    ERROR_HASH = 'Hash value \"{}\" not found'

    def __init__(self, url, username=None, password=None, timeout=10):
        self.url = url
        self.token = None
        if username and password:
            token = '{}:{}'.format(username, password).encode('utf-8')
            token = base64.b64encode(token).decode('utf-8')
            self.token = 'Basic {}'.format(token)
        self.timeout = timeout

    def __make_request(self, data):
        """ Make a POST request to the database instance with JSON data.

        Returns JSON response, raises HarperDBError if the server returns 500.
        """
        headers = {
            'Content-Type': 'application/json',
        }
        if self.token:
            headers['Authorization'] = self.token
        response = requests.request(
            'POST',
            self.url,
            headers=headers,
            data=json.dumps(data),
            timeout=self.timeout)
        body = response.json()
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            raise HarperDBError(body.get('error', 'An unknown error occurred'))
        return body

    # Schemas and Tables

    def _create_schema(self, schema):
        return self.__make_request({
            'operation': 'create_schema',
            'schema': schema,
        })

    def _drop_schema(self, schema):
        return self.__make_request({
            'operation': 'drop_schema',
            'schema': schema,
        })

    def _describe_schema(self, schema):
        return self.__make_request({
            'operation': 'describe_schema',
            'schema': schema,
        })

    def _create_table(self, schema, table, hash_attribute):
        return self.__make_request({
            'operation': 'create_table',
            'schema': schema,
            'table': table,
            'hash_attribute': hash_attribute,
        })

    def _describe_table(self, schema, table):
        return self.__make_request({
            'operation': 'describe_table',
            'schema': schema,
            'table': table,
        })

    def _describe_all(self):
        return self.__make_request({
            'operation': 'describe_all',
        })

    def _drop_table(self, schema, table):
        return self.__make_request({
            'operation': 'drop_table',
            'schema': schema,
            'table': table,
        })

    def _drop_attribute(self, schema, table, attribute):
        return self.__make_request({
            'operation': 'drop_attribute',
            'schema': schema,
            'table': table,
            'attribute': attribute,
        })

    # NoSQL Operations

    def _insert(self, schema, table, records):
        return self.__make_request({
            'operation': 'insert',
            'schema': schema,
            'table': table,
            'records': records,
        })

    def _update(self, schema, table, records):
        return self.__make_request({
            'operation': 'update',
            'schema': schema,
            'table': table,
            'records': records,
        })

    def _delete(self, schema, table, hash_values):
        return self.__make_request({
            'operation': 'delete',
            'schema': schema,
            'table': table,
            'hash_values': hash_values,
        })

    def _search_by_hash(
            self,
            schema,
            table,
            hash_values,
            get_attributes=['*']):
        return self.__make_request({
            'operation': 'search_by_hash',
            'schema': schema,
            'table': table,
            'hash_values': hash_values,
            'get_attributes': get_attributes,
        })

    def _search_by_value(
            self,
            schema,
            table,
            search_attribute,
            search_value,
            get_attributes=['*']):
        return self.__make_request({
            'operation': 'search_by_value',
            'schema': schema,
            'table': table,
            'search_attribute': search_attribute,
            'search_value': search_value,
            'get_attributes': get_attributes,
        })

    # SQL Operations

    def _sql(self, sql_string):
        return self.__make_request({
            'operation': 'sql',
            'sql': sql_string,
        })

    # CSV Operations

    def _csv_data_load(self, schema, table, path, action='insert'):
        with open(path) as csv_file:
            data = csv_file.read()
        return self.__make_request({
            'operation': 'csv_data_load',
            'action': action,
            'schema': schema,
            'table': table,
            'data': data,
        })

    def _csv_file_load(self, schema, table, file_path, action='insert'):
        return self.__make_request({
            'operation': 'csv_file_load',
            'action': action,
            'schema': schema,
            'table': table,
            'file_path': file_path,
        })

    def _csv_url_load(self, schema, table, csv_url, action='insert'):
        return self.__make_request({
            'operation': 'csv_url_load',
            'action': action,
            'schema': schema,
            'table': table,
            'csv_url': csv_url,
        })

    # Users and Roles

    def _add_user(self, role, username, password, active=True):
        return self.__make_request({
            'operation': 'add_user',
            'role': role,
            'username': username,
            'password': password,
            'active': active,
        })

    def _alter_user(self, role, username, password, active=True):
        return self.__make_request({
            'operation': 'alter_user',
            'role': role,
            'username': username,
            'password': password,
            'active': active,
        })

    def _drop_user(self, username):
        return self.__make_request({
            'operation': 'drop_user',
            'username': username,
        })

    def _user_info(self, username):
        return self.__make_request({
            'operation': 'user_info',
            'username': username,
        })

    def _list_users(self):
        return self.__make_request({'operation': 'list_users'})

    def _add_role(self, role, permission):
        return self.__make_request({
            'operation': 'add_role',
            'role': role,
            'permission': permission,
        })

    def _alter_role(self, id, permission):
        return self.__make_request({
            'operation': 'alter_role',
            'id': id,
            'permission': permission,
        })

    def _drop_role(self, id):
        return self.__make_request({
            'operation': 'drop_role',
            'id': id,
        })

    def _list_roles(self):
        return self.__make_request({
            'operation': 'list_roles',
        })

    # Clustering

    def _add_node(self, name, host, port, subscriptions=None):
        if subscriptions:
            return self.__make_request({
                'operation': 'add_node',
                'name': name,
                'host': host,
                'port': port,
                'subscriptions': subscriptions,
            })
        return self.__make_request({
            'operation': 'add_node',
            'name': name,
            'host': host,
            'port': port,
        })

    def _update_node(self, name, host, port, subscriptions=None):
        if subscriptions:
            return self.__make_request({
                'operation': 'update_node',
                'name': name,
                'host': host,
                'port': port,
                'subscriptions': subscriptions,
            })
        return self.__make_request({
            'operation': 'update_node',
            'name': name,
            'host': host,
            'port': port,
        })

    def _remove_node(self, name):
        return self.__make_request({
            'operation': 'remove_node',
            'name': name,
        })

    def _cluster_status(self):
        return self.__make_request({
            'operation': 'cluster_status',
        })

    # Registration

    def _registration_info(self):
        return self.__make_request({
            'operation': 'registration_info',
        })

    def _get_fingerprint(self):
        return self.__make_request({
            'operation': 'get_fingerprint',
        })

    def _set_license(self, key, company):
        return self.__make_request({
            'operation': 'set_license',
            'key': key,
            'company': company,
        })

    # Utilities

    def _delete_files_before(self, schema, table, date):
        return self.__make_request({
            'operation': 'delete_files_before',
            'schema': schema,
            'table': table,
            'date': date,
        })

    def _export_local(
            self,
            path,
            search_attribute=None,
            search_value=None,
            hash_values=None,
            sql=None,
            format='json'):
        call = {
            'operation': 'export_local',
            'path': path,
            'format': format,
        }
        search_operation = dict()
        if search_attribute:
            search_operation['operation'] = 'search_by_value'
            search_operation['search_attribute'] = search_attribute
        if search_value:
            search_operation['operation'] = 'search_by_value'
            search_operation['search_value'] = search_value
        if hash_values:
            search_operation['operation'] = 'search_by_hash'
            search_operation['hash_values'] = hash_values
        if sql:
            search_operation['operation'] = 'sql'
            search_operation['sql'] = sql
        call['search_operation'] = search_operation
        return self.__make_request(call)

    def _export_to_s3(
            self,
            aws_access_key,
            aws_secret_access_key,
            bucket,
            key,
            search_attribute=None,
            search_value=None,
            hash_values=None,
            sql=None,
            format='json'):
        call = {
            'operation': 'export_to_s3',
            'format': format,
            's3': {
                'aws_access_key_id': aws_access_key,
                'aws_secret_access_key': aws_secret_access_key,
                'bucket': bucket,
                'key': key,
            },
        }
        search_operation = dict()
        if search_attribute:
            search_operation['operation'] = 'search_by_value'
            search_operation['search_attribute'] = search_attribute
        if search_value:
            search_operation['operation'] = 'search_by_value'
            search_operation['search_value'] = search_value
        if hash_values:
            search_operation['operation'] = 'search_by_hash'
            search_operation['hash_values'] = hash_values
        if sql:
            search_operation['operation'] = 'sql'
            search_operation['sql'] = sql
        call['search_operation'] = search_operation
        return self.__make_request(call)

    def _read_log(
            self,
            limit=1000,
            start=0,
            from_date=None,
            to_date=None,
            order='desc'):
        # "from" is a keyword in python, so we use from_date and to_date
        return self.__make_request({
            'operation': 'read_log',
            'limit': limit,
            'start': start,
            'from': from_date,
            'until': to_date,
            'order': order,
        })

    def _system_information(self):
        return self.__make_request({
            'operation': 'system_information',
        })

    # Jobs

    def _get_job(self, id):
        return self.__make_request({
            'operation': 'get_job',
            'id': id,
        })

    def _search_jobs_by_start_date(self, from_date, to_date):
        return self.__make_request({
            'operation': 'search_jobs_by_start_date',
            'from_date': from_date,
            'to_date': to_date,
        })
