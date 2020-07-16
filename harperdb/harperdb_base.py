import base64
import json
import requests

from .exceptions import *


class HarperDBBase():

    """ Extensible base class implements HarperDB API functions.
    """

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

    # Jobs

    def _get_job(self, id):
        return self.__make_request({
            'operation': 'get_job',
            'id': id,
        })
