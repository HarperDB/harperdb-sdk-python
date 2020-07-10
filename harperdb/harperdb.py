import base64
import json
import requests

from .exceptions import *


class HarperDB():

    """ Each instance of HarperDB represents a running HarperDB instance at a
    URL, passed to the constructor. Optionally implement Basic Auth as keyword
    arguments.  HarperDB API functions are exposed as instance methods, which
    produce and consume JSON following the API documentation.

    Instance Parameters:
      - url (string): Full URL of HarperDB instance
      - username (string): (optional) Basic Auth username
      - password (string): (optional) Basic Auth password
      - timeout (float): Seconds to wait for a server response, default 10

    Instance Attributes:
      - token (string): Value used in Authorization header, or None. The value
        is generated automatically when instantiated with both username and
        password
      - timeout (float): Seconds to wait for a server response
      - url (string): Full URL of HarperDB instance

    Instance Methods:
    These methods expose the HarperDB API functions, and return JSON from the
    target database instance at HarperDB.url
      Schemas and Tables:
        - describe_all()
        - create_schema(schema)
        - describe_schema(schema)
        - drop_schema(schema)
        - create_table(schema, table, hash_attribute)
        - describe_table(schema, table)
        - drop_table(schema, table)
        - drop_attribute(schema, table, attribute)
      NoSQL Operations:
        - insert(schema, table, [records])
        - update(schema, table, [records])
        - delete(schema, table, [hashes])
        - search_by_hash(schema, table, [hashes], get_attributes=['*'])
        - search_by_value(schema,
                          table,
                          search_attribute,
                          search_value,
                          get_attributes=['*'])
      SQL Operations:
        - sql(SQL)
      CSV Operations:
        - csv_data_load(schema, table, path, action="insert")
      Jobs:
        - get_job(id)
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

    def create_schema(self, schema):
        return self.__make_request({
            'operation': 'create_schema',
            'schema': schema,
        })

    def drop_schema(self, schema):
        return self.__make_request({
            'operation': 'drop_schema',
            'schema': schema,
        })

    def describe_schema(self, schema):
        return self.__make_request({
            'operation': 'describe_schema',
            'schema': schema,
        })

    def create_table(self, schema, table, hash_attribute):
        return self.__make_request({
            'operation': 'create_table',
            'schema': schema,
            'table': table,
            'hash_attribute': hash_attribute,
        })

    def describe_table(self, schema, table):
        return self.__make_request({
            'operation': 'describe_table',
            'schema': schema,
            'table': table,
        })

    def describe_all(self):
        return self.__make_request({
            'operation': 'describe_all',
        })

    def drop_table(self, schema, table):
        return self.__make_request({
            'operation': 'drop_table',
            'schema': schema,
            'table': table,
        })

    def drop_attribute(self, schema, table, attribute):
        return self.__make_request({
            'operation': 'drop_attribute',
            'schema': schema,
            'table': table,
            'attribute': attribute,
        })

    # NoSQL Operations

    def insert(self, schema, table, records):
        return self.__make_request({
            'operation': 'insert',
            'schema': schema,
            'table': table,
            'records': records,
        })

    def update(self, schema, table, records):
        return self.__make_request({
            'operation': 'update',
            'schema': schema,
            'table': table,
            'records': records,
        })

    def delete(self, schema, table, hash_values):
        return self.__make_request({
            'operation': 'delete',
            'schema': schema,
            'table': table,
            'hash_values': hash_values,
        })

    def search_by_hash(
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

    def search_by_value(
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

    def sql(self, sql_string):
        return self.__make_request({
            'operation': 'sql',
            'sql': sql_string,
        })

    # CSV Operations

    def csv_data_load(self, schema, table, path, action='insert'):
        with open(path) as csv_file:
            data = csv_file.read()
        return self.__make_request({
            'operation': 'csv_data_load',
            'action': action,
            'schema': schema,
            'table': table,
            'data': data,
        })

    # Jobs

    def get_job(self, id):
        return self.__make_request({
            'operation': 'get_job',
            'id': id,
        })
