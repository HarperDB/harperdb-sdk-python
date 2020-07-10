import json
import responses
import requests
import unittest


class HarperDBTestCase(unittest.TestCase):

    # test parameters for database instance
    URL = 'http://localhost:9925'
    USERNAME = 'user'
    PASSWORD = 'pass'
    # return values for mock server calls
    DESCRIBE_ALL = {
        'test_schema_1': {
            'test_table_1': {
                '__createdtime__': 1234567890000,
                '__updatedtime__': 1234567890002,
                'hash_attribute': 'id',
                'id': 'assignedUUID',
                'name': 'test_table_1',
                'residence': None,
                'schema': 'test_schema_1',
                'attributes': [
                    {
                        'attribute': '__createdtime__',
                    },
                    {
                        'attribute': '__updatedtime__',
                    },
                    {
                        'attribute': 'id',
                    },
                ],
                'record_count': 3,
            },
        },
    }
    DESCRIBE_ALL_UPDATED = {
        'test_schema_1': {
            'test_table_1': {
                '__createdtime__': 1234567890000,
                '__updatedtime__': 1234567890002,
                'hash_attribute': 'id',
                'id': 'assignedUUID',
                'name': 'test_table_1',
                'residence': None,
                'schema': 'test_schema_1',
                'attributes': [
                    {
                        'attribute': '__createdtime__',
                    },
                    {
                        'attribute': '__updatedtime__',
                    },
                    {
                        'attribute': 'id',
                    },
                ],
                'record_count': 3,
            },
        },
        'test_schema_2': {},
    }
    SCHEMA_CREATED = {
        'message': 'schema successfully created',
    }
    SCHEMA_DROPPED = {
        'message': 'successfully deleted schema',
    }
    SCHEMA_EXISTS = {
        'error': 'schema already exists',
    }
    SCHEMA_NOT_EXISTS = {
        'error': 'Schema does not exist'
    }
    DESCRIBE_SCHEMA_1 = {
        'test_schema_1': {
            '__createdtime__': 1234567890000,
            '__updatedtime__': 1234567890002,
            'hash_attribute': 'id',
            'id': 'assignedUUID',
            'name': 'test_table_1',
            'residence': None,
            'schema': 'test_schema_1',
            'attributes': [
                {
                    'attribute': '__createdtime__',
                },
                {
                    'attribute': '__updatedtime__',
                },
                {
                    'attribute': 'id',
                },
            ],
            'record_count': 3,
        },
    }
    # older versions of HarperDB return array of tables
    DESCRIBE_SCHEMA_1__LEGACY = [
        {
            '__createdtime__': 1234567890000,
            '__updatedtime__': 1234567890002,
            'hash_attribute': 'id',
            'id': 'assignedUUID',
            'name': 'test_table_1',
            'residence': None,
            'schema': 'test_schema_1',
            'attributes': [
                {
                    'attribute': '__createdtime__',
                },
                {
                    'attribute': '__updatedtime__',
                },
                {
                    'attribute': 'id',
                },
            ],
            'record_count': 3,
        },
    ]
    DESCRIBE_SCHEMA_2 = {}
    DESCRIBE_SCHEMA_2_UPDATED = {
        'test_schema_2': {
            '__createdtime__': 1234567890000,
            '__updatedtime__': 1234567890002,
            'hash_attribute': 'id',
            'id': 'assignedUUID',
            'name': 'test_table_1',
            'residence': None,
            'schema': 'test_schema_2',
            'attributes': [
                {
                    'attribute': '__createdtime__',
                },
                {
                    'attribute': '__updatedtime__',
                },
                {
                    'attribute': 'id',
                },
            ],
            'record_count': 0,
        },
    }
    # older versions of HarperDB return array of tables
    DESCRIBE_SCHEMA_2_UPDATED__LEGACY = [
        {
            '__createdtime__': 1234567890000,
            '__updatedtime__': 1234567890002,
            'hash_attribute': 'id',
            'id': 'assignedUUID',
            'name': 'test_table_1',
            'residence': None,
            'schema': 'test_schema_2',
            'attributes': [
                {
                    'attribute': '__createdtime__',
                },
                {
                    'attribute': '__updatedtime__',
                },
                {
                    'attribute': 'id',
                },
            ],
            'record_count': 0,
        },
    ]
    DESCRIBE_TABLE = {
        '__createdtime__': 1234567890000,
        '__updatedtime__': 1234567890002,
        'hash_attribute': 'id',
        'id': 'assignedUUID',
        'name': 'test_table_1',
        'residence': None,
        'schema': 'test_schema_1',
        'attributes': [
            {
                'attribute': '__createdtime__',
            },
            {
                'attribute': '__updatedtime__',
            },
            {
                'attribute': 'id',
            },
        ],
        'record_count': 3,
    }
    DESCRIBE_UPDATED_TABLE = {
        '__createdtime__': 1234567890000,
        '__updatedtime__': 1234567890002,
        'hash_attribute': 'id',
        'id': 'assignedUUID',
        'name': 'test_table_1',
        'residence': None,
        'schema': 'test_schema_1',
        'attributes': [
            {
                'attribute': '__createdtime__',
            },
            {
                'attribute': '__updatedtime__',
            },
            {
                'attribute': 'id',
            },
            {
                'attribute': 'name',
            },
        ],
        'record_count': 4,
    }
    TABLE_CREATED = {
        'message': 'table successfully created',
    }
    TABLE_DROPPED = {
        'message': 'successfully deleted table',
    }
    TABLE_EXISTS = {
        'error': 'table already exists',
    }
    TABLE_NOT_EXISTS = {
        'error': 'invalid_table'
    }
    LOGIN_FAILED = {
            'error': 'Login failed',
        }
    RECORDS = [
        {
            '__createdtime__': 1234567890000,
            '__updatedtime__': 1234567890002,
            'id': 'uniqueHash',  # hash attribute
            'pi': 3.14159,  # record data
        },
    ]
    RECORDS_UPDATED = [
        {
            '__createdtime__': 1234567890000,
            '__updatedtime__': 1234567890002,
            'id': 'uniqueHash',
            'pi': 3.14159,
            'foo': 'bar',  # updated record data
        },
    ]
    NO_RECORDS = []
    RECORDS_DELETED = {
        'message': '1 of 1 record successfully deleted',
        'deleted_hashes': [
            1
        ],
        'skipped_hashes': []
    }
    RECORDS_NOT_DELETED = {
        'message': '0 of 1 record successfully deleted',
        'deleted_hashes': [],
        'skipped_hashes': ['invalid_hash']
    }
    RECORD_INSERTED = {
        'message': 'inserted 1 of 1 records',
        'skipped_hashes': [],
        'inserted_hashes': [
            'assignedID_1',
        ],
    }
    RECORD_NOT_INSERTED = {
        'message': 'inserted 0 of 1 records',
        'skipped_hashes': [
            'assignedID_1',
        ],
        'inserted_hashes': [],
    }
    RECORDS_NOT_INSERTED = {
        'message': 'inserted 1 of 2 records',
        'skipped_hashes': [
            '2',
        ],
        'inserted_hashes': [
            '1',
        ],
    }
    RECORDS_INSERTED = {
        'message': 'inserted 2 of 2 records',
        'skipped_hashes': [],
        'inserted_hashes': [
            'assignedID_2',
            'assignedID_3',
        ],
    }
    RECORD_UPSERTED = {
        'message': 'updated 1 of 1 records',
        'skipped_hashes': [],
        'update_hashes': [
            '2',
        ],
    }
    RECORD_PART_UPSERTED = {
        'message': 'updated 1 of 2 records',
        'skipped_hashes': [],
        'update_hashes': [
            '2',
        ],
    }
    RECORDS_UPSERTED = {
        'message': 'updated 2 of 2 records',
        'skipped_hashes': [],
        'update_hashes': [
            '1',
            '2'
        ],
    }
    DOG_RECORD = [
        {
            'id': '2',
            'name': 'Dino',
            'age': '3',
            'color': 'Gray',
        },
    ]
    DOG_RECORDS = [
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
    ]


    def assertLastRequestMatchesSpec(self, spec):
        """ Helper method to assert that the body of the last request made
        matches spec.
        """
        payload = json.loads(responses.calls[-1].request.body)
        self.assertDictEqual(payload, spec)
