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
    USER_ADDED = {
        'message': 'user successfully added',
    }
    USER_ALTERED = {
        "message": "updated 1 of 1 records",
        "skipped_hashes": [],
        "new_attributes": [],
        "update_hashes": [
            "user"
        ]
    }
    USER_DROPPED = {
        "message": "user successfully deleted"
    }
    USER_INFO = {
        "__createdtime__": 1234567890000,
        "__updatedtime__": 1234567890002,
        "active": True,
        "role": {
            "__createdtime__": 1234567890000,
            "__updatedtime__": 1234567890002,
            "id": "aUniqueID",
            "permission": {
                "super_user": False
            },
            "role": "developer"
        },
        "username": "user"
    }
    LIST_USERS = [
        {
            "__createdtime__": 1234567890000,
            "__updatedtime__": 1234567890002,
            "active": True,
            "role": {
                "__createdtime__": 1234567890000,
                "__updatedtime__": 1234567890002,
                "id": "aUniqueID",
                "permission": {
                    "super_user": False
                },
                "role": "developer"
            },
            "username": "user"
        }
    ]
    ADD_ROLE = {
        'role': 'developer',
        'permission': {
            'super_user': False,
            'dev': {
                'tables': {
                    'dog': {
                        'read': True,
                        'insert': True,
                        'update': True,
                        'delete': False,
                        'attribute_restrictions': [],
                    },
                },
            },
        },
        'id': 'aUniqueID',
        '__updatedtime__': 1234567890000,
        '__createdtime__': 1234567890002,
    }
    ALTER_ROLE = {
        "message": "updated 1 of 1 records",
        "skipped_hashes": [],
        "new_attributes": [],
        "update_hashes": [
            "1ef29958-7c9b-4aad-a4f5-12e72f989758"
        ]
    }
    DROP_ROLE = {
        "message": "developer successfully deleted"
    }
    LIST_ROLES = [
        {
            'role': 'developer',
            'permission': {
                'super_user': False,
                'dev': {
                    'tables': {
                        'dog': {
                            'read': True,
                            'insert': True,
                            'update': True,
                            'delete': False,
                            'attribute_restrictions': [],
                        },
                    },
                },
            },
            'id': 'aUniqueID',
            '__updatedtime__': 1234567890000,
            '__createdtime__': 1234567890002,
        }
    ]
    GET_JOB = [
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
    ]
    START_JOB = {
        'message': 'Starting job with id aUniqueID'
    }
    DROP_ATTRIBUTE = {
        'message': 'successfully deleted attribute',
    }
    NODE_ADDED = {
        "message": "successfully added node to manifest"
    }
    NODE_UPDATED = {
        "message": "successfully updated dummy"
    }
    CLUSTER_STATUS = {
        "is_enabled": False,
        "node_name": 0
    }
    NODE_REMOVED = {
        "message": "successfully removed node from manifest"
    }
    REGISTRATION = {
        "registered": True,
        "version": "2.1.2",
        "storage_type": "lmdb",
        "ram_allocation": 1024,
        "license_expiration_date": 0,
    }
    FINGERPRINT = {
        "message": "aUniqueFingerprint"
    }
    # need a real sample response, this is just an assumed placeholder
    SET_LICENSE = {
        "message": "success"
    }
    READ_LOG = {
        'file': [
            {
                'level': 'LEVEL',
                'message': 'MESSAGE',
                'timestamp': '2020-01-01T00:00:00.000Z'
            },
        ],
    }
    SYSTEM_INFORMATION = {
        "system": {},
        "time": {},
        "cpu": {},
        "memory": {},
        "disk": {},
        "network": {},
        "harperdb_processes": {}
    }

    def assertLastRequestMatchesSpec(self, spec):
        """ Helper method to assert that the body of the last request made
        matches spec.
        """
        payload = json.loads(responses.calls[-1].request.body)
        self.assertDictEqual(payload, spec)
