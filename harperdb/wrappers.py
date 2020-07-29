import csv
import datetime

from .exceptions import HarperDBError
from .harperdb_base import HarperDBBase, HarperDBError


class HarperDBWrapper(HarperDBBase):

    """ HarperDBWrapper provides a high-level, object-oriented interface for
    HarperDB. From this top-level object an application programmer can make
    references to schemas, tables, and records, while making minimal
    transactions with the server when values are used or modified. Each
    instance of HarperDBWrapper represents a running HarperDB instance at a
    URL, passed to the constructor. Optionally implement Basic Auth as keyword
    arguments.

    Schemas are subscriptable by name, and iterating yields instances of
    HarperDBSchema. The length of a HarperDBWrapper instance returns the number
    of schemas in the target database. HarperDB API functions are implemented
    as low-level instance methods, which produce and consume JSON following the
    API documentation.

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

    High-Level Methods:
      - create_schema(name): Create a schema, returns HarperDBSchema
      - drop_schema(name): Drop a schema

    Low-Level Methods:
    These methods expose the HarperDB API functions, and return JSON from the
    target database instance at HarperDBWrapper.url
      Schemas and Tables:
        - _describe_all()
        - _create_schema(schema)
        - _describe_schema(schema)
        - _drop_schema(schema)
        - _create_table(schema, table, hash_attribute)
        - _describe_table(schema, table)
        - _drop_table(schema, table)
        - _drop_attribute(schema, table, attribute)
      NoSQL Operations:
        - _insert(schema, table, [records])
        - _update(schema, table, [records])
        - _delete(schema, table, [hashes])
        - _search_by_hash(schema, table, [hashes], get_attributes=['*'])
        - _search_by_value(schema,
                           table,
                           search_attribute,
                           search_value,
                           get_attributes=['*'])
      SQL Operations:
        - _sql(SQL)
      CSV Operations:
        - _csv_data_load(schema, table, path, action="insert")
        - _csv_file_load(schema, table, file_path, action="insert")
        - _csv_url_load(schema, table, csv_url, action="insert")
      Users and Roles:
        - _add_user(role id, username, password, active=True)
        - _add_role(name, permission)
        - _alter_user(role, username, password, active=True)
        - _alter_role(id, permission)
        - _drop_role(id)
        - _drop_user(username)
        - _user_info(username)
        - _list_roles()
        - _list_users()
      Clustering:
        - _add_node(name, host, port, subscriptions)
        - _update_node(name, host, port, subscriptions)
        - _remove_node(name)
        - _cluster_status()
      Registration:
        - _registration_info()
        - _get_fingerprint()
        - _set_license(key, company)
      Utilities:
        - _delete_files_before(schema, table, date)
        - _export_local(path,
                        search_operation,
                        search_attribute=None,
                        search_value=None,
                        hash_values=None,
                        sql=None,
                        format="json")
        - _export_to_s3(aws_access_key_id,
                        aws_secret_access_key,
                        bucket,
                        key,
                        search_operation,
                        search_attribute=None,
                        search_value=None,
                        hash_value=None,
                        sql=None,
                        format="json")
        - _read_log(limit=1000,
                    start=0,
                    from_date=None,
                    until_date=None,
                    order="desc")
        - _system_information()
      Jobs:
        - _get_job(id)
    """

    def __getitem__(self, key):
        return HarperDBSchema(key, self)

    def __delitem__(self, key):
        self._drop_schema(key)

    def __iter__(self):
        # get a current dict of schemas and iterate over that
        return _HarperDBSchemas(self.__get_schemas())

    def __len__(self):
        return len(self.__get_schemas())

    def __get_schemas(self):
        """ Returns a dictionary of schemas, subscriptable by name.
        """
        return_value = dict()
        schemas = self._describe_all()
        for schema, tables in schemas.items():
            schema_object = HarperDBSchema(schema, self)
            return_value[schema] = schema_object
        return return_value

    def create_schema(self, name):
        """ Create a schema in this database.
        """
        self._create_schema(name)
        return HarperDBSchema(name, self)

    def drop_schema(self, name):
        """ Drop a schema from this database.
        """
        self._drop_schema(name)


class HarperDBRecord():

    """ Record data is subscriptable by record data key, and supports item
    assignment. Record metadata is stored in instance attributes.

    You should never need to instantiate this class directly, use
    HarperDBTable.upsert instead.

    Instance Attributes:
      - created_time (datetime.datetime): equal to __createdtime__,
        timezone naive
      - table (HarperDBTable): Instance of parent table
      - updated_time (datetime.datetime): equal to __updatedtime__,
        timezone naive
      - __createdtime__ (int): Epoch time in milliseconds
      - __updatedtime__ (int): Epoch time in milliseconds

    Instance Methods:
      - delete(): Delete this record
      - to_dict(): Returns record data as a dictionary
    """

    def __init__(self, table, hash_value):
        assert isinstance(table, HarperDBTable)
        self.table = table
        self._hash_value = hash_value

    def __getitem__(self, key):
        try:
            return self.table.schema.database._search_by_hash(
                schema=self.table.schema.name,
                table=self.table.name,
                hash_values=[self._hash_value])[0][key]
        except IndexError:
            raise HarperDBError(
                'record with hash \"{}\" not found in \"{}.{}\"'.format(
                    self._hash_value,
                    self.table.schema.name,
                    self.table.name))

    def __setitem__(self, key, value):
        self.table.schema.database._update(
            schema=self.table.schema.name,
            table=self.table.name,
            records={key: value})

    def delete(self):
        """ Delete this record.
        """
        self.table.schema.database._delete(
            schema=self.table.schema.name,
            table=self.table.name,
            hash_values=[self._hash_value])

    def to_dict(self):
        record = self.table.schema.database._search_by_hash(
            schema=self.table.schema.name,
            table=self.table.name,
            hash_values=[self._hash_value])[0]
        return_value = dict()
        for key, value in record.items():
            if key in ['__createdtime__', '__updatedtime__']:
                continue
            return_value[key] = value
        return return_value

    @property
    def created_time(self):
        return datetime.datetime.fromtimestamp(self.__createdtime__ / 1000)

    @property
    def updated_time(self):
        return datetime.datetime.fromtimestamp(self.__updatedtime__ / 1000)

    @property
    def __createdtime__(self):
        records = self.table.schema.database._search_by_hash(
            schema=self.table.schema.name,
            table=self.table.name,
            hash_values=[self._hash_value])
        try:
            # HarperDB returns a list of records, empty if none found
            record = records[0]
        except IndexError:
            raise HarperDBError(HarperDBBase.ERROR_HASH.format(
                self._hash_value))
        return record['__createdtime__']

    @property
    def __updatedtime__(self):
        records = self.table.schema.database._search_by_hash(
            schema=self.table.schema.name,
            table=self.table.name,
            hash_values=[self._hash_value])
        try:
            # HarperDB returns a list of records, empty if none found
            record = records[0]
        except IndexError:
            raise HarperDBError(HarperDBBase.ERROR_HASH.format(
                self._hash_value))
        return record['__updatedtime__']


class HarperDBSchema():

    """ Tables are subscriptable by name, and iterating yields
    instances of HarperDBTable. The length a HarperDBSchema instance returns
    the number of tables in the schema. Schema metadata is contained in
    instance attributes.

    You should never need to instantiate this class directly, use
    HarperDBWrapper.create_schema instead.

    Instance Attributes:
      - name (string): Name of this schema
      - database (HarperDBWrapper): Instance of the parent database

    Instance Methods:
      - create_table(name): Create a table, returns HarperDBTable
      - drop(): Drop this schema
      - drop_table(name): Drop a table
    """

    def __init__(self, name, database):
        assert isinstance(database, HarperDBWrapper)
        self.name = name
        self.database = database

    def __delitem__(self, key):
        self.database._drop_table(schema=self.name, table=key)

    def __getitem__(self, key):
        return HarperDBTable(name=key, schema=self)

    def __iter__(self):
        # get a current list of tables and iterate over that
        return _HarperDBTables(self.database._describe_schema(self.name), self)

    def __len__(self):
        return len(self.database._describe_schema(self.name))

    def create_table(self, name, hash_attribute):
        """ Create a table in this schema.
        """
        self.database._create_table(
            schema=self.name,
            table=name,
            hash_attribute=hash_attribute)
        return HarperDBTable(
            name=name,
            schema=self,
            hash_attribute=hash_attribute)

    def drop(self):
        """ Drop this schema.
        """
        self.database._drop_schema(self.name)

    def drop_table(self, name):
        """ Drop a table from this schema.
        """
        self.database._drop_table(schema=self.name, table=name)


class HarperDBTable():

    """ Records are subscriptable by hash_attribute, but HarperDBTable is not
    iterable. The length of a HarperDBTable instance returns the number of
    records in the table. Table metadata is contained in instance attributes.

    You should never need to instantiate this class directly, use
    HarperDBSchema.create_table instead.

    Instance Attributes:
      - attributes (list): All record attributes (string) in this table
      - created_time (datetime.datetime): equal to __createdtime__,
        timezone naive
      - hash_attribute (string): Primary key of this table
      - id (string): Unique identifier assigned to this table
      - name (string): Name of this table
      - record_count (int): Number of records in this table
      - schema (HarperDBSchema): Instance of the parent schema
      - updated_time (datetime.datetime): equal to __updatedtime__,
        timezone naive
      - __createdtime__ (int): Epoch time in milliseconds
      - __updatedtime__ (int): Epoch time in milliseconds

    Instance Methods:
      - delete(hash): Delete a record by hash value
      - drop(): Drop this table
      - search_by_value(search_attribute, search_value): Return a list of
        matching HarperDBRecord instances.
      - upsert(record): Insert a record from a dictionary, or list of
        dictionaries. If the a value is given for the table's hash_attribute,
        and this table has a matching record, that record will be updated. Any
        records skipped by the server will be omitted from the return value.
      - upsert_from_csv(path): Insert records from a CSV file, with headers in
        the first row. Any records which have a value for the table's
        hash_attribute will be updated. Any records skipped by the server will
        be omitted from the return value.
    """

    def __init__(self, name, schema, hash_attribute=None):
        assert isinstance(schema, HarperDBSchema)
        self.name = name
        self.schema = schema
        self._hash_attribute = hash_attribute

    def __getitem__(self, key):
        return HarperDBRecord(table=self, hash_value=key)

    def __delitem__(self, key):
        response = self.schema.database._delete(
            schema=self.schema.name,
            table=self.name,
            hash_values=[key])
        if response['skipped_hashes']:
            raise HarperDBError(HarperDBBase.ERROR_HASH.format(key))

    def __len__(self):
        table = self.schema.database._describe_table(
            schema=self.schema.name,
            table=self.name)
        return table['record_count']

    def delete(self, hash_value):
        """ Delete a record from this table.
        """
        # TODO: support list of hash_values
        response = self.schema.database._delete(
            schema=self.schema.name,
            table=self.name,
            hash_values=[hash_value])
        if response['skipped_hashes']:
            raise HarperDBError(HarperDBBase.ERROR_HASH.format(hash_value))

    def drop(self):
        """ Drop this table.
        """
        self.schema.database._drop_table(
            schema=self.schema.name,
            table=self.name)

    def search_by_value(self, search_attribute, search_value):
        """ Returns a list of HarperDBRecord instances for each record found
        where the search_attribute of the record matches seach_value. Wild
        cards (*) are allowed.
        """
        records = self.schema.database._search_by_value(
            schema=self.schema.name,
            table=self.name,
            search_attribute=search_attribute,
            search_value=search_value)
        return_value = list()
        for record in records:
            return_value.append(HarperDBRecord(
                table=self,
                hash_value=record[self.hash_attribute]))
        return return_value

    def upsert(self, records):
        """ Insert a record from a dictionary, or list of dictionaries. If a
        value is given for the table's hash_attribute, and this table has a
        matching record, that record will be updated. Any records skipped by
        the server will be omitted from the return value.
        """
        # list in, list out
        return_list = True
        if not isinstance(records, list):
            records = [records]
            return_list = False
        # insert records
        insert_return_json = self.schema.database._insert(
            schema=self.schema.name,
            table=self.name,
            records=records)
        inserted_hashes = insert_return_json['inserted_hashes']
        inserted_hashes = [str(hash) for hash in inserted_hashes]
        upserted_hashes = inserted_hashes
        skipped_hashes = insert_return_json['skipped_hashes']
        skipped_hashes = [str(hash) for hash in skipped_hashes]
        # any skipped records need to be updated
        if skipped_hashes:
            # make a list of records to update
            records_to_update = list()
            for record in records:
                if record.get(self.hash_attribute) in skipped_hashes:
                    records_to_update.append(record)
            update_return_json = self.schema.database._update(
                schema=self.schema.name,
                table=self.name,
                records=records_to_update)
            upserted_hashes += update_return_json['update_hashes']
        if not return_list:
            # return a single record
            if upserted_hashes:
                return HarperDBRecord(
                    table=self,
                    hash_value=upserted_hashes[0])
            # else, the record was skipped
            return
        # return a list of records
        return_value = list()
        for hash_value in upserted_hashes:
            return_value.append(HarperDBRecord(
                table=self,
                hash_value=hash_value))
        return return_value

    def upsert_from_csv(self, path):
        """ Insert records from a CSV file, with headers in the first row. Any
        records which have a value for the table's hash_attribute will be
        updated. Any records skipped by the server will be omitted from the
        return value.
        """
        # simply pass file contents to upsert() and return its result
        records = list()
        with open(path, newline='') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            for row in csv_reader:
                records.append(row)
        return self.upsert(records)

    @property
    def attributes(self):
        table = self.schema.database._describe_table(
            schema=self.schema.name,
            table=self.name)
        attributes = list()
        for attribute in table['attributes']:
            if attribute['attribute'] == '__createdtime__':
                continue
            if attribute['attribute'] == '__updatedtime__':
                continue
            attributes.append(attribute['attribute'])
        return attributes

    @property
    def created_time(self):
        return datetime.datetime.fromtimestamp(self.__createdtime__ / 1000)

    @property
    def hash_attribute(self):
        if not self._hash_attribute:
            table = self.schema.database._describe_table(
                schema=self.schema.name,
                table=self.name)
            self._hash_attribute = table['hash_attribute']
        return self._hash_attribute

    @property
    def id(self):
        table = self.schema.database._describe_table(
            schema=self.schema.name,
            table=self.name)
        return table['id']

    @property
    def record_count(self):
        return len(self)

    @property
    def updated_time(self):
        return datetime.datetime.fromtimestamp(self.__updatedtime__ / 1000)

    @property
    def __createdtime__(self):
        table = self.schema.database._describe_table(
            schema=self.schema.name,
            table=self.name)
        return table['__createdtime__']

    @property
    def __updatedtime__(self):
        table = self.schema.database._describe_table(
            schema=self.schema.name,
            table=self.name)
        return table['__updatedtime__']


class _HarperDBSchemas():

    """ Iterator created from a schemas dictionary.
    """

    def __init__(self, schemas):
        self.schemas = schemas
        self.schema_list = list(self.schemas)
        self.schema_list_index = 0

    def __next__(self):
        try:
            key = self.schema_list[self.schema_list_index]
        except IndexError:
            self.schema_list_index = 0
            raise StopIteration
        self.schema_list_index += 1
        return self.schemas[key]


class _HarperDBTables():

    """ Iterator created from a list of tables and HarperDBSchema instance.
    """

    def __init__(self, tables, schema):
        if not isinstance(tables, list):
            # older versions of HarperDB return a list of tables in a schema,
            # convert incoming dictionaries to a list for iteration
            tables = [table for table in tables.values()]
        self.schema = schema
        self.tables = tables
        self.table_list_index = 0

    def __next__(self):
        try:
            table = self.tables[self.table_list_index]
        except IndexError:
            self.table_list_index = 0
            raise StopIteration
        self.table_list_index += 1
        return HarperDBTable(
            name=table['name'],
            schema=self.schema,
            hash_attribute=table['hash_attribute'])
