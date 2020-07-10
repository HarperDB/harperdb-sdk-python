# harperdb

Python3 implementations of HarperDB API functions. Also provides wrappers for an object-oriented interface.

### Installation

```
pip3 install harperdb
```

### Requirements

- Python>=3.4
- [requests~=2.0](https://pypi.org/project/requests/)
- [responses~=0.10](https://pypi.org/project/responses/) (required for testing only)

---

# harperdb.HarperDB

Each instance of `HarperDB` represents a running HarperDB instance at a URL, passed to the constructor. Optionally implement Basic Auth as keyword arguments. HarperDB API functions are exposed as instance methods, which produce and consume JSON following the API documentation.

```
import harperdb
db = harperdb.HarperDB(
    url=HARPERDB_URL,
    username=HARPERDB_USERNAME,
    password=HARPERDB_PASSWORD)
```

#### Instance Parameters:

  - **url** (string): Full URL of HarperDB instance
  - **username** (string): (optional) Basic Auth username
  - **password** (string): (optional) Basic Auth password
  - **timeout** (float): Seconds to wait for a server response, default 10

#### Instance Attributes:

  - **token** (string): Value used in Authorization header, or `None`. The value
    is generated automatically when instantiated with both username and
    password
  - **timeout** (float): Seconds to wait for a server response
  - **url** (string): Full URL of HarperDB instance

#### Instance Methods:

These methods expose the HarperDB API functions, and return JSON from the target database instance at `HarperDB.url`

Schemas and Tables:

- **describe_all()**
- **create_schema(schema)**
- **describe_schema(schema)**
- **drop_schema(schema)**
- **create_table(schema, table, hash_attribute)**
- **describe_table(schema, table)**
- **drop_table(schema, table)**
- **drop_attribute(schema, table, attribute)**

NoSQL Operations:

- **insert(schema, table, [records])**
- **update(schema, table, [records])**
- **delete(schema, table, [hashes])**
- **search_by_hash(schema, table, [hashes], get_attributes=['*'])**
- **search_by_value(schema, table, search_attribute, search_value, get_attributes=['*'])**

SQL Operations:

- **sql(SQL)**

CSV Operations:

- **csv_data_load(schema, table, path, action="insert")**

Jobs:

- **get_job(id)**

---

# harperdb.wrappers.HarperDBWrapper

`HarperDBWrapper` provides a high-level, object-oriented interface for HarperDB. From this top-level object an application programmer can make references to schemas, tables, and records, while making minimal transactions with the server when values are used or modified. Each instance of `HarperDBWrapper` represents a running HarperDB instance at a URL, passed to the constructor. Optionally implement Basic Auth as keyword arguments.

Schemas are subscriptable by name, and iterating yields instances of `HarperDBSchema`. The length of a `HarperDBWrapper` instance returns the number of schemas in the target database. HarperDB API functions are implemented as low-level instance methods, which produce and consume JSON following the API documentation.

```
import harperdb
db = harperdb.wrappers.HarperDBWrapper(
    url=HARPERDB_URL,
    username=HARPERDB_USERNAME,
    password=HARPERDB_PASSWORD)
dev_schema = db.create_schema('dev')
len(db)  # returns 1
db['dev']  # returns dev_schema
for schema in db:
    schema.name  # returns "dev"
```

Schemas can be dropped using `HarperDB.drop_schema`, or using the `del` keyword and `HarperDBSchema.name` value like a dictionary:

```
db.drop_schema('dev')
# same as
del db['dev']
```

#### Instance Parameters:

  - **url** (string): Full URL of HarperDB instance
  - **username** (string): (optional) Basic Auth username
  - **password** (string): (optional) Basic Auth password
  - **timeout** (float): Seconds to wait for a server response, default 10

#### Instance Attributes:

- **token** (string): Value used in Authorization header, or None. The value is generated automatically when instantiated with both username and password
- **timeout** (float): Seconds to wait for a server response
- **url** (string): Full URL of HarperDB instance

#### High-Level Instance Methods:
- **create_schema(name)**: Create a schema, returns `HarperDBSchema`
- **drop_schema(name)**: Drop a schema

#### Low-Level Instance Methods:
These methods expose the HarperDB API functions, and return JSON from the target database instance at `HarperDBWrapper.url`

Schemas and Tables:

- **_describe_all()**
- **_create_schema(schema)**
- **_describe_schema(schema)**
- **_drop_schema(schema)**
- **_create_table(schema, table, hash_attribute)**
- **_describe_table(schema, table)**
- **_drop_table(schema, table)**
- **_drop_attribute(schema, table, attribute)**

NoSQL Operations:

- **_insert(schema, table, [records])**
- **_update(schema, table, [records])**
- **_delete(schema, table, [hashes])**
- **_search_by_hash(schema, table, [hashes], get_attributes=['*'])**
- **_search_by_value(schema, table, search_attribute, search_value, get_attributes=['*'])**

SQL Operations:

- **_sql(SQL)**

CSV Operations:

- **_csv_data_load(schema, table, path, action="insert")**

Jobs:

- **_get_job(id)**

---

# harperdb.wrappers.HarperDBSchema

Tables are subscriptable by name, and iterating yields instances of `HarperDBTable`. The length of a `HarperDBSchema` instance returns the number of tables in the schema. Schema metadata is contained in instance attributes.

You should never need to instantiate this class directly, use `HarperDBWrapper.create_schema` instead.

```
dog_table = dev_schema.create_table(
        name='dog',
        hash_attribute='id')
len(dev_schema)  # returns 1
dev_schema.database  # returns db
```

Tables can be dropped using `HarperDBSchema.drop_table`, or using the `del` keyword and `HarperDBTable.name` value like a dictionary:

```
dev_schema.drop_table('dog')
# same as
del dev_schema['dog']
```

Schemas can be dropped using the instance method `dev_schema.drop()`.

#### Instance Attributes:

- **name** (string): Name of this schema
- **database** (HarperDBWrapper): Instance of the parent database

#### Instance Methods:

- **create_table(name)**: Create a table, returns `HarperDBTable`
- **drop()**: Drop this schema
- **drop_table(name)**: Drop a table

---

# harperdb.wrappers.HarperDBTable

Records are subscriptable by `hash_attribute`, but `HarperDBTable` is not iterable. The length of a `HarperDBTable` instance returns the number of records in the table. Table metadata is contained in instance attributes.

You should never need to instantiate this class directly, use `HarperDBSchema.create_table` instead.

`HarperDBTable.upsert` inserts a record from a dictionary, and updates records if a value is given for the table's `hash_attribute` and a matching record is found in the table.

```
penny = dog_table.upsert({
    'id': 1,
    'dog_name': 'Penny',
    'owner_name': 'Kyle',
    'breed_id': 154,
    'age': 5,
    'weight_lbs': 35,
    'adorable': True,
})
dog_table[1]  # returns penny
dog_table.record_count  # same as len(dog_table)
dog_table.__createdtime__  # returns int, Unix time with milliseconds
dog_table.created_time  # returns datetime.datetime
dog_table.hash_attribute  # returns "id"
dog_table.schema  # returns dev_schema
```

`HarperDBTable.upsert` accepts either a dictionary of record data, or a list of such dictionaries, returning an instance of `HarperDBRecord` for each record. Any records skipped by the server are omitted from the return value.

Use `HarperDBTable.upsert_from_csv` to load record data in bulk from a CSV file. Returns an instance of `HarperDBRecord` for each record. Any records skipped by the server are omitted from the return value.

Records can be deleted using `HarperDBTable.delete`, or using the `del` keyword and `HarperDBTable.hash_attribute` value like a dictionary:

```
dog_table.delete(1)
# or
del dog_table[1]
```

Searching by a record value returns a list of matching `HarperDBRecord` instances.

```
dog_table.search_by_value(
    attribute='name',
    value='penny')  # returns a list containing penny
```

Tables can be dropped using the instance method `dog_table.drop()`.

#### Instance Attributes:

- **attributes** (list): All record attributes (string) in this table
- **created_time** (datetime.datetime): equal to `__createdtime__`
- **hash_attribute** (string): Primary key of this table
- **id** (string): Unique identifier assigned to this table
- **name** (string): Name of this table
- **record_count** (int): Number of records in this table
- **schema** (HarperDBSchema): Instance of the parent schema
- **updated_time** (datetime.datetime): equal to `__updatedtime__`
- **\_\_createdtime\_\_** (int): Epoch time in milliseconds
- **\_\_updatedtime\_\_** (int): Epoch time in milliseconds

#### Instance Methods:

- **delete(hash)**: Delete a record by hash value
- **drop()**: Drop this table
- **search_by_value(search_attribute, search_value)**: Return a list of
matching `HarperDBRecord` instances.
- **upsert(record)**: Insert a record from a dictionary, or list of dictionaries. If a value is given for the table's hash_attribute, and this table has a matching record, that record will be updated. Any records skipped by the server will be omitted from the return value. Returns `HarperDBRecord`, or a list of `HarperDBRecord` instances.
- **upsert_from_csv(path)**: Insert records from a CSV file, with headers in the first row. Any records which have a value for the table's `hash_attribute` will be updated. Any records skipped by the server will be omitted from the return value. Returns a list of `HarperDBRecord` instances.

---

# harperdb.wrappers.HarperDBRecord

Record data is subscriptable by record data key, and supports item assignment. Record metadata is stored in instance attributes.

You should never need to instantiate this class directly, use `HarperDBTable.upsert` instead.

```
penny['owner_name']  # returns "Kyle"
penny['age'] = 6  # Happy Birthday!
penny.__updatedtime__  # returns int, Unix time with milliseconds
penny.updated_time  # returns datetime.datetime
penny.table  # returns dog_table
```

Records can be deleted using the instance method `penny.delete()`.

`HarperDBRecord.to_dict()` returns a dictionary of the record.

#### Instance Attributes:

- **created_time** (datetime.datetime): equal to `__createdtime__`
- **table** (HarperDBTable): Instance of parent table
- **updated_time** (datetime.datetime): equal to `__updatedtime__`
- **\_\_createdtime\_\_** (int): Epoch time in milliseconds
- **\_\_updatedtime\_\_** (int): Epoch time in milliseconds

#### Instance Methods:

- **delete()**: Delete this record
- **to_dict()**: Returns record data as a dictionary

---

# harperdb.exceptions.HarperDBError

Raised when the server returns an error (500), or a hash is not found.

This is the only Exception raised explicitly.
