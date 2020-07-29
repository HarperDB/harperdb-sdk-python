import harperdb
import harperdb_testcase


class TestHarperDB(harperdb_testcase.HarperDBTestCase):

    def test_api_functions(self):
        """ API Functions are exposed as instance methods.
        """
        # check that instance methods expose functions from HarperDBBase,
        # raises AttributeError and fails if not implemented.
        # The behavior of these methods is covered in TestHarperDBBase
        db = harperdb.HarperDB(self.URL)

        db.create_schema
        db.drop_schema
        db.describe_schema
        db.create_table
        db.describe_table
        db.describe_all
        db.drop_table
        db.drop_attribute
        db.insert
        db.update
        db.delete
        db.search_by_hash
        db.search_by_value
        db.sql
        db.csv_data_load
        db.csv_file_load
        db.csv_url_load
        db.get_job
