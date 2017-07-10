"""Test for the autload module"""
import unittest

from autoload import autoload
import StringIO

# These tests assume that the seed server is running at the location specified
# in urlbase. It might be a good idea to have the test suite start the server
# automaticly at some known location. Like wise, they assume that the 
# authorization provided is correct. have the tests make a test user and org
# after connecting to the server might also be a good idea. The tests will also 
# make changes to the database that the server connects to. It would be ideal 
# to have the tests reverse all changes or perhaps make changes to some 
# temporary test db.
class AutoloadTest(unittest.TestCase):

    def __init__(self,args):
        # Change these values to reflect local setup
        urlbase = "http://localhost:8000"
        self.loader = autoload.AutoLoad(urlbase,'demo@example.com','676e837f22c17b6321a58d03ea0333a058005a20')
        unittest.TestCase.__init__(self,args)

    # Tests tha all api calls made by the loader are successfull
    def test_autoload(self):
        col_mappings = [{"from_field": "Address",
                     "to_field": "address_line_1",
                     "to_table_name": "PropertyState",
                    },
                    {"from_field": "Score",
                     "to_field": "energy_score",
                     "to_table_name": "PropertyState",
                    }]
        file_handle = StringIO.StringIO('Address,klfjgkldsjg\n123 Test Road,100')
        dataset_name = 'TEST'
        cycle_id = '1'
        org_id = '1'

        # make a new data set
        resp = self.loader.create_dataset(dataset_name, org_id)
        self.assertEqual(resp['status'], 'success')
        dataset_id = resp['id']

        # upload and save to Property state table
        resp = self.loader.upload(file_handle,dataset_id)
        self.assertEqual(resp['success'], True)
        file_id = resp['import_file_id']

        resp = self.loader.save_raw_data(file_id,cycle_id,org_id)
        self.assertEqual(resp['status'], 'not-started')
        save_prog_key = resp['progress_key']
        self.loader.wait_for_task(save_prog_key)


        # perform column mapping
        self.loader.save_column_mappings(org_id, file_id, col_mappings)
        resp = self.loader.perform_mapping(file_id,org_id)
        self.assertEqual(resp['status'], 'success')
        map_prog_key = resp['progress_key']

        self.loader.wait_for_task(map_prog_key)
        resp = self.loader.mapping_done(file_id,org_id)
        self.assertEqual(resp['status'], 'success')


        # attempt to match with existing records
        resp = self.loader.start_system_matching(file_id,org_id)
        self.assertEqual(resp['status'], 'success')
        match_prog_key = resp['progress_key']
        self.loader.wait_for_task(match_prog_key)

    def test_is_string(self):
        print self
        s = autoload.test()
        print s
        self.assertTrue(isinstance(s, basestring))
        
        
# Things to test (sketch)
# Success, i.e. test for succesful completion of entire process
# Failure if column headers don't match and/or column names can't be mapped automatically
# Failure if input format won't work
# Test new records created (this probably duplicates existing test functionality in SEED)
# Test new information merged into existing record (this probably also duplicates existing test functionality in SEED)
