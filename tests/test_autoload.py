"""Test for the autload module"""
import unittest

from autoload import autoload

# These tests assume that the seed server is running at the location specified
# in urlbase. It might be a good idea to have the test suite start the server
# automaticly at some known location. Like wise, they assume that the 
# authorization provided is correct. have the tests make a test user and org
# after connecting to the server might also be a good idea. The tests will also 
# make changes to the database that the server connects to. It would be ideal 
# to have the tests reverse all changes or perhaps make changes to some 
# temporary test db.

class AutoloadTest(unittest.TestCase):

    def __init__(self):
        # Change these values to reflect local setup
        urlbase = "http://localhost:8000"
        authorization = {'authorization':'demo@example.com:3a3ba54fe467e7e2f239b0e5f9bc24c446b811e3'}
        file_patj
        self.loader = AutoLoad(urlbase,authorization)

    def test_autoload(self):
        mappings = [{"from_field": "Address",
                     "to_field": "address_line_1",
                     "to_table_name": "PropertyState",
                    },
                    {"from_field": "City",
                     "to_field": "city",
                     "to_table_name": "PropertyState",
                    }]
        self.autoload.autoload_file("../test.csv","TEST","1","1", mappings)

        
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
