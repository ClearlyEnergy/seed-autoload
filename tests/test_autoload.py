"""Test for the autload module"""
import unittest

from autoload import autoload

class AutoloadTest(unittest.TestCase):
        
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
