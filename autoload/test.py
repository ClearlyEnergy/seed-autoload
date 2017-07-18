"""Test for the autoload module"""
import autoload
import StringIO
import datetime

from django.test import TestCase
from django.utils import timezone
from seed.landing.models import SEEDUser as User
from seed.models.properties import PropertyState
from seed.models.certification import GreenAssessment, GreenAssessmentProperty
from seed.lib.superperms.orgs.models import Organization, OrganizationUser
from seed.models import Cycle

class AutoloadTest(TestCase):

    def setUp(self):
        user_details = {
            'username': 'test_user@demo.com',
            'password': 'test_pass',
        }
        self.user = User.objects.create(username='test_user@demo.com')
        self.user.set_password('test_pass')
        self.user.email = 'test_user@demo.com'
        self.user.save()

        self.org = Organization.objects.create()
        OrganizationUser.objects.create(user=self.user, organization=self.org)

        self.user.default_organization_id = self.org.id
        self.user.save()

        self.client.login(username='test_user@demo.com',password='test_pass')

        self.cycle = Cycle.objects.create(organization=self.org,user=self.user,name="test",start=timezone.now(),end=timezone.now())
        self.cycle.save()

        self.loader = autoload.AutoLoad(self.user,self.org)

    # test that autoload returns with succes and that there exists a
    # property state with the correct file_id
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

        resp = self.loader.autoload_file(file_handle,dataset_name,self.cycle.pk,col_mappings)
        self.assertEqual(resp['status'],'success')

        file_id = resp['import_file_id']
        self.assertTrue(PropertyState.objects.filter(import_file_id=file_id,address_line_1='123 Test Road').exists())

    # test that upload of GreenAssessmentProperty succeds and that the
    # new record can be found in the database
    def test_green_assessment_property(self):
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

        assessment = GreenAssessment.objects.create(
                name='Home Energy Score',
                award_body='Department of Energy',
                recognition_type='SCR',
                description='Developed by DOE...',
                is_numeric_score=True,
                is_integer_score=True,
                validity_duration=datetime.timedelta(days=365),
                organization=self.org)

        resp = self.loader.autoload_file(file_handle,dataset_name,self.cycle.pk,col_mappings)
        # check that the upload of initial data succeeds
        self.assertEqual(resp['status'],'success')
        file_id = resp['import_file_id']

        green_assessment = {"source":"home energy score",
                            "metric":10,
                            "date":"2017-07-10",
                            "assessment":assessment}

        resp = self.loader.create_green_assessment_property(file_id,green_assessment,self.org.pk,'123 Test Road')

        self.assertEqual(resp['status'],'success')

        self.assertTrue(GreenAssessmentProperty.objects.filter(assessment=assessment,_metric=10,date='2017-07-10').exists())

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
