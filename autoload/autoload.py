import os
import time, calendar
import json

from django.core.files.storage import default_storage, FileSystemStorage
from django.conf import settings

import seed.data_importer.tasks as tasks
from helix.models import HELIXGreenAssessmentProperty, HelixMeasurement
import helix.helix_utils
from seed.models.certification import (
    GreenAssessment,
    GreenAssessmentURL,
    GreenAssessmentPropertyAuditLog
)
from seed.models.properties import PropertyView
from seed.models import Column
from seed.models.auditlog import (
    AUDIT_USER_EDIT,
    AUDIT_USER_CREATE,
    AUDIT_USER_EXPORT,
    DATA_UPDATE_TYPE
)
from seed.data_importer.models import ImportFile
from seed.utils.cache import get_cache

class AutoLoad:
    def __init__(self, user, org):
        self.org = org
        self.user = user

    def autoload_file(self, file_id, col_mappings):
        # upload and save to Property state table
#        file_id = self.upload('autoload.csv', data, dataset, cycle)

        resp = self.save_raw_data(file_id)
        if (resp['status'] == 'error'):
            return resp
        save_prog_key = resp['progress_key']
        self.wait_for_task(save_prog_key)

        # perform column mapping
        self.save_column_mappings(file_id, col_mappings)
        resp = self.perform_mapping(file_id)
        if (resp['status'] == 'error'):
            return resp
        map_prog_key = resp['progress_key']

        self.wait_for_task(map_prog_key)
        self.mapping_done(file_id)

        # attempt to match with existing records
        resp = self.start_system_matching(file_id)
        if (resp['status'] == 'error'):
            return resp
        match_prog_key = resp['progress_key']
        self.wait_for_task(match_prog_key)

        return {'status': 'success', 'import_file_id': file_id}

    """ wait for a celery task to finish running"""
    def wait_for_task(self, key):
        prog = 0
        while prog < 100:
            prog = int(get_cache(key)['progress'])
            
            # Call to sleep is required otherwise this method will hang. It
            # could be maybe to reduced less than 1 second.
            time.sleep(1.0)

    """Upload a file to the specified import record"""
    def upload(self, filename, data, dataset, cycle):
        if 'S3' in settings.DEFAULT_FILE_STORAGE:
            path = 'data_imports/' + filename + '.'+ str(calendar.timegm(time.gmtime())/1000)
            temp_file = default_storage.open(path, 'w')
            temp_file.write(data)
            temp_file.close()
        else:        
            path = settings.MEDIA_ROOT + "/uploads/" + filename
            path = FileSystemStorage().get_available_name(path)

            # verify the directory exists
            if not os.path.exists(os.path.dirname(path)):
                os.makedirs(os.path.dirname(path))

            # save the file
            with open(path, 'wb+') as temp_file:
                temp_file.write(data)

        f = ImportFile.objects.create(
                import_record=dataset,
                uploaded_filename=filename,
                file=path,
                cycle=cycle,
                source_type="Assessed Raw")
        return f.pk

    """Initiate task to save file data into propertystate tabel"""
    def save_raw_data(self, file_id):
        r = tasks.save_raw_data(file_id)
        return r

    """Tell the seed server how to map between the fields in the input file and
       those in the PropertyState table

       Sample mappings:

       [{"from_field": "Address",
         "to_field": "address_line_1",
         "to_table_name": "PropertyState",
        },
        {"from_field": "City",
         "to_field": "city",
         "to_table_name": "PropertyState",
        }],
    """
    def save_column_mappings(self, file_id, mappings):
        import_file = ImportFile.objects.get(pk=file_id)
        org = self.org
        status = Column.create_mappings(mappings, org, self.user)

        column_mappings = [
            {'from_field': m['from_field'],
             'to_field': m['to_field'],
             'to_table_name': m['to_table_name']} for m in mappings]

        if status:
            import_file.save_cached_mapped_columns(column_mappings)
            return {'status': 'success'}
        else:
            return {'status': 'error'}

    """ Populate fields in PropertyState according to previously established
        Mapping"""
    def perform_mapping(self, file_id):
        return tasks.map_data(file_id)

    """ The server needs to be informed that we are finished with all mapping
        for this file. Not sure what exactly this does but it seems
        important"""
    def mapping_done(self, file_id):
        tasks.finish_mapping(file_id, True)

    """ Attempts to find existing entries in PropertyState that correspond to the
        same property that was uploaded and merge them into a new entry"""
    def start_system_matching(self, file_id):
        return tasks.match_buildings(file_id)

    """ adds a green_assessment_property to a recently uploaded property.
        If another file has been merged with the property since the file with
        the given id was uploaded then this method will fail.

        assesssment_data must be a dictionart with entries:
            : Parameter: source
            : Description:  source of this certification e.g. assessor
            : required: false
            : Parameter: status
            : Description:  status for multi-step processes
            : required: false
            : Parameter: status_date
            : Description:  date status first applied
            : required: false
            : Parameter: metric
            : Description:  score if value is numeric
            : required: false
            : Parameter: rating
            : Description:  score if value is non-numeric
            : required: false
            : Parameter: version
            : Description:  version of certification issued
            : required: false
            : Parameter: date
            : Description:  date certification issued  ``YYYY-MM-DD``
            : required: false
            : Parameter: target_date
            : Description:  date achievement expected ``YYYY-MM-DD``
            : required: false
            : Parameter: eligibility
            : Description:  BEDES eligible if true
            : required: false
            : Parameter: urls
            : Description:  array of related green assessment urls
            : required: false
            : Parameter: assessment
            : Description:  id of associated green assessment
    """
    def create_green_assessment_property(self, assessment_data, address, postal_code):
        
        # a green assessment property needs to be associated with a
        # property view. I'm using address as the key to find the correct view.
        data_log = {'created': False, 'updated': False}
        view = PropertyView.objects.filter(state__normalized_address=address, state__postal_code=postal_code, state__organization=self.org)
        if len(view) > 1:
            print(address1 + ' has duplicates')
            return
        else:
            view = view.first()

        # pull urls out of dict for use later
        green_assessment_urls = assessment_data.pop('urls', [])

        green_property = None
        priorAssessments = HELIXGreenAssessmentProperty.objects.filter(
                view=view,
                assessment=assessment_data['assessment'])
        if 'reference_id' in assessment_data:
            priorAssessments = priorAssessments.filter(
                reference_id=assessment_data['reference_id']
            )
            
        if(not priorAssessments.exists()):
            # If the property does not have an assessment in the database
            # for the specifed assesment type createa new one.
            assessment_data.update({'view': view})
            green_property = HELIXGreenAssessmentProperty.objects.create(**assessment_data)
            green_property.initialize_audit_logs(user=self.user)
            green_property.save()
            data_log['created'] = True
        else:
            # find most recently created property and a corresponding audit log
            green_property = priorAssessments.order_by('date').last()
            old_audit_log = GreenAssessmentPropertyAuditLog.objects.filter(greenassessmentproperty=green_property).exclude(record_type=AUDIT_USER_EXPORT).order_by('created').last()

            # update fields
            green_property.pk = None
            for (key, value) in assessment_data.items():
                setattr(green_property, key, value)
            green_property.save()

            # log changes
            green_property.log(
                    changed_fields=assessment_data,
                    ancestor=old_audit_log.ancestor,
                    parent=old_audit_log,
                    user=self.user)
            data_log['updated'] = True

        # add any urls provided in assessment data to the url table
        for url in green_assessment_urls:
            if (url != ''):
                GreenAssessmentURL.objects.get_or_create(
                     url=url,
                     property_assessment=green_property)

        return data_log, green_property

