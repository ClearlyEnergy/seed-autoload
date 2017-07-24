import os
import time

from django.core.files.storage import FileSystemStorage
from django.conf import settings
from django.utils import timezone

import seed.data_importer.tasks as tasks
from seed.models.certification import (
    GreenAssessmentProperty,
    GreenAssessmentPropertyAuditLog
)
from seed.models.properties import PropertyView
from seed.models import (
    Cycle,
    Column
)
from seed.data_importer.models import (
    ImportFile,
    ImportRecord
)
from seed.utils.cache import get_cache


class AutoLoad:
    def __init__(self, user, org):
        self.org = org
        self.user = user

    def autoload_file(self, data, dataset_name, cycle_id,  col_mappings):
        # make a new data set
        dataset_id = self.create_dataset(dataset_name)

        # upload and save to Property state table
        file_id = self.upload(data, dataset_id, cycle_id)

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

    """Make repeated calls to progress API endpoint until progress is
       Reported to be completed (progress == 100)"""
    def wait_for_task(self, key):
        prog = 0
        while prog < 100:
            prog = int(get_cache(key)['progress'])
            time.sleep(1)

    """Create a new import record for the specified organization with the
       specified name"""
    def create_dataset(self, name):
        record = ImportRecord.objects.create(
                name=name,
                app='seed',
                start_time=timezone.now(),
                created_at=timezone.now(),
                last_modified_by=self.user,
                super_organization=self.org,
                owner=self.user
        )
        return record.id

    """Upload a file to the specified import record"""
    def upload(self, data, record_id, cycle_id):
        filename = "autoload"
        path = settings.MEDIA_ROOT + "/uploads/" + filename
        path = FileSystemStorage().get_available_name(path)

        # verify the directory exists
        if not os.path.exists(os.path.dirname(path)):
            os.makedirs(os.path.dirname(path))

        # save the file
        with open(path, 'wb+') as temp_file:
            temp_file.write(data)

        record = ImportRecord.objects.get(pk=record_id)

        f = ImportFile.objects.create(
                import_record=record,
                uploaded_filename=filename,
                file=path,
                cycle=Cycle.objects.get(pk=cycle_id),
                source_type="Assessed Raw")
        return f.pk

    """Initiate task on seed server to save file data into propertystate
       table"""
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
        status1 = Column.create_mappings(mappings, org, self.user)

        column_mappings = [
            {'from_field': m['from_field'],
             'to_field': m['to_field'],
             'to_table_name': m['to_table_name']} for m in mappings]

        if status1:
            import_file.save_cached_mapped_columns(column_mappings)
            return {'status': 'success'}
        else:
            return {'status': 'error'}

    """ Populate fields in PropertyState according to previously established
        Mapping"""
    def perform_mapping(self, file_id):
        return tasks.map_data(file_id)

    """ The server needs to be informed that we are finished with all mapping
        for this file. Not sure what exactly this does but it seems important"""
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
    def create_green_assessment_property(self, file_id, assessment_data, org_id, address):
        view = PropertyView.objects.filter(state__address_line_1=address)

        property_list = GreenAssessmentProperty.objects.filter(view=view[0])

        if(not property_list.exists() or property_list[0].assessment != assessment_data['assessment']):
            assessment_data.update({'view': view[0]})
            green_property = GreenAssessmentProperty.objects.create(**assessment_data)
            green_property.initialize_audit_logs()
            green_property.save()
        else:
            green_property = property_list[0]
            old_audit_log = GreenAssessmentPropertyAuditLog.objects.filter(greenassessmentproperty=green_property).order_by('created').last()
            green_property.pk = None
            for (key, value) in assessment_data.items():
                setattr(green_property, key, value)
            green_property.save()
            green_property.log(
                    changed_fields=assessment_data,
                    ancestor=old_audit_log.ancestor,
                    parent=old_audit_log)

        return {'status': 'success'}
