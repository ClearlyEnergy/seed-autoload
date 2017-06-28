import requests
import json
from time import sleep

def test():
    """Entry point for the application script"""
    return("Call your main application code here")    

"""Initialize an instance of this class with the location of the seed sever
   (localhost:8000) and a valid user/api key pair. Auth information should be
   provided as a dictionary {"Authorization":"user:api_key"}"""
class AutoLoad:
    def __init__(self, url_base, auth):
        self.url_base = url_base
        self.auth = auth

    def autoload_file(self, file_path, dataset_name, cycle_id, org_id, col_mappings):
        # make a new data set
        dataset_id = self.create_dataset(dataset_name, org_id)

        # upload and save to Property state table
        file_id = self.upload(file_path,dataset_id)
        save_prog_key = self.save_raw_data(file_id,cycle_id)
        self.wait_for_task(save_prog_key)

        # perform column mapping
        self.save_column_mappings(org_id, file_id, col_mappings)
        map_prog_key = self.perform_mapping(file_id)
        self.wait_for_task(map_prog_key)
        self.mapping_done(file_id,org_id)

        # attempt to match with existing records
        match_prog_key = self.start_system_matching(file_id)
        self.wait_for_task(match_prog_key)

    """Make repeated calls to progress API endpoint until progress is
       Reported to be completed (progress == 100)"""
    def wait_for_task(self, key):
        url = self.url_base + '/api/v2/progress/'

        data = {
            'progress_key' : key
        }

        progress = 0
        while progress < 100:
            r = requests.post(url,headers=self.auth,data=data)
            print r.json()
            progress = int(r.json()['progress'])
            # delay before next request to limit number of requests sent to server
            sleep(0.5)

    """Create a new import record for the specified organization with the
       specified name"""
    def create_dataset(self, name, org_id):
        url = self.url_base + '/api/v2/datasets/?organization_id=' + org_id

        form_data = {
            'name' : name
        }

        r = requests.post(url,headers=self.auth,data=form_data)

        record_id = r.json()['id']
        return record_id

    """Upload a file to the specified import record"""
    def upload(self, file_path, record_id):
        url = self.url_base + '/api/v2/upload/'

        upload = {
            'qqfile' : open(file_path,'r'),
        }

        form_data = {
            'import_record' : record_id
        }

        r = requests.post(url,headers=self.auth,files=upload,data=form_data)

        file_id = r.json()['import_file_id']
        return file_id

    """Initiate task on seed server to save file data into propertystate
       table"""
    def save_raw_data(self, file_id, cycle_id):
        url = self.url_base + '/api/v2/import_files/%(file_id)s/save_raw_data/' % {'file_id' : file_id}

        form_data = {
            'cycle_id' : cycle_id
        }

        r = requests.post(url,headers=self.auth,data=form_data)

        progress_key = r.json()['progress_key']
        return progress_key


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
    def save_column_mappings(self, org_id, file_id, mappings):
        url = self.url_base + '/api/v2/import_files/%(file_id)s/save_column_mappings/' % {'file_id':file_id}

        data = {"organization_id": org_id}
        data.update({"mappings":mappings})

        # This requests requires content-type to be json
        head = {'Content-Type': 'application/json'}
        head.update(self.auth)

        # also note data must be run through json.dumps before posting
        r = requests.post(url,headers=head,data=json.dumps(data))

    """ Populate fields in PropertyState according to previously established
        Mapping"""
    def perform_mapping(self, file_id):
        url = self.url_base + '/api/v2/import_files/%(file_id)s/perform_mapping/' % {'file_id':file_id}

        r = requests.post(url,headers=self.auth)

        progress_key = r.json()['progress_key']
        return progress_key

    """ The server needs to be informed that we are finished with all mapping
        for this file. Not sure what exactly this does but it seems important"""
    def mapping_done(self, file_id, org_id):
        url = self.url_base + '/api/v2/import_files/%(file_id)s/mapping_done/' % {'file_id':file_id}

        r = requests.put(url,headers=self.auth,params={"organization_id":org_id})

    """ Attempts to find existing entries in PropertyState that correspond to the
        same property that was uploaded and merge them into a new entry"""
    def start_system_matching(self, file_id):
        url = self.url_base + '/api/v2/import_files/%(file_id)s/start_system_matching/' % {'file_id':file_id}

        r = requests.post(url,headers=self.auth)

        progress_key = r.json()['progress_key']
        return progress_key
