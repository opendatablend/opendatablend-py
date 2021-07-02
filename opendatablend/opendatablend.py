import os
from frictionless import Package
import requests

# Open Data Blend API base URL
base_url = 'https://packages.opendatablend.io'

class Output:   
    def __init__(self, data_file_name, metadata_file_name):
        self.data_file_name = data_file_name
        self.metadata_file_name = metadata_file_name
    
    
# Help to get and cache a data file and the corresponding metadata
def get_data(dataset_path, resource_name, base_path='/opendatablend', access_key=''):    
        
    # Get the dataset metadata
    dataset = Package(dataset_path)
    
    # Get the data file metadata
    data_file = dataset.get_resource(resource_name)
    
    # Set the fully qualified file name to mirror the logical folder structure at the server
    data_file_name = base_path + data_file.path.replace(base_url, '')
    
    # Create the directory for the data file if it doesn't exist
    if not os.path.exists(os.path.dirname(data_file_name)):
        try:
            os.makedirs(os.path.dirname(data_file_name))
        except OSError as ex:
            if ex.errno != errno.EEXIST:
                raise
                
    # Download the data file if it doesn't exist
    if not os.path.exists(data_file_name):
        if access_key != '':
            data = requests.get(data_file.path + '?accesskey=' + access_key, stream=True)
        else:
            data = requests.get(data_file.path + access_key, stream=True)
    
        local_file = open(data_file_name,'wb')
        for chunk in data.iter_content(chunk_size=1024):
            local_file.write(chunk)
    
        local_file.close()                
    
    # Save a local copy of the dataset metadata if it doesn't exist
    if dataset_path.startswith('http'):
        metadata_file_name = cache_dataset_metadata(dataset, base_path)
    
    output = Output(data_file_name, metadata_file_name)
    
    # Return the fully qualified name so it can be used
    return output

def cache_dataset_metadata(dataset, base_path):
    
    metadata_data_file_snapshot_path = dataset.get('snapshot_path')
    
    metadata_file_name = base_path + metadata_data_file_snapshot_path.replace(base_url, '')
    
    # Create the directory for the dataset metadata file if it doesn't exist
    if not os.path.exists(os.path.dirname(metadata_file_name)):
        try:
            os.makedirs(os.path.dirname(metadata_file_name))
        except OSError as ex:
            if ex.errno != errno.EEXIST:
                raise 
                
    # Download the dataset metadata file if it doesn't exist
    if not os.path.exists(metadata_file_name):
        data = requests.get(metadata_data_file_snapshot_path)
        
        open(metadata_file_name, 'wb').write(data.content)
    
    # Return the fully qualified metadata file name so it can be used
    return metadata_file_name