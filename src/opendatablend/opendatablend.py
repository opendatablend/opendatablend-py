import os
from frictionless import Package
import requests
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__

# Open Data Blend API base URL
base_url = 'https://packages.opendatablend.io'
base_url_local_substitution = 'opendatablend'

class Output:   
    def __init__(self, data_file_name, metadata_file_name):
        self.data_file_name = data_file_name
        self.metadata_file_name = metadata_file_name
    
    
# Get and cache a data file and the dataset metadata
def get_data(dataset_path, resource_name, base_path='/', access_key='', file_system='local', configuration={}):    
        
    # Get the dataset metadata
    dataset = Package(dataset_path)
    
    # Get the data file metadata
    data_file = dataset.get_resource(resource_name)
    
    # Set the fully qualified data file name to mirror the logical folder structure at the server
    data_file_name = base_path + data_file.path.replace(base_url, base_url_local_substitution)
    
    # Cache the file in the specified file system
    output_data_file_name = cache_date_file(data_file, data_file_name, access_key, file_system)

    # Save a copy of the dataset metadata to the specific file system
    if dataset_path.startswith('http'):
        output_metadata_file_name = cache_dataset_metadata(dataset, base_path, file_system)
    
    output = Output(output_data_file_name, output_metadata_file_name)
    
    # Return the output object which contains the fully qualified file names
    return output


def cache_date_file(data_file, data_file_name, access_key, file_system):
    
    if file_system == "local":
        output_data_file_name = cache_data_file_to_local_file_system(data_file, access_key, data_file_name)
    elif file_system == "azure_blob_storage":
        output_data_file_name = cache_data_file_azure_blob_storage_file_system(data_file, access_key, data_file_name, configuration)           
    else:
        print("No data file could be cached. Please specify a supported file system.")
    
    return output_data_file_name

def cache_data_file_to_local_file_system(data_file, access_key, data_file_name):

    # Create the directory for the data file if it doesn't exist
    if not os.path.exists(os.path.dirname(data_file_name)):
        try:
            os.makedirs(os.path.dirname(data_file_name))
        except OSError as ex:
            if ex.errno != os.errno.EEXIST:
                raise
                
    # Only download the data file if it doesn't exist
    if not os.path.exists(data_file_name):
        if access_key != '':
            data = requests.get(data_file.path + '?accesskey=' + access_key, stream=True)
        else:
            data = requests.get(data_file.path + access_key, stream=True)
        
        # Download th data file using a 4 MB chunk size
        local_file = open(data_file_name,'wb')
        for chunk in data.iter_content(chunk_size=4 * 1024 * 1024):
            local_file.write(chunk)
    
        local_file.close()

    return output_data_file_name  

def cache_data_file_azure_blob_storage_file_system(data_file, access_key, data_file_name, configuration):
    
    # Get the Azure Blob Storage configurations
    connection_string =  configuration["connection_string"]
    container_name = configuration["container_name"]

    # Create the blob client
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # Create the blob container if it doesn't exist
    container_client = blob_service_client.get_container_client(container_name)
    if not container_client.exists():
        container_client.create_container()

    # Remove the leading slash
    output_data_file_name = data_file_name.replace("/opendatablend","opendatablend")

    # Only upload the data file if it doesn't exist
    blob_client = container_client.get_blob_client(output_data_file_name)

    if not blob_client.exists():
        if access_key != '':
            data = requests.get(data_file.path + '?accesskey=' + access_key, stream=True)
        else:
            data = requests.get(data_file.path + access_key, stream=True)

        for chunk in data.iter_content(chunk_size=4 * 1024 * 1024):
            blob_client.upload_blob(chunk, overwrite=True)

    return output_data_file_name


def cache_dataset_metadata(dataset, base_path, file_system):

    metadata_data_file_snapshot_path = dataset.get('snapshot_path')
    
    # Set the fully qualified metadata file name to mirror the logical folder structure at the server
    metadata_file_name = base_path + metadata_data_file_snapshot_path.replace(base_url, base_url_local_substitution)

    if file_system == "local":
        output_metadata_file_name = cache_dataset_metadata_to_local_file_system(metadata_data_file_snapshot_path, metadata_file_name)
    elif file_system == "azure_blob_storage":
        output_metadata_file_name = cache_dataset_metadata_to_azure_blob_storage_file_system(metadata_data_file_snapshot_path, metadata_file_name, configuration)
    else:
        print("No metadata file could be cached. Please specify a supported file system.")
    
    return output_metadata_file_name

def cache_dataset_metadata_to_local_file_system(metadata_data_file_snapshot_path, metadata_file_name):
    
    # Create the directory for the dataset metadata file if it doesn't exist
    if not os.path.exists(os.path.dirname(metadata_file_name)):
        try:
            os.makedirs(os.path.dirname(metadata_file_name))
        except OSError as ex:
            if ex.errno != os.errno.EEXIST:
                raise 
                
    # Download the dataset metadata file if it doesn't exist
    if not os.path.exists(metadata_file_name):
        data = requests.get(metadata_data_file_snapshot_path)
        
        open(metadata_file_name, 'wb').write(data.content)
    
    # Return the fully qualified metadata file name so it can be used
    return metadata_file_name

def cache_dataset_metadata_to_azure_blob_storage_file_system(metadata_data_file_snapshot_path, metadata_file_name, configuration):

    # Get the Azure Blob Storage configurations
    connection_string =  configuration["connection_string"]
    container_name = configuration["container_name"]

    # Create the blob client
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # Create the blob container if it doesn't exist
    container_client = blob_service_client.get_container_client(container_name)
    if not container_client.exists():
        container_client.create_container()

    # Remove the leading slash
    output_metadata_file_name = metadata_file_name.replace("/opendatablend","opendatablend")

    # Only upload the data file if it doesn't exist
    blob_client = container_client.get_blob_client(output_metadata_file_name)

    if not blob_client.exists():
        data = requests.get(metadata_data_file_snapshot_path, stream=True)

        for chunk in data.iter_content(chunk_size=4 * 1024 * 1024):
            blob_client.upload_blob(chunk, overwrite=True)

    # Return the fully qualified metadata file name so it can be used
    return output_metadata_file_name