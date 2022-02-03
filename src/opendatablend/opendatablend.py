import os
from frictionless import Package
import requests
import glob
# pip install azure-storage-blob
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
# pip install azure-storage-file-datalake
from azure.storage.filedatalake import DataLakeServiceClient
import datetime

# Open Data Blend API base URL
base_url = 'https://packages.opendatablend.io'
base_url_local_substitution = 'opendatablend'

class Output:
    def __init__(self, data_file_name, metadata_file_name):
        self.data_file_name = data_file_name
        self.metadata_file_name = metadata_file_name
        print(data_file_name)
        print(metadata_file_name)


# Get and cache a data file and the dataset metadata
def get_data(dataset_path, resource_name, base_path='/', access_key='',azure_dict ={}):

    # Get the dataset metadata
    dataset = Package(dataset_path)

    # Get the data file metadata
    data_file = dataset.get_resource(resource_name)

    # Set the fully qualified data file name to mirror the logical folder structure at the server
    data_file_name = base_path + data_file.path.replace(base_url, base_url_local_substitution)

    # Create the directory for the data file if it doesn't exist
    if not os.path.exists(os.path.dirname(data_file_name)):
        try:
            os.makedirs(os.path.dirname(data_file_name))
        except OSError as ex:
            if ex.errno != os.errno.EEXIST:
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

    if azure_dict['connection_string'] != "" and azure_dict['Container_name']!="":
        Azure_storage(data_file_name,azure_dict)

    # Return the output object which contains the fully qualified file names
    return output


def cache_dataset_metadata(dataset, base_path):

    metadata_data_file_snapshot_path = dataset.get('snapshot_path')

    # Set the fully qualified metadata file name to mirror the logical folder structure at the server
    metadata_file_name = base_path + metadata_data_file_snapshot_path.replace(base_url, base_url_local_substitution)

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


def Azure_storage(local_path,azure_dict):
    # azure_dict = Azure_dict
    connection_string = azure_dict['connection_string']
    container_name = azure_dict['Container_name']
    # ISO date for date folder creation
    ISO_date = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    # global service_client
    service_client = DataLakeServiceClient.from_connection_string(connection_string)
    # List the cointainer
    containers = service_client.list_file_systems()
    container_list = [c.name for c in containers]

    if container_name not in container_list:
        # Create the container
        file_system_client = service_client.create_file_system(file_system=container_name)
        print("A new conatiner is created in the name of ", container_name)
    else:
        print(f"The {container_name} container name is already present")
    # Select the cointainer and get the file system
    file_system_client = service_client.get_file_system_client(file_system=container_name)
    # create the directory and subdirectory file name to mirror the logical folder structure at the server
    target_path = local_path.rsplit("/",2)[0]
    print(target_path)
    target_dir = file_system_client.create_directory(f"{target_path}/{ISO_date}")
    local_folder_path = local_path.rsplit("/",1)[0]
    # list all upload files
    filelist = glob.glob(os.path.join(local_folder_path, "*"))

    for i in range(len(filelist)):
        local_file_name = filelist[i].split("\\")[-1]
        print(local_file_name)
        file_client = target_dir.create_file(local_file_name) #change local file name
        local_file = open(filelist[i],'rb') #filelist
        file_contents = local_file.read()
        # upload the file
        file_client.upload_data(file_contents, overwrite=True)
    print("Data update on Azure Successfully")