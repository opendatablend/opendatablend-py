import os
from frictionless import Package
import requests
import glob
# pip install azure-storage-blob
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__

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
def get_data(dataset_path, resource_name, base_path='/', access_key='',connection_string=None):

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

    file_name = data_file_name.split("/")[-1]

    if not connection_string == None:
        upload_blob_odb(data_file_name,connection_string)

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


def upload_blob_odb(local_path,connection_string):
    # Create the BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_name = 'data'
    #Get the list of the Container Name
    containers = blob_service_client.list_containers()
    container_list = [c.name for c in containers]

    # if "data" container is not avalable create container with "data" name
    if container_name not in container_list:
        # Create the container
        container_client = blob_service_client.create_container(container_name)
        print("A new conatiner is created in the name of ", container_name)
    else:
        print(f"The {container_name} container name is already present")

    #Setup and container client
    container_client = blob_service_client.get_container_client(container_name)
    #Get the list of blob present inside the container
    # List the blobs in the container
    blob_list = container_client.list_blobs()
    blob_list_conatiner = [blob.name for blob in blob_list]

    local_folder_path = local_path.rsplit("/",1)[0]
    # print(f"filename: {local_folder_path}")
    filelist = glob.glob(os.path.join(local_folder_path, "*"))
    # print(filelist)
    for i in range(len(filelist)):
        local_file_name = filelist[i].split("\\")[-1]
        if local_file_name not in blob_list_conatiner:
            # Create a blob client using the local file name as the name for the blob
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=local_file_name)

            print("\nUploading to Azure Storage as blob:\n\t" + local_file_name)

            # Upload the created file
            with open(filelist[i], "rb") as data:
                blob_client.upload_blob(data)
        else:
            print(f"{local_file_name} files are already present")


if __name__ == "__main__":
    dataset_path = 'https://packages.opendatablend.io/v1/open-data-blend-road-safety/datapackage.json'
    resource_name = 'date-parquet'
    access_key=''
    connection_string = "DefaultEndpointsProtocol=https;AccountName=shoebwork01;AccountKey=QLtWqXuJqDVIezRG6eM76ND9yY4XxmJsFyRdJp7iawbo2iVPee4xEWHmrfa7SkVnOmtB72FicgJNrXEc7Zf0Pw==;EndpointSuffix=core.windows.net"
    output_data = get_data(dataset_path, resource_name, base_path='/', access_key='',connection_string=connection_string)