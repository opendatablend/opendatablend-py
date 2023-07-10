
import opendatablend as odb

dataset_path = 'https://packages.opendatablend.io/v1/open-data-blend-road-safety/datapackage.json'
access_key = '<ACCESS_KEY_HERE>' # The access key can be set to an empty string if you are making a public API request

# Specify the resource names of the data files. In this example, a subset of the available data files will be requested in Parquet format.
resource_names = [
    'date-parquet',
    'time-of-day-parquet',
    'geolocation-parquet',
    'road-safety-accident-info-parquet',
    'road-safety-accident-location-parquet',
    'road-safety-accident-2021-parquet'
    ]

# Get the data and store the output object using the Azure Blob Storage file system
configuration = {
    "connection_string" : "DefaultEndpointsProtocol=https;AccountName=<AZURE_BLOB_STORAGE_ACCOUNT_NAME>;AccountKey=<AZURE_BLOB_STORAGE_ACCOUNT_KEY>;EndpointSuffix=core.windows.net",
    "container_name" : "<AZURE_BLOB_STORAGE_CONTAINER_NAME>" # e.g. odbp-integration
    }
output = odb.get_data_files(dataset_path, resource_names, access_key=access_key, file_system="azure_blob_storage", configuration=configuration)

# Print the file locations
print(output.data_file_names)
print(output.metadata_file_name)