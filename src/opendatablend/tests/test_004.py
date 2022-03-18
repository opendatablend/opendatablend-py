import opendatablend as odb

dataset_path = 'https://packages.opendatablend.io/v1/open-data-blend-road-safety/datapackage.json'
access_key = '<ACCESS_KEY_HERE>' # The access key can be set to an empty string if you are making a public API request

# Specify the resource name of the data file. In this example, the 'date' data file will be requested in .parquet format.
resource_name = 'date-parquet'

# Get the data and store the output object using the Azure Data Lake Storage Gen2 file system
configuration = {
    "connection_string" : "DefaultEndpointsProtocol=https;AccountName=<ADLS_GEN2_ACCOUNT_NAME>;AccountKey=<ADLS_GEN2_ACCOUNT_KEY>;EndpointSuffix=core.windows.net",
    "container_name" : "<ADLS_GEN2_CONTAINER_NAME>" # e.g. odbp-integration
}
output = odb.get_data(dataset_path, resource_name, access_key=access_key, file_system="azure_blob_storage", configuration=configuration)

# Print the file locations
print(output.data_file_name)
print(output.metadata_file_name)