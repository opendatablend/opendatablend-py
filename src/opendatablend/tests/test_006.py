import opendatablend as odb

dataset_path = 'https://packages.opendatablend.io/v1/open-data-blend-road-safety/datapackage.json'
access_key = '<ACCESS_KEY>' # The access key can be set to an empty string if you are making a public API request

# Specify the resource name of the data file. In this example, the 'date' data file will be requested in .parquet format.
resource_name = 'date-parquet'

# Get the data and store the output object using the Google Cloud Storage file system
configuration = {
    "service_account_private_key_file" : "<PATH_TO_SERVICE_ACCOUNT_PRIVATE_KEY_FILE>",
    "bucket_name" : "<BUCKET_NAME>", # e.g. odbp-integration
    "bucket_location" : "<BUCKET_LOCATION>" # e.g. europe-west2
}

output = odb.get_data(dataset_path, resource_name, access_key=access_key, file_system="google_cloud_storage", configuration=configuration)

# Print the file locations
print(output.data_file_name)
print(output.metadata_file_name)