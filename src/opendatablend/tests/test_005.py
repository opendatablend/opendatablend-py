import opendatablend as odb

dataset_path = 'https://packages.opendatablend.io/v1/open-data-blend-road-safety/datapackage.json'
access_key = '<ACCESS_KEY_HERE>' # The access key can be set to an empty string if you are making a public API request

# Specify the resource name of the data file. In this example, the 'date' data file will be requested in .parquet format.
resource_name = 'date-parquet'

# Get the data and store the output object using the Amazon S3 file system
configuration = {
    "aws_access_key_id" : "<AWS_ACCESS_KEY_ID>",
    "aws_secret_access_key" : "AWS_SECRET_ACCESS_KEY",
    "bucket_name" : "<BUCKET_NAME>", # e.g. odbp-integration
    "bucket_region" : "<BUCKET_REGION>" # e.g. eu-west-2
}

output = get_data(dataset_path, resource_name, access_key=access_key, file_system="amazon_s3", configuration=configuration)

# Print the file locations
print(output.data_file_name)
print(output.metadata_file_name)