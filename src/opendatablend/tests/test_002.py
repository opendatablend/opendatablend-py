import opendatablend as odb

dataset_path = 'https://packages.opendatablend.io/v1/open-data-blend-road-safety/datapackage.json'
access_key = '<ACCESS_KEY_HERE>'

# Specify the resource name of the data file. In this example, the 'date' data file will be requested in .parquet format.
resource_name = 'date-parquet'

# Get the data and store the output object
output = odb.get_data(dataset_path, resource_name, access_key=access_key)

# Print the file locations
print(output.data_file_name)
print(output.metadata_file_name)