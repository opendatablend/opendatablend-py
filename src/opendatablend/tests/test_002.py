import opendatablend as odb

dataset_path = 'https://packages.opendatablend.io/v1/open-data-blend-road-safety/datapackage.json'

# Specify the resource name of the data file. In this example, the 'date' data file will be requested in .parquet format.
resource_name = 'date-parquet'

# Get the data and store the output object
output = odb.get_data(dataset_path, resource_name)

# Print the file locations
print(output.data_file_name)
print(output.metadata_file_name)