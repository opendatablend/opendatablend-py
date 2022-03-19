![alt text](https://raw.githubusercontent.com/opendatablend/opendatablend-py/master/images/odblogo.png "Open Data Blend")

# Open Data Blend for Python

Open Data Blend for Python is the fastest way to get data from the Open Data Blend Dataset API. It is a lightweight, easy-to-use extract and load (EL) tool.

It has a single function called `get_data` that can be used to get any data file belonging to an Open Data Blend dataset. The function transparently downloads and caches the data locally or in cloud storage, mirroring the same folder hierarchy as on the remote server. It also caches a copy of the dataset metadata file (datapackage.json) at the point that the data file request is made. The cache is persistent which means the files will be kept until they are deleted.

The versioned dataset metadata can be used to re-download a specific version of a data file (sometimes referred to as 'time travel'). You can learn more about how we version our datasets in the [Open Data Blend Docs](https://docs.opendatablend.io/open-data-blend-datasets/dataset-snapshots).

In addition to downloading the data and metadata files, `get_data` returns an object called `Output` which includes the locations of the downloaded files. From there, you can query and analyse the data directly using something light like [Pandas](https://pandas.pydata.org/) or, for more resource intensive processing, a data lakehouse platform like [Databricks](https://databricks.com/).

# Installation

Install the latest version of `opendatablend` from [PyPI](https://pypi.org/):

```Python
pip install opendatablend
```

# Usage Examples

---
**NOTE**

If you want to run the examples, be sure to replace placeholder values such as  `<ACCESS_KEY>` with appropriate string literals or variables.

---

Some of the following examples require the `pandas` and `pyarrow` packages to be installed:

```Python
pip install pandas
pip install pyarrow
```

## Making Public API Requests

---
**NOTE**

Public API requests have a [monthly limit](https://docs.opendatablend.io/open-data-blend-datasets/dataset-api#usage-limits).

---

### Get The Data

```python
import opendatablend as odb
import pandas as pd

dataset_path = 'https://packages.opendatablend.io/v1/open-data-blend-road-safety/datapackage.json'

# Specify the resource name of the data file. In this example, the 'date' data file will be requested in .parquet format.
resource_name = 'date-parquet'

# Get the data and store the output object
output = odb.get_data(dataset_path, resource_name)

# Print the file locations
print(output.data_file_name)
print(output.metadata_file_name)
```

### Use The Data

```python
# Read a subset of the columns into a dataframe
df_date = pd.read_parquet(output.data_file_name, columns=['drv_date_key', 'drv_date', 'drv_month_name', 'drv_month_number', 'drv_quarter_name', 'drv_quarter_number', 'drv_year'])

# Check the contents of the dataframe
df_date
```

## Making Authenticated API Requests

### Get The Data

```python
import opendatablend as odb
import pandas as pd

dataset_path = 'https://packages.opendatablend.io/v1/open-data-blend-road-safety/datapackage.json'
access_key = '<ACCESS_KEY>'

# Specify the resource name of the data file. In this example, the 'date' data file will be requested in .parquet format.
resource_name = 'date-parquet'

# Get the data and store the output object
output = odb.get_data(dataset_path, resource_name, access_key=access_key)

# Print the file locations
print(output.data_file_name)
print(output.metadata_file_name)
```

### Use The Data

```python
# Read a subset of the columns into a dataframe
df_date = pd.read_parquet(output.data_file_name, columns=['drv_date_key', 'drv_date', 'drv_month_name', 'drv_month_number', 'drv_quarter_name', 'drv_quarter_number', 'drv_year'])

# Check the contents of the dataframe
df_date
```

## Ingesting Data Directly into Cloud Storage Services

### Azure Blob Storage

```python
import opendatablend as odb

dataset_path = 'https://packages.opendatablend.io/v1/open-data-blend-road-safety/datapackage.json'
access_key = '<ACCESS_KEY>' # The access key can be set to an empty string if you are making a public API request

# Specify the resource name of the data file. In this example, the 'date' data file will be requested in .parquet format.
resource_name = 'date-parquet'

# Get the data and store the output object using the Azure Blob Storage file system
configuration = {
    "connection_string" : "DefaultEndpointsProtocol=https;AccountName=<AZURE_BLOB_STORAGE_ACCOUNT_NAME>;AccountKey=<AZURE_BLOB_STORAGE_ACCOUNT_KEY>;EndpointSuffix=core.windows.net",
    "container_name" : "<AZURE_BLOB_STORAGE_CONTAINER_NAME>" # e.g. odbp-integration
}
output = odb.get_data(dataset_path, resource_name, access_key=access_key, file_system="azure_blob_storage", configuration=configuration)

# Print the file locations
print(output.data_file_name)
print(output.metadata_file_name)
```

### Azure Data Lake Storage (ADLS) Gen2

```python
import opendatablend as odb

dataset_path = 'https://packages.opendatablend.io/v1/open-data-blend-road-safety/datapackage.json'
access_key = '<ACCESS_KEY>' # The access key can be set to an empty string if you are making a public API request

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
```

### Amazon S3

```python
import opendatablend as odb

dataset_path = 'https://packages.opendatablend.io/v1/open-data-blend-road-safety/datapackage.json'
access_key = '<ACCESS_KEY>' # The access key can be set to an empty string if you are making a public API request

# Specify the resource name of the data file. In this example, the 'date' data file will be requested in .parquet format.
resource_name = 'date-parquet'

# Get the data and store the output object using the Amazon S3 file system
configuration = {
    "aws_access_key_id" : "<AWS_ACCESS_KEY_ID>",
    "aws_secret_access_key" : "AWS_SECRET_ACCESS_KEY",
    "bucket_name" : "<BUCKET_NAME>", # e.g. odbp-integration
    "bucket_region" : "<BUCKET_REGION>" # e.g. eu-west-2
}

output = odb.get_data(dataset_path, resource_name, access_key=access_key, file_system="amazon_s3", configuration=configuration)

# Print the file locations
print(output.data_file_name)
print(output.metadata_file_name)
```

### Google Cloud Storage

Coming soon.

## Additional Examples

For more in-depth examples, see the [examples](https://github.com/opendatablend/opendatablend-py/tree/master/examples) folder.
