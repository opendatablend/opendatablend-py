![alt text](https://raw.githubusercontent.com/opendatablend/opendatablend-py/master/images/odblogo.png "Open Data Blend")

# Open Data Blend for Python

Open Data Blend for Python is the fastest way to get data from the Open Data Blend Dataset API. It is a lightweight, easy-to-use extract and load (EL) tool.

It has a single function called `get_data` that can be used to get any data file belonging to an Open Data Blend dataset. The function transparently downloads and caches the data locally, mirroring the same folder hierarchy as on the remote server. It also caches a copy of the dataset metadata file (datapackage.json) at the point that the data file request is made. The local cache is persistent which means the files will be kept until they are deleted.

The versioned dataset metadata can be used to re-download a specific version of a data file (sometimes referred to as 'time travel'). You can learn more about how we version our datasets in the [Open Data Blend Docs](https://docs.opendatablend.io/open-data-blend-datasets/dataset-snapshots).

After downloading the data and metadata files, `get_data` returns an object called `Output` containing the local paths of the files. From there, you can load the data in [Pandas](https://pandas.pydata.org/), [Koalas](https://github.com/databricks/koalas), or something similar to begin your analysis or feature engineering.

# Installation

Install the latest version of `opendatablend` from [PyPI](https://pypi.org/):

```Python
pip install opendatablend
```

# Usage Examples

The following examples require the `pandas` and `pyarrow` packages to be installed:

```Python
pip install pandas
pip install pyarrow
```

## Making Public API Requests

Note: Public API requests are [limited per month](https://docs.opendatablend.io/open-data-blend-datasets/dataset-api#usage-limits).

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
access_key = '<ACCESS_KEY_HERE>'

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

## Additional Examples

For more in-depth examples, see the [examples](https://github.com/opendatablend/opendatablend-py/tree/master/examples) folder.
