![alt text](images/odblogo.png "Open Data Blend")

# Open Data Blend Module for Python

`opendatablend` is a lightweight Python module for use with the [Open Data Blend Datasets](https://www.opendatablend.io/datasets) service and, more specifically, the Open Data Blend Dataset API.

The `get_data` function  downloads and caches the requested data files locally. It will only download a data file if the requested version does not already exist in the local cache. It also saves a copy of the dataset metadata (datapackage.json) for future use (e.g. to take advantage of the [Dataset Versions](https://docs.opendatablend.io/open-data-blend-datasets/dataset-snapshots) feature.

After downloading the data and metadata files, it returns an object called `Output` with the locations of these files. The files can then be loaded with your favourite Python libraries.

# Installation

Place a copy of the [opendatablend.py](./opendatablend/opendatablend.py) file in the same location that you are running your code from.

# Required Packages

The opendatablend module required the `frictionless` and `requests` packages to be installed.

```Python
pip install frictionless
pip install requests
```

# Usage Examples

In addition to the required packages mentioned above, the following examples require the `pandas` and `pyarrow` packages to be installed.

```Python
pip install pandas
pip install pyarrow
```

## Making Public API Requests

Note: Public API requests are [limited per month](https://docs.opendatablend.io/open-data-blend-datasets/dataset-api#usage-limits).

```python
from opendatablend import get_data as gd
import pandas as pd

dataset_path = 'https://packages.opendatablend.io/v1/open-data-blend-road-safety/datapackage.json'

# Specify the resource name of the data file. In this example, the date data file will be requested in .parquet format.
resoure_name = 'date-parquet'

# Get the data and store the output object
output = gd(dataset_path, resoure_name)

# Print the file locations
print(output.data_file_name)
print(output.metadata_file_name)

# Read a subset of the columns into a dataframe
df_date = pd.read_parquet(output.data_file_name, columns=['drv_date_key', 'drv_date', 'drv_month_name', 'drv_month_number', 'drv_quarter_name', 'drv_quarter_number', 'drv_year'])

# Check the contents of the dataframe
df_date
```

## Making Authenticated API Requests
```python
from opendatablend import get_data as gd
import pandas as pd

dataset_path = 'https://packages.opendatablend.io/v1/open-data-blend-road-safety/datapackage.json'
access_key = '<ACCESS_KEY_HERE>'

# Specify the resource name of the data file. In this example, the date data file will be requested in .parquet format.
resoure_name = 'date-parquet'

# Get the data and store the output object
output = gd(dataset_path, resoure_name,access_key=access_key)

# Print the file locations
print(output.data_file_name)
print(output.metadata_file_name)

# Read a subset of the columns into a dataframe
df_date = pd.read_parquet(output.data_file_name, columns=['drv_date_key', 'drv_date', 'drv_month_name', 'drv_month_number', 'drv_quarter_name', 'drv_quarter_number', 'drv_year'])

# Check the contents of the dataframe
df_date
```

## Additional Examples

For more in-depth examples, see the [examples](./examples) folder.
