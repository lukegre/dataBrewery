dataBrewery
===========
Downloads datasets for climate science that are date based. Also includes some tools to process the data to a standard format (time, lat, lon) and resolution.

**Still in development**

Installation
------------
```bash
python setup.py install
```

Basic usage
-----------
You first need to set up a `config (yaml)` file to define the location (URL) and login details of the data you'd like to download.
The local storage path must also be defined in the config.
This can be a centrally stored location for other people in your group (or projects) to access the data.
The URL and local storage path can contain date formatting.
There is a config template file with a description for each of the entries

During runtime, you need to specify the date range that you'd like to download.

More documentation to follow

Currently, set up to be used interactively. CLI will probably be developed at some point
```python
import databrewery as db

data_catalog = db.Catalog('<path_to_catalog_file.yaml>', verbose=2)
print(data_catalog)

date_range = slice('2001-10-01', '2005-09-01')
# will ask to download the files if not present in local_store
data_catalog.lsce_ffnnv1.local_files(date_range)
# files will then download after confirmation

```
