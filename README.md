dataBrewery
===========

Downloads datasets for climate science that are date based. Also includes some tools to process the data to a standard format (time, lat, lon) and resolution. 

The idea is that you use the `config.yaml` file to define the location and lgoin details of the data you'd like to download. The local path must also be defined. You also need to specify the date range that you'd like to download. This can also be done later, but it is much better to do this in the configuragion.

The downloading function will determine if the file is readable by a netCDF reader or an nuzip function. You can then define any date for the default file name and it will download that path to the specified location with your login details. The downloading functions are relatively stable and will not change much. 

The Brewery class allows you to access the local path of the data, regardless of where your files are stored. This is for quick easy access to centrally downloaded data for each of your projects. 

Se the demo file for basic usage. 

More documentation will follow. 

