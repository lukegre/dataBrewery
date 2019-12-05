dataBrewery
===========

Downloads datasets for climate science that are date based. Also includes some tools to process the data to a standard format (time, lat, lon) and resolution. 

The principle is that you use the `config.yaml` file to define the location and lgoin details of the data you'd like to download. The local path must also be defined. The downloading function will determine if the file is readable by a netCDF reader or an nuzip function. You can then define any date for the default file name and it will download that path to the specified location with your login details. The downloading functions are relatively stable and will not change much. 

The Brewery function allows you to also define the path of the data, which you can use to access the path of your locally downloaded data. This is for quick easy access to centrally downloaded data for each of your projects. 

More documentation will follow. 

