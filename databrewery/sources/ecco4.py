from config_utils import load_config
config = load_config()


def main():
    import pandas as pd
    date_range = pd.date_range(config['ecco4']['start'], 
                               config['ecco4']['end'], 
                               freq="1D")
    
    for func_name in config['ecco4']['functions']:
        func = globals()[func_name]
        func(date_range)

        
def download(date_range):
    from netCDF4 import Dataset as ncDataset
    from utils import download_utils as du
    import pandas as pd
    import os

    du.check_pandas_datetime_index(date_range)

    remote_name = "https://ecco.jpl.nasa.gov/drive/files/Version4/Release4/interp_monthly/MXLDEPTH/{year:04d}/MXLDEPTH_{year:04d}_{month:02d}.nc"
    local_name = os.path.join(config['path']['raw'], "ecco4/{year:04d}/MXLDEPTH_{year:04d}_{month:02d}.nc")

    date_range_monthly = pd.date_range(date_range[0], date_range[-1], freq='1M')

    for t in date_range_monthly:
        remote_path = remote_name.format(year=t.year, month=t.month)
        local_path = local_name.format(year=t.year, month=t.month)

        du.download_wget(remote_path, local_path, 
                         local_file_checker=ncDataset, 
                         user=config['ecco4']['username'], 
                         password=config['ecco4']['password'])

        
def preprocessor_ecco4_mxldepth_mon2day_05to025(xds):
    import re
    from numpy import arange
    from xarray import DataArray
    from pandas import date_range
    from calendar import monthlen
    
    # check correct_file
    fname = xds.encoding['source']  # MXLDEPTH_1994_01
    if re.findall('MXLDEPTH_[12][09][0-9][0-9]_[01][0-9]', fname):
        pass
    else:
        raise UserWarning('Not the correct input file for ECCO4 MXLDEPTH processing')
    
    xda = xds['MXLDEPTH']
    
    # fixing the i x j indexing to lat and lons
    xda_050m = DataArray(
        xda.values,
        dims=['time', 'lat', 'lon'],
        coords=dict(time=xda.time.values, 
                    lat=xda.latitude.values,
                    lon=xda.longitude.values))
    
    # creating time array
    year  = xda_050m.time.to_index().year.unique().values[0]
    month = xda_050m.time.to_index().month.unique().values[0]
    ndays = monthlen(year, month)
    time = date_range(f'{year}-{month}-01', f'{year}-{month}-{ndays}')
    
    # interpolating data to quarter degree
    xda_025d = (xda_050m
                .interp(lat=arange(-89.875, 90, .25), 
                        lon=arange(-179.875, 180, .25), 
                        method='linear')
                # filling gaps due to interpolation along 180deg 
                .roll(lon=720, roll_coords=False)  
                .interpolate_na(dim='lon', limit=10)
                .roll(lon=-720, roll_coords=False)
                .reindex(time=time, method='nearest'))

    return xda_025d.to_dataset(name='mxldepth')

    
def standardise_025_dialy_stored_monthly(date_range):
    from utils.netcdf_utils import daily_netcdf_stored_monthly 
    import os

    daily_netcdf_stored_monthly(
        date_range,
        os.path.join(config['path']['raw'],  "ecco4/{year:04d}/MXLDEPTH_{year:04d}_{month:02d}.nc"),
        os.path.join(config['path']['grid'], "daily_025/ecco4_{{0}}/{year}/ecco4_{{0}}_{year:04d}_{month:02d}.nc"),
        config['ecco4']['variables'],
        preprocessor_ecco4_mxldepth_mon2day_05to025)


if __name__ == "__main__":
    main()
