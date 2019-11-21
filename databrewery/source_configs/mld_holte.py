from config_utils import load_config
config = load_config()


def main():
    import pandas as pd
    date_range = pd.date_range(config['mld_holte']['start'], 
                               config['mld_holte']['end'], 
                               freq="1D")
    
    for func_name in config['mld_holte']['functions']:
        func = globals()[func_name]
        func(date_range)

def download(*args):
    from utils import download_utils as dl
    from pandas import date_range as pd_date_range
    import os 

    remote_path   = "http://mixedlayer.ucsd.edu/data/Argo_mixedlayers_monthlyclim_05092018.nc"
    local_path = os.path.join(config['path']['raw'],  "mld_holte/Argo_mixedlayers_monthlyclim_05092018.nc")

    dl.download_wget(remote_path, local_path, verbose=True)

    
def preprocessor_mld_holte_025d(xds, date=None):
    from utils import xarray_tools as xt
    import astropy.convolution as conv
    from pandas import DatetimeIndex, date_range
    from numpy import arange
    coords = {'iMONTH': 'month', 'iLAT': 'lat', 'iLON': 'lon'}
    keys = ['mld_da_median', 'mld_da_max']
    if date is not None:
        xds = xds.isel(iMONTH=[date.month])

    xds_1mc = (xds
           .rename(coords)
           .set_coords(coords.values())
           .transpose(*coords.values())
           .interpolate_na(dim='lon', limit=1)
           .interpolate_na(dim='lat', limit=1)
           [keys].load())
    
    for key in keys:
        k = conv.kernels.Gaussian2DKernel(2.5)
        xds_1mc[key] = xds_1mc[key].convolve.spatial(kernel=k)
    
    xds_025mc = (xds_1mc
           .interp(lat=arange(-89.875, 90, .25), 
                   lon=arange(-179.875, 180, .25), 
                   method='linear')
           .roll(lon=720, roll_coords=False)
           .interpolate_na(dim='lon', limit=10)
           .roll(lon=-720, roll_coords=False))
    
    xds = xds_025mc
    
    if date is not None:
        t1 = f"{date.year}-{date.month}-01"
        t2 = f"{date.year}-{date.month}-{date.days_in_month}"
        xds_025mc['month'] = DatetimeIndex([t1])
        xds_025mc = xds_025mc.rename({'month': 'time'})

        xds_025d = xds_025mc.reindex(time=date_range(t1, t2, freq='1D'), method='nearest')
        xds = xds_025d

    return xds 

    
def standardise_025_dialy_stored_monthly(date_range):
    from utils.netcdf_utils import dataset_to_netcdf_per_var
    from utils.download_utils import date_range_to_custom_freq, is_file_valid
    from netCDF4 import Dataset as ncDataset
    from xarray import open_mfdataset 
    from numpy import sort
    from glob import glob
    import os
    
    input_str = os.path.join(config['path']['raw'],  "mld_holte/Argo_mixedlayers_monthlyclim_05092018.nc")
    output_str = os.path.join(config['path']['grid'],"daily_025/mld_holte_{{0}}/{year}/mld_holte_{{0}}_{year:04d}_{month:02d}.nc")
    keys = config['mld_holte']['variables']
        
    months = date_range_to_custom_freq(date_range)
    for t in months:
        kwargs = dict(year=t.year, month=t.month, day=t.day, dayofyear=t.dayofyear, hour=t.hour)
        files = sort(glob(input_str.format(**kwargs)))
        output_path = output_str.format(**kwargs)

        # this is a speedup so that data isn't loaded before checking if files exist
        # if keys are not provided, this part will be skipped and loop will be slow
        # if files already exist 
        keys_month = set(keys)
        for key in keys:
            fname = output_path.format(key)
            if is_file_valid(fname, ncDataset):
                print(f"File exists: {fname}")
                keys_month -= set([key])  # remove key from monthly keys
        # if the monthly keys is an empty array, then all files exist
        if len(keys_month) == 0:
            continue 
        keys_month = list(keys_month)
        
        xds = open_mfdataset(
            files, 
            concat_dim='time', 
            combine='nested', 
            preprocess=lambda xds: preprocessor_mld_holte_025d(xds, t)
        )[keys_month]

        dataset_to_netcdf_per_var(xds, output_path)
    
    
if __name__ == "__main__":
    main()
