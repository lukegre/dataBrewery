from config_utils import load_config

def main():
    import pandas as pd
    date_range = pd.date_range(config['era5']['start'],
                               config['era5']['end'],
                               freq="1D")

    for func_name in config['era5']['functions']:
        func = globals()[func_name]
        func(date_range)


def download(date_range):
    from utils import download_utils as du
    from netCDF4 import Dataset as ncDataset
    import os
    import cdsapi

    config = load_config()
    c = cdsapi.Client()

    du.check_pandas_datetime_index(date_range)

    local_name = os.path.join(config['path']['raw'], 'era5/{year}/era5_{year}{month:02d}_u10_slp.nc')

    month_range = du.date_range_to_custom_freq(date_range)
    for t in month_range:
        local_path = local_name.format(year=t.year, month=t.month)

        request_ERA5_U10_SLP(cds_client, t.year, t.month, local_path)


def preprocessor_era5_025d(xds):
    from utils.gridding_utils import center_coords_gmt
    from numpy import arange

    xds['wind_speed'] = (xds.u10**2 + xds.v10**2)**0.5

    xds = xds.rename({'latitude': 'lat', 'longitude': 'lon'})
    xds = center_coords_gmt(xds, verbose=False)
    xds = (xds
           .interp(lon=arange(-179.875, 180, 0.25),
                   lat=arange(-89.875, 90, 0.25))
           .roll(lon=720, roll_coords=False)
           .interpolate_na(dim='lon', limit=10)
           .roll(lon=-720, roll_coords=False)
           .resample(time='1D', keep_attrs=True)
           .mean('time', keep_attrs=True))

    return xds


def standardise_025_dialy_stored_monthly(date_range):
    from utils import netcdf_utils as nu
    from utils import gridding_utils as gu
    from utils import xarray_tools
    import os

    nu.daily_netcdf_stored_monthly(
        date_range,
        os.path.join(config['path']['raw'],  "era5/{year:04d}/era5_{year:04d}{month:02d}_u10_slp.nc"),
        os.path.join(config['path']['grid'], "daily_025/era5_{{0}}/{year}/era5_{{0}}_{year:04d}_{month:02d}.nc"),
        config['era5']['variables'],
        preprocessor_era5_025d)


if __name__ == '__main__':
    main()
