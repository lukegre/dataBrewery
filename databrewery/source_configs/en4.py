
def preprocessor_en4_025d(xds):
    from numpy import arange
    from pandas import date_range
    from calendar import monthlen
    from utils.gridding_utils import center_coords_gmt

    # creating time array
    year  = xds.time.to_index().year.unique().values[0]
    month = xds.time.to_index().month.unique().values[0]
    ndays = monthlen(year, month)
    time = date_range(f'{year}-{month}-01', f'{year}-{month}-{ndays}')

    xds = xds.sel(depth=0, method='nearest').drop('depth')
    xds = center_coords_gmt(xds, verbose=False)

    # interpolating data to quarter degree
    xds = (xds
           .interp(lat=arange(-89.875, 90, .25), 
                   lon=arange(-179.875, 180, .25), 
                   method='linear')
           .roll(lon=720, roll_coords=False)
           .interpolate_na(dim='lon', limit=10)
           .roll(lon=-720, roll_coords=False)
           .reindex(time=time, method='nearest'))
    
    return xds

