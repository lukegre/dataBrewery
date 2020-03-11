
def _raw_to_daily_025d(xds):
    import re
    from ..utils.preprocessing_tools import center_coords_gmt
    # check correct_file
    fname = xds.encoding['source']  # MXLDEPTH_1994_01
    if re.findall('CCMP_Wind_Analysis_', fname):
        pass
    else:
        raise UserWarning('Not the correct input file for CCMPv2 processing')
    
    xds = xds.rename({'longitude': 'lon', 'latitude': 'lat'})
    xds = xds.drop('nobs')

    xds['wind_speed'] = (xds.uwnd**2 + xds.vwnd**2)**0.5

    xds = xds.resample(time='1D').mean()
    xds = center_coords_gmt(xds, False)
    xds = xds.load()
    return xds
    
    
preprocessors = {
    "raw_to_daily_025d": _raw_to_daily_025d,
}