
class preprocessors:
    @staticmethod
    def raw_to_daily_025(xds):
        from ..utils.preprocessing_tools import center_coords_gmt
        from numpy import datetime64
        import re

        xds = center_coords_gmt(xds, verbose=False)

        pattern = "([12][90][0-9]{2}/[01][0-9]/[0-3][0-9])"
        date = re.findall(pattern, xds.encoding['source'])[0].replace('/', '-')
        xds['time'] = datetime64(date)
        xds = xds.expand_dims(dim='time')

        return xds
    