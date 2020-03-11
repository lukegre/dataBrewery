
class preprocessors:
    @staticmethod
    def raw_to_daily_025(xds):
        from ..utils.preprocessing_tools import center_coords_gmt
        import re

        xds = xds.rename({'latitude': 'lat', 'longitude': 'lon'})
        xds = center_coords_gmt(xds, verbose=False)

        return xds
    