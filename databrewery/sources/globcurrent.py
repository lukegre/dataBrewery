
def preprocessor_globcurrent_025d(xds):
    from utils.gridding_utils import center_coords_gmt
    from numpy import datetime64
    import re
    
    xds = xds.rename({"eastward_eulerian_current_velocity": 'u15vel',
                      "northward_eulerian_current_velocity": 'v15vel'})
    xds['current_speed'] = (xds.u15vel**2 + xds.v15vel**2)**0.5
    
    return xds

