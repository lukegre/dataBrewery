
def center_coords_gmt(xda, verbose=True, lon_name='lon', lat_name='lat'):
    def strictly_increasing(L):
        return all([x<y for x, y in zip(L, L[1:])])

    lat = lat_name
    lon = lon_name

    x = xda[lon].values
    y = xda[lat].values

    x[x >= 180] -= 360
    if not strictly_increasing(x):
        if verbose: print('flip lons', end=', ')
        sort_idx = np.argsort(x)
        xda = xda.isel(**{lon: sort_idx})
        xda[lon].values = x[sort_idx]

    if not strictly_increasing(y):
        if verbose: print('flip lats', end=', ')
        xda = xda.isel(**{lat: slice(None, None, -1)})

    return xda