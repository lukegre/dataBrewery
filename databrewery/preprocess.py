
class DataProcressor:
    def __init__(self, keg, processor, space_res, time_res, verbose=1):
        
        resolution = dict(space=space_res, time=time_res)
        self.resolution = db.core.ObjectDict(resolution)
        
        self.path = keg.path
        self.date = keg.date
        self.name = keg.name
        
        self.processor_proven = False
        self.processor = processor
        self._previous_savename = ''

    def get_existing_and_missing_raw_files(self, date_range):
        from os.path import isfile
        expected_files = set(self.path.raw[date_range])
        
        existing_files = set([f for f in expected_files if isfile(f)])
        missing_files = sorted(expected_files - existing_files)
        existing_files = sorted(existing_files)
        
        if not existing_files:
            self.qprint(f"No files for {self.name} from {ds[0]} to {ds[-1]}")
            
        return existing_files, missing_files

    def _process_date_slice(self, dates, file_concat_freq='1M'): 
        
        def slice_dates_by_concat_freq(date_range, concat_freq):
            import re
            from pandas import Timestamp
            concat_range = pd.date_range(date_range[0], 
                                         date_range[-1], 
                                         freq=concat_freq)

            unit = re.sub('[^AMDh]', '', concat_freq).lower()
            i0 = date_range[0] - pd.Timedelta(1, unit=unit)
            
            date_slices = []
            for i1 in concat_range:
                i = (date_range > i0) & (date_range <= i1)
                date_slices += date_range[i],
                i0 = date_slices[-1][-1]
            
            return date_slices
        
        def make_savename_with_placeholder(date):
            fname = self.path.grid.format(
                res_time=self.resolution.time,
                res_space=str(self.resolution.space).replace('.',''),
                variable="{0}",
                t=date)
            if fname == self._previous_savename:
                raise FileExistsError(f"You seem to be providing the same target for saving the "
                                      f"gridded netCDF file for the previous set of dates. Ensure "
                                      f"that your file concatenation frequency matches the file "
                                      f"naming procedure. ")
                
            self._previous_savename = fname
            return fname
        
        def check_varfiles_exist_and_return_missing_vars(filename):
            xds = self.processor([filename])
            
            variable_list_all = list(xds.data_vars.keys())
            
            variable_files_missing = set(variable_list_all)
            
            for var in variable_list_all:
                sname = savename.format(var)
                if is_local_file_valid(sname):
                    self.qprint(f"File exists {sname}")
                    variable_files_missing -= set([var])
            
            return list(variable_files_missing)
        
        def test_processor_output_space_res(filename):
            if not self.processor_proven:
                xds = self.processor([filename])
                latdiff = np.diff(xds.lat.values).mean()
                assert latdiff == self.resolution.space
                self.processor_proven = True
        
        def dataset_variables_to_individual_netcdfs(xds, name_with_placeholder):
            
            for var in variable_list:
                save_name = name_with_placeholder.format(var)
                sdir = os.path.split(save_name)[0]
                self.qprint(f'Saving file to {save_name}')

                os.makedirs(sdir, mode=511, exist_ok=True)
                
                xda = xds[var]
                xda.attrs.update(xds.attrs)
                xda.to_netcdf(save_name, encoding={var: {'zlib': True, 'complevel': 4}})
        
        dates = self.path.raw._try_convert_to_date_format(dates)
        
        t0, t1 = dates.start, dates.stop
        ts = self.path.raw._check_freq_string(dates.step)
        date_range = pd.date_range(t0, t1, freq=ts)
        
        date_slices = slice_dates_by_concat_freq(date_range, file_concat_freq)
                
        for ds in date_slices:
            files, missing_files = self.get_existing_and_missing_raw_files(ds)
            
            if not files: continue
                
            savename = make_savename_with_placeholder(ds[0])
            variable_list = check_varfiles_exist_and_return_missing_vars(files[0])
            test_processor_output_space_res(files[0])
            
            if not variable_list: continue

            print(f'Loading {self.name} for {ds[0]} to {ds[-1]}')
            xds = self.processor(files)
            xds = xds[variable_list].load()
            
            if missing_files:
                xds.attrs['missing_files'] = str(missing_files)
            
            dataset_variables_to_individual_netcdfs(xds, savename)

    def qprint(self, *msg):
        if self.verbose == 1:
            print(*msg)

    def vprint(self, *msg):
        if self.verbose == 2:
            print(*msg)


class NetCDFpreprocessorWrapper:
    from warnings import filterwarnings
    filterwarnings('ignore', ".*reduce.*", RuntimeWarning)
    
    def __init__(self, netcdf_preprocessor):
        self.preprocessor = netcdf_preprocessor
    
    def __call__(self, files, attr_kwargs={}):
        import xarray as xr
            
        xds = xr.open_mfdataset(files, 
                                concat_dim='time', 
                                combine='nested', 
                                parallel=True, 
                                preprocess=self.preprocessor)
        xds.attrs.update(attr_kwargs)
        return xds 


class ZipPreprocessorWrapper:
    def __init__(self, secondary_preprocessor):
        self.processor = secondary_preprocessor
        
    def __call__(self, zipname, unzip_dest=None):
        import xarray as xr
        files = unzip(zipname, unzip_dest)
        xds = xr.open_mfdataset(files, 
                                concat_dim='time', 
                                combine='nested', 
                                parallel=True, 
                                preprocess=self.processor)
        return xds


def is_local_file_valid(local_path):
    from os.path import isfile
    if not isfile(local_path):
        return False

    # has an opener been passed, if not assumes file is valid
    if local_path.endswith('.nc'):
        from netCDF4 import Dataset as opener
        error = OSError
    elif local_path.endswith('.zip'):
        from zipfile import ZipFile as opener, BadZipFile as error
    else:
        error = BaseException
        opener = lambda p: None  # dummy opener

    # tries to open the path, if it fails, not valid, if it passes, valid
    try:
        with opener(local_path) as obj:
            return True
    except error:
        return False


def rename_to_latlon(xds):
    
    time = ['mtime']
    lat = ['latitude', 'lats', 'yt_ocean']
    lon = ['longitude', 'lons', 'xt_ocean']
    
    rename_dict = {}
    for key in xds.coords.keys():
        if key in time: 
            rename_dict[key] = 'time'
        if key in lat: 
            rename_dict[key] = 'lat'
        if key in lon: 
            rename_dict[key] = 'lon'
    
    xds = xds.rename(rename_dict)
    if rename_dict:
        xds = _netcdf_add_brew_hist(xds, 'renamed time lats and lons')

    return xds


def center_coords_at_0(xds):
    import numpy as np
    
    def strictly_increasing(L):
        return all([x<y for x, y in zip(L, L[1:])])

    x = xds['lon'].values
    y = xds['lat'].values

    x[x >= 180] -= 360
    if not strictly_increasing(x):
        sort_idx = np.argsort(x)
        xds = xds.isel(**{'lon': sort_idx})
        xds['lon'].values = x[sort_idx]
        xds = _netcdf_add_brew_hist(xds, 'centered coords from 0:360 to -180:180')

    if not strictly_increasing(y):
        xds = xds.isel(**{'lat': slice(None, None, -1)})
        xds = _netcdf_add_brew_hist(xds, 'flipped lats to -90:90')
        

    return xds


def center_time_monthly_15th(xds):
    from pandas import Timestamp
    assert xds.time.size == 1, 'Only accepts one month DataArrays'
    
    attrs = xds.attrs
    time = Timestamp(xds.time.values[0])
    
    xds.time.values[:] = Timestamp(year=time.year, month=time.month, day=15)
    
    xds.attrs = attrs
    xds = _netcdf_add_brew_hist(xds, 'centered monthly data to 15th')
    return xds


def shallowest(xda):
    for dim in xda.dims:
        depth_dim = None
        if hasattr(xda[dim], 'units'):
            units = xda[dim].units
            if 'meters' in units:
                depth_dim = dim
                break
    if depth_dim is None:
        return xda
    
    xda = xda.sel(**{depth_dim: 0}, method='nearest').drop(depth_dim)
    xda = _netcdf_add_brew_hist(xda, 'Shallowest depth selected')
    return xda


def downsample_025(xda):
    import xarray as xr
    if isinstance(xda, xr.DataArray):
        xda = _netcdf_add_brew_hist(xda, 'downsampled lat,lon to 0.25deg')
        return xda.downscale(lat_step=0.25, lon_step=0.25) - 273.15
    if isinstance(xda, xr.Dataset):
        return xr.merge([downscale(xda[key]) for key in xda.data_vars])


def interpolate_025(xds, method='linear'):
    from numpy import arange
    attrs = xds.attrs
    xds = (xds
            .interp(lat=arange(-89.875, 90, .25), 
                    lon=arange(-179.875, 180, .25), 
                    method=method)
            # filling gaps due to interpolation along 180deg 
            .roll(lon=720, roll_coords=False)  
            .interpolate_na(dim='lon', limit=10)
            .roll(lon=-720, roll_coords=False))
    
    xds.attrs = attrs
    xds = _netcdf_add_brew_hist(xds, 'interpolated to 0.25deg')
    
    return xds


def interpolate_1deg(xds, method='linear'):
    from numpy import arange
    attrs = xds.attrs
    xds = (xds
            .interp(lat=arange(-89.5, 90), 
                    lon=arange(-179.5, 180), 
                    method=method)
            # filling gaps due to interpolation along 180deg 
            .roll(lon=180, roll_coords=False)  
            .interpolate_na(dim='lon', limit=3)
            .roll(lon=-180, roll_coords=False))
    
    xds.attrs = attrs
    xds = _netcdf_add_brew_hist(xds, 'interpolated to 1deg')
    
    return xds


def resample_time_1D(xds):
    attrs = xds.attrs
    
    xds = (xds
           .resample(time='1D', keep_attrs=True)
           .mean('time', keep_attrs=True))
    
    xds.attrs.update(attrs)
    xds = _netcdf_add_brew_hist(xds, 'resampled to time to 1D')
    
    return xds


def resample_time_1M(xds):
    import pandas as pd
    attrs = xds.attrs
    
    xds = (xds
           .resample(time='1MS', keep_attrs=True)
           .mean('time', keep_attrs=True))
    xds.time.values += pd.Timedelta('14D')
    
    xds.attrs.update(attrs)
    xds = _netcdf_add_brew_hist(xds, 'resampled to time to 1M')
    
    return xds


def fill_time_monthly_to_daily(xds):
    from calendar import monthlen
    import pandas as pd
    import re
    
    time = xds.time.to_index()
    year_0 = time.year.unique().values[0]
    mon_0 = time.month.unique().values[0]
    
    year_1 = year_0 if mon_0 < 12 else year_0 + 1
    mon_1 = 1 if mon_0 == 12 else mon_0 + 1
    
    t0 = pd.Timestamp(f'{year_0}-{mon_0}-01')
    t1 = pd.Timestamp(f'{year_1}-{mon_1}-01')
    
    date_range = pd.date_range(start=t0, end=t1, freq='1D', closed='left')
    xds = xds.reindex(time=date_range, method='nearest')
    
    xds = _netcdf_add_brew_hist(xds, 'time filled from monthly to daily')
    
    return xds
    
    
def unzip(zip_path, dest_dir=None, verbose=1):
    """returns a list of unzipped file names"""
    import os
    from zipfile import ZipFile
    
    def get_destination_directory(zipped):
        file_name = zipped.filename
        file_list = zipped.namelist()
        if len(file_list) == 1:
            destdir = os.path.split(file_name)[0]
        else:
            destdir = os.path.splitext(file_name)[0]
        
        return destdir
    
    def get_list_of_zipped_files(zipped, dest_dir):
        flist_zip = set(zipped.namelist())
        flist_dir = set(os.listdir(dest_dir))
        
        for file in flist_dir:
            if not is_local_file_valid(file):
                flist_dir -= set(file)
                
        files_to_extract = list(flist_zip - flist_dir)
        
        if not files_to_extract:
            if verbose: print(f'All files extracted: {zipped.filename}')
        return files_to_extract
    
    if not os.path.isfile(zip_path):
        raise OSError(f'The zip file does not exist: {zip_path}')
    
    zipped = ZipFile(zip_path, 'r')
    if dest_dir is None:
        dest_dir = get_destination_directory(zipped)
        os.makedirs(dest_dir, exist_ok=True)
    
    files_to_extract = get_list_of_zipped_files(zipped, dest_dir)
    for file in files_to_extract:
        shortpath = shorten_path_for_print(os.path.join(dest_dir, file), 80)
        if verbose: print(f" Extracting: {shortpath}")
        zipped.extractall(path=dest_dir, members=[file])
        
    return [os.path.join(dest_dir, f) for f in zipped.namelist()]


def gunzip(zip_path, dest_path=None):
    import shutil
    import gzip

    if dest_path is None:
        dest_path = zip_path.replace('.gz', '')

    with gzip.open(zip_path, 'rb') as f_in:
        with open(dest_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
            return f_out
        
        
def untar(tar_path, dest_dir=None, verbose=1):
    """returns a list of untarred file names"""
    import os
    import pathlib
    import tarfile
    
    if not os.path.isfile(tar_path):
        raise OSError(f'The tar file does not exist: {tar_path}')
    
    if tar_path.endswith('gz'):
        mode = 'r:gz'
    else:
        mode = 'r:'
    tarred = tarfile.open(tar_path, mode)
    
    if dest_dir is None:
        dest_dir = pathlib.Path(tar_path).parent
    else:
        os.makedirs(dest_dir, exist_ok=True)
    
    tarred.extractall(path=dest_dir)
        
    return [os.path.join(dest_dir, f) for f in tarred.getnames()]

        
def shorten_path_for_print(path, maxlen=100):
    if len(path) <= maxlen:
        return path

    from urllib.parse import urlparse
    url = urlparse(path)
    out = ''

    out += url.scheme + '://' if url.scheme is not '' else ''
    out += url.netloc

    out += '/'.join(url.path[:15].split('/')[:-1])
    out += '/.../'
    length_remaining = maxlen - len(out)
    end_of_path_ugly = url.path[-length_remaining:]
    out += '/'.join(end_of_path_ugly.split('/')[1:])

    return out


def _netcdf_add_brew_hist(xds, msg, key='history'):
    from pandas import Timestamp
    
    now = Timestamp.today().strftime('%Y-%m-%dT%H:%M')
    prefix = f"\n[DataBrewery@{now}] "
    msg = prefix + msg
    if key not in xds.attrs:
        xds.attrs[key] = msg
    elif xds.attrs[key] == '':
        xds.attrs[key] = msg
    else:
        xds.attrs[key] += '; ' + msg
        
    return xds


def apply_process_pipeline(xds, pipe):
    attrs = xds.attrs 
    for func in pipe:
        xds = func(xds)
    
    return xds


