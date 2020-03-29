
def url_exists(uri: str) -> bool:
    import requests
    if uri.startswith('ftp'):
        return True

    try:
        with requests.get(uri, stream=True) as response:
            try:
                response.raise_for_status()
                return True
            except requests.exceptions.HTTPError:
                return False
    except requests.exceptions.ConnectionError:
        return False


def download_url(url, local_path, verbose=True, username=None, password=None):
    import urllib.request
    from requests.auth import HTTPBasicAuth
    import requests
    from tqdm import tqdm

    def vprint(message):
        if verbose:
            print(message)

    if is_file_valid(local_path):
        vprint(f'Local file exists {local_path}')
        return True

    auth = HTTPBasicAuth(username, password)
    req = requests.get(url, auth=auth, stream=True)
    if not req.ok:
        vprint(f"URL does not exist: {url}")
        return False

    local_dir = os.path.split(local_path)[0]
    if not os.path.isdir(local_dir):
        os.makedirs(local_dir)

    size = int(req.headers.get('content-length', 0))
    step = 5 * 2**10
    pbar = tqdm(desc='Downloading ' + url.split('/')[-1], total=size, unit='B', unit_scale=True)
    with open(local_path, 'wb') as f:
        for data in req.iter_content(step):
            pbar.update(len(data))
            f.write(data)
    pbar.close()
    return True


def download_ftp_file(url, local_path, user=None, password=None, local_file_checker=None, verbose=False):
    import ftplib
    from tqdm import tqdm

    def vprint(message, **kwargs):
        if verbose:
            print(message, **kwargs)

    local_dir, local_file = os.path.split(local_path)
    if not os.path.isdir(local_dir):
        os.makedirs(local_dir)

    url = url.split('//')[1]
    host = url.split('/')[0]
    remote_path = url.replace(host, '')
    remote_folder, remote_file = os.path.split(remote_path)

    with ftplib.FTP(host) as ftp_server:

        ftp_server.login(user, password)

        flist = ftp_server.nlst(remote_folder)
        if '*' in remote_path:
            remote_path_glob = flist_match_glob(flist, remote_path)
            if remote_path_glob is None:
                print(f"URL does not exist: ftp://{host}{remote_path}")
                return False
            else:
                remote_path = remote_path_glob
                remote_file = os.path.split(remote_path)[1]

        if remote_path not in flist:
            vprint(f"URL does not exist: ftp://{host}{remote_path}")
            return False

        with open(local_path, 'wb') as fd:
            ftp_server.sendcmd("TYPE i")  # avoids ASCII error
            total = ftp_server.size(remote_path)

            with tqdm(total=total, desc=f'Downloading {remote_file}', unit='B', unit_scale=True) as pbar:
                def cb(data):
                    l = len(data)
                    pbar.update(l)
                    fd.write(data)

                ftp_server.retrbinary('RETR {}'.format(remote_path), cb)
                return True


def flist_match_glob(flist, fname):
    from fnmatch import fnmatch
    matches = [f for f in flist if fnmatch(f, fname)]
    if len(matches) == 1:
        return matches[0]
    elif len(matches) > 1:
        raise UserWarning(f'* can only be used to match single files: {remote_path}')
    else:
        return None


def download_wget(remote_path, local_path, *wget_args, verbose=True, local_file_checker=None, **wget_kwargs):
    def vprint(message, **kwargs):
        if verbose:
            print(message, **kwargs)
    import os

    remote_dir, remote_file = os.path.split(remote_path)
    local_dir, local_file = os.path.split(local_path)

    if not dir_writable(local_dir):
        raise OSError(f'Cannot write to {local_dir}')

    if is_file_valid(local_path, local_file_checker):
        vprint(f"File exists: {local_path}")
        return local_path

    if not url_exists(remote_path):
        vprint(f"URL does not exist: {remote_path}")
        return False

    command = "wget --show-progress --timeout=5 --tries=2"
    for opt in wget_args:
        command += f" --{opt}"

    for opt, val in wget_kwargs.items():
        command += f' --{opt}={val}'

    if not os.path.isdir(local_dir):
        os.makedirs(local_dir)

    command += f" --output-document={local_path} "
    command += remote_path

    os.system(command)


def dir_writable(path):
    import os
    return os.access(path, os.W_OK)


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
        def opener(p): return None  # dummy opener

    # tries to open the path, if it fails, not valid, if it passes, valid
    try:
        with opener(local_path):
            return True
    except error:
        return False
class NetCDFloader:
    """
    A class that loads netCDF data for a given date, but will
    not load the data if already loaded.
    Takes a DataBrew.Path object and a set of functions can be applied
    to the loaded netCDF.
    """
    def __init__(self, db_name, prep_funcs=[], varlist=[],
                 decode_times=True, verbose=1):
        self.name = db_name
        self.data = None
        self.file = ''
        self.prep_funcs = prep_funcs
        self.varlist = varlist
        self.decode_times = decode_times
        self.verbose = verbose

    def get_data(self, time, raise_error=True):
        fname = self._get_nearest_fname(time)
        if fname is None:
            return DummyNetCDF()
        if fname != self.file:
            if self.verbose == 1:
                print('.', end='')
            elif self.verbose == 2:
                print(f'Loading: {fname}')
            self.file = fname
            self.data = self._load_netcdf(fname, time=time)
        return self.data

    def _get_nearest_fname(self, time, max_dist_time=5):
        fname = self.name.set_date(time)
        dt = 1

        if '*' in str(fname):
            from glob import glob
            flist = glob(str(fname))
            return flist

        while (not fname.exists()) & (dt <= max_dist_time):
            fname = self.name.set_date(time + pd.Timedelta(f'{dt}D'))
            if fname.exists():
                continue
            else:
                fname = self.name.set_date(time - pd.Timedelta(f'{dt}D'))
            dt += 1

        if not fname.exists():
            return None

        return fname

    def _load_netcdf(self, fname, time=None):
        from xarray import open_dataset
        if not isinstance(fname, (list, str)):
            fname = str(fname)

        xds = xr.open_mfdataset(fname,
                                decode_times=self.decode_times,
                                combine='nested',
                                concat_dim='time',
                                parallel=True)
        if self.varlist:
            var_key = self.varlist
        else:
            var_key = list(xds.data_vars.keys())
        xds = xds[var_key]

        if (time is not None) and ('time' in xds.dims):
            if isinstance(time, (int, float)):
                xds = xds.isel(time=time).drop('time')
            else:
                xds = xds.mean('time')

        if self.prep_funcs:
            xds = self._apply_process_pipeline(xds, self.prep_funcs)

        return xds.load()

    def _apply_process_pipeline(self, xds, pipe):
        xds.load()
        for func in pipe:
            xds = func(xds)

        return xds


class DummyNetCDF:
    def sel_points(self, *args, **kwargs):
        return pd.Series([None])

    def __getitem__(self, key):
        return DummyNetCDF()
