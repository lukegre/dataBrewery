import os
import warnings

warnings.filterwarnings('ignore', category=DeprecationWarning)


class Downloader:
    verbose = 2
    missing_files = []
    _cache_flist = []
    _cache_dir = ''

    def _check_host_valid(self, host):
        import re
        match = re.findall(':[0-9]{2,}', host)
        if len(match) != 0:
            raise BaseException(
                f'Detected port {match[0]} assignment in URL, please '
                'specify as argument in config.yaml file under login'
            )

    def _vdownload(self, remote, local, pbar_desc):
        pass

    def _qdownload(self, remote, local):
        pass

    def download_file(self, remote, local):
        if self.is_local_file_valid(local):
            self.qprint('File exists locally:', shorten_path_for_print(local))
            return None

        remote = self.get_remote_pathname_match(remote)
        if remote is None:
            return None

        # making local directory
        local_dir = os.path.split(local)[0]
        os.makedirs(local_dir, exist_ok=True, mode=511)

        short_local_path = self.shorten_path_for_print(local, 100)
        description = f'Downloading {short_local_path}'
        if int(self.verbose) > 1:
            self._vdownload(remote, local, description)

        else:
            self.qprint(description)
            self._qdownload(remote, local)

    def get_remote_pathname_match(self, remote_path):
        """
        pass a filename with *?[] and returns any matching filename
        note that only one match is accepted - otherwise returns None
        """
        from urllib.parse import urlparse
        from fnmatch import fnmatch
        import os

        url = urlparse(remote_path)

        # get the remote_directory
        remote_path = url.path
        remote_directory, remote_file = os.path.split(remote_path)

        if remote_directory != self._cache_dir:
            self._cache_dir = remote_directory
            self._cache_flist = self.listdir(remote_directory)

        if self._cache_flist == []:
            self.qprint(f'Remote file does not exist: {remote_directory}')
            self.missing_files += remote_path,
            return

        # returns matches for *? [0-9A-Z]
        file_match = [f for f in self._cache_flist if fnmatch(f, remote_path)]
        num_matches = len(file_match)

        if num_matches == 1:
            return file_match[0]

        # The rest is purely to inform the user of mismatches
        if num_matches > 1:
            msg = (f"URL returns {num_matches} matches: {remote_path}\n"
                   f"{file_match}\n\nThe URL must only return one file. "
                    "* is only for changing elements in a file")
        if num_matches < 1:
            msg = (f"Remote file does not exist: " +
                   self.shorten_path_for_print(remote_path, 100))

        self.qprint(msg)
        self.missing_files += remote_path,

        return None

    def listdir(self, path):
        return [path]

    def qprint(self, *msg):
        if self.verbose == 1:
            print(*msg)

    def vprint(self, *msg):
        if self.verbose > 1:
            print(*msg)

    def close_connection(self):
        pass

    @staticmethod
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

    @staticmethod
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
            opener = open  # dummy opener
            
        # tries to open the path, if it fails, not valid, if it passes, valid
        try:
            with opener(local_path) as obj:
                return True
        except error:
            return False


class FTP(Downloader):
    def __init__(self, host, verbose=2, username=None, password=None, **kwargs):
        import ftplib

        username = "anonymous" if username is None else username
        password = "" if password is None else password

        self._check_host_valid(host)
        self.ftp = ftplib.FTP(host)
        self.ftp.login(username, password)
        self.verbose = verbose

    def _vdownload(self, remote, local, pbar_desc):
        from tqdm import tqdm
        from urllib.parse import urlparse

        remote = urlparse(remote).path

        with open(local, 'wb') as fd:
            size = self.get_file_size(remote)
            with tqdm(total=size, desc=pbar_desc, unit='B', unit_scale=True) as pbar:
                def cb(data):
                    l = len(data)
                    pbar.update(l)
                    fd.write(data)
                self.ftp.retrbinary('RETR {}'.format(remote), cb)

    def _qdownload(self, remote, local):
        from urllib.parse import urlparse
        remote = urlparse(remote).path
        with open(local, 'wb') as fd:
            self.ftp.retrbinary('RETR {}'.format(remote), fd.write)

    def listdir(self, directory='.'):
        """Will always list the directory, even if a file is given"""
        from ftplib import error_reply, error_temp

        try:
            flist = self.ftp.nlst(directory)
            return sorted(flist)
        except error_temp:
            return []
        except BrokenPipeError:
            raise error_temp('Server timeout. Try clossing and restarting the connection')
        except error_reply:
            return []

    def get_file_size(self, path):
        self.ftp.sendcmd("TYPE i")
        return self.ftp.size(path)

    def close_connection(self):
        self.ftp.close()


class SFTP(Downloader):
    def __init__(self, host, verbose=2, username=None, password=None, **kwargs):
        import pysftp

        self._check_host_valid(host)

        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        sftp_options = dict(host=host, username=username, password=password, cnopts=cnopts)
        sftp_options.update(kwargs)

        self.sftp = pysftp.Connection(**sftp_options)
        self.verbose = verbose

    def _vdownload(self, remote, local, pbar_desc):
        from tqdm import tqdm
        from urllib.parse import urlparse

        remote = urlparse(remote).path

        class TqdmWrap(tqdm):
            def viewBar(self, a, b):
                self.total = int(b)
                self.update(int(a - self.n))  # update pbar with increment

        with open(local, 'wb') as fd:
            with TqdmWrap(desc=pbar_desc, unit='b', unit_scale=True) as pbar:
                def cb(data):
                    l = len(data)
                    pbar.update(l)
                    fd.write(data)

                self.sftp.get(remote, local, callback=pbar.viewBar)

    def _qdownload(self, remote, local):
        from urllib.parse import urlparse
        remote = urlparse(remote).path
        with open(local, 'wb') as fd:
            self.sftp.get(remote, local)
            return

    def listdir(self, directory=''):
        """Will always list the directory, even if a file is given"""
        import os

        try:
            flist = self.sftp.listdir(directory)
            flist = [os.path.join(directory, f) for f in flist]
            return sorted(flist)
        except FileNotFoundError:
            return []

    def get_file_size(self, path):
        return self.sftp.stat(path).st_size

    def close_connection(self):
        self.sftp.close()


class HTTP(Downloader):
    def __init__(self, *args, verbose=2, username=None, password=None, **kwargs):
        from requests.auth import HTTPBasicAuth

        self.verbose = verbose
        self.auth = HTTPBasicAuth(username, password)

    def _vdownload(self, remote, local, pbar_desc):
        import requests
        from tqdm import tqdm

        req = requests.get(remote, auth=self.auth, stream=True)
        if not req.ok:
            self.vprint(f"URL does not exist: {remote}")
            return False

        step = 5 * 2**10
        size = int(req.headers.get('content-length', 0))
        pbar = tqdm(desc=pbar_desc, total=size, unit='B', unit_scale=True)
        with open(local, 'wb') as f:
            for data in req.iter_content(step):
                pbar.update(len(data))
                f.write(data)
        pbar.close()

    def _qdownload(self, remote, local):
        import requests

        req = requests.get(remote, auth=self.auth, stream=True)
        if not req.ok:
            self.vprint(f"URL does not exist: {remote}")
            return False

        step = 5 * 2**10
        with open(local, 'wb') as f:
            for data in req.iter_content(step):
                f.write(data)

    def get_remote_pathname_match(self, remote_path):
        return remote_path


class CDS(Downloader):
    def __init__(self, *args, **cdsapi_client_kwargs):
        import cdsapi
        self.cds = cdsapi.Client(**cdsapi_client_kwargs)

        self.times = [f'{t:02d}:00' for t in range(24)]
        self.variables = [  # from Climate Data Store website
            '10m_u_component_of_wind',
            '10m_v_component_of_wind',
            'mean_sea_level_pressure',
            # 'sea_surface_temperature',
            # 'sea_ice_cover',
            # 'air_density_over_the_oceans',
            # 'eastward_turbulent_surface_stress',
            # 'northward_turbulent_surface_stress',
            # 'surface_latent_heat_flux',
            # 'surface_net_solar_radiation',
            # 'surface_net_thermal_radiation',
            # 'surface_sensible_heat_flux',
            # 'evaporation',
            # 'total_precipitation',
            # '2m_dewpoint_temperature',
            # '2m_temperature'
            ]

    def download_file(self, date, local):
        """
        date is pandas.Timestamp object
        local is the path to a local directory
        """
        from pandas import Timestamp
        assert isinstance(date, Timestamp), 'ERA5 input argument must be a pandas.Timestamp'

        if self.is_local_file_valid(local):
            self.vprint('File exists locally:', self.shorten_path_for_print(local))
            return None

        # making local directory
        local_dir = os.path.split(local)[0]
        if not os.path.isdir(local_dir):
            os.makedirs(local_dir, exist_ok=True, mode=744)

        self.cds.retrieve(  # according to Climate Data Store API
            'reanalysis-era5-single-levels',
            {
                'product_type': 'reanalysis',
                'format': 'netcdf',
                'variable': self.variables,
                'year':  f"{date.year:04d}",
                'month': f"{date.month:02d}",
                'day':  [f"{d:02d}" for d in range(1, 32)],
                'time': self.times,
            },
            local)


def shorten_path_for_print(path, maxlen=100):
    if len(path) <= maxlen:
        return path

    from urllib.parse import urlparse
    url = urlparse(path)
    out = ''

    out += url.scheme + '://' if url.scheme is not '' else ''
    out += url.netloc

    out += '/'.join(url.path[:20].split('/')[:-1])
    out += '/.../'
    length_remaining = maxlen - len(out)
    end_of_path_ugly = url.path[-length_remaining:]
    out += '/'.join(end_of_path_ugly.split('/')[1:])

    return out


def determine_connection_type(remote_url_unformatted):
    from urllib.parse import urlparse
    url = urlparse(remote_url_unformatted)

    if url.scheme == '':
        raise BaseException('No connection scheme for URL, need '
                            'this to determine the connection type')

    downloader_dict = {'sftp':  SFTP,
                      'http':  HTTP,
                      'https': HTTP,
                      'ftp':   FTP,
                      'cds':   CDS,
                      }

    downloader = downloader_dict.get(url.scheme, None)
    if downloader is None:
        raise BaseException(f'The URL scheme ({url.scheme}) does not exist in dataBrewery config')

    return downloader
