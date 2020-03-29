import os
import warnings

warnings.filterwarnings('ignore', category=DeprecationWarning)


class Downloader:
    """
    verbosity levels
        0: doesnt print anything
        1: prints minimal - commands only
        2: prints line by line commands and live progress of downloads
    """
    _cache_flist = []
    _cache_dir = ''

    def __init__(self, host, verbose=2, service=None,
                 username='anonymous', password=None, **kwargs):

        if password is None:
            from keyring import get_password
            password = get_password(service, username)

        self._check_host_valid(host)
        self.verbose = verbose

        self._method_init(host, username, password, **kwargs)

    def _method_init(self, host, username, password, **kwargs):
        # placehoder method for individual methods
        pass

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

        slocal = shorten_path_for_print(local)
        if self.is_local_file_valid(local):
            self._print(f'File exists locally: {slocal}', lvl=2)
            return 2

        remote = self.get_remote_pathname_match(remote)
        if remote is None:
            return 1

        # making local directory
        local_dir = os.path.split(local)[0]
        os.makedirs(local_dir, exist_ok=True, mode=511)

        description = f'Downloading {slocal}'
        if int(self.verbose) >= 2:
            self._vdownload(remote, local, description)
        else:
            self._print(description, lvl=1)
            self._qdownload(remote, local)
        return 0

    def get_remote_pathname_match(self, remote_path):
        """
        pass a filename with *?[] and returns any matching filename
        note that only one match is accepted - otherwise returns None
        """
        from urllib.parse import urlparse
        from fnmatch import fnmatch
        import os

        sremote = shorten_path_for_print(remote_path)
        url = urlparse(remote_path)

        # get the remote_directory
        remote_path = url.path
        remote_directory, remote_file = os.path.split(remote_path)

        if remote_directory != self._cache_dir:
            self._cache_dir = remote_directory
            self._cache_flist = self.listdir(remote_directory)

        if self._cache_flist == []:
            self._print(f'URL does not exist: {remote_directory}', lvl=3)
            return None

        # returns matches for *? [0-9A-Z]
        file_match = [f for f in self._cache_flist if fnmatch(f, remote_path)]
        num_matches = len(file_match)

        if num_matches == 1:
            return file_match[0]

        # The rest is purely to inform the user of mismatches
        if num_matches > 1:
            msg = (f"URL returns {num_matches} matches: {remote_path}\n"
                   f"{file_match}\n\nThe URL must only return one file. "
                   f"* is only for changing elements in a file")
        if num_matches < 1:
            msg = f"Remote file does not exist: {sremote}"

        self._print(msg, lvl=3)

        return None

    def listdir(self, path):
        return [path]

    def _print(self, *msg, lvl=1):
        """
        process printer where verbosity is defined by set level
        the run verbosity is taken from self.verbose
        """
        if self.verbose >= lvl:
            print(*msg)

    def close_connection(self):
        pass

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
            with opener(local_path):
                return True
        except error:
            return False


class FTP(Downloader):
    """
    creates a connection under `self.ftp`
    this connection can be closed
    """

    def _method_init(self, host, username, password, **kwargs):
        import ftplib

        self.ftp = ftplib.FTP(host)
        self.ftp.login(username, password)

    def _vdownload(self, remote, local, pbar_desc):
        from tqdm import tqdm
        from urllib.parse import urlparse

        remote = urlparse(remote).path

        with open(local, 'wb') as fd:
            size = self.get_file_size(remote)
            with tqdm(total=size, desc=pbar_desc, unit='B', unit_scale=True) as pbar:
                def cb(data):
                    pbar.update(len(data))
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
            raise error_temp('Server timeout. Try restarting the connection')
        except error_reply:
            return []

    def get_file_size(self, path):
        self.ftp.sendcmd("TYPE i")
        return self.ftp.size(path)

    def close_connection(self):
        self.ftp.close()


class SFTP(Downloader):

    def _method_init(self, host, username, password, **kwargs):
        import pysftp

        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        sftp_options = dict(username=username, password=password,
                            host=host, cnopts=cnopts)
        sftp_options.update(kwargs)

        self.sftp = pysftp.Connection(**sftp_options)

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
                    pbar.update(len(data))
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

    def _method_init(self, host, username, password, **kwargs):
        from requests.auth import HTTPBasicAuth

        self.auth = HTTPBasicAuth(username, password)

    def _vdownload(self, remote, local, pbar_desc):
        import requests
        from tqdm import tqdm

        req = requests.get(remote, auth=self.auth, stream=True)
        if not req.ok:
            self._print(f"URL does not exist: {remote}", lvl=2)
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
            self._print(f"URL does not exist: {remote}", lvl=2)
            return False

        step = 5 * 2**10
        with open(local, 'wb') as f:
            for data in req.iter_content(step):
                f.write(data)

    def get_remote_pathname_match(self, remote_path):
        return remote_path


class CDS(Downloader):

    def _method_init(self, host, username, password, **cdsapi_client_kwargs):
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

        assert isinstance(date, Timestamp), (
            'ERA5 input argument must be a pandas.Timestamp')

        slocal = shorten_path_for_print(local)
        if self.is_local_file_valid(local):
            self._print('File exists locally:', slocal, lvl=2)
            return 2

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
        return 0


def shorten_path_for_print(path, maxlen=100):
    if len(path) <= maxlen:
        return path

    from urllib.parse import urlparse
    url = urlparse(path)
    out = ''

    out += url.scheme + '://' if url.scheme != '' else ''
    out += url.netloc

    out += '/'.join(url.path[:20].split('/')[:-1])
    out += '/.../'
    length_remaining = maxlen - len(out)
    end_of_path_ugly = url.path[-length_remaining:]
    out += '/'.join(end_of_path_ugly.split('/')[1:])

    return out


def determine_connection_type(remote_url_unformatted):
    from urllib.parse import urlparse
    url = urlparse(str(remote_url_unformatted))

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
        raise BaseException(f'The URL scheme ({url.scheme}) does '
                            'not exist in dataBrewery config')

    return downloader
