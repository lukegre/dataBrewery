import os
import warnings

warnings.filterwarnings('ignore', category=DeprecationWarning)


def is_file_valid(local_path, opener=None):
    import os

    # does the file exists
    if not os.path.isfile(local_path):
        return False

    # has an opener been passed, if not assumes file is valid
    if opener is None:
        return True

    # tries to open the path, if it fails, not valid, if it passes, valid
    try:
        with opener(local_path) as obj:
            return True
    except:
        return False


def download_wget(remote_path, local_path, *wget_args, verbose=True, local_file_checker=None, **wget_kwargs):
    def vprint(message, **kwargs):
        if verbose:
            print(message, **kwargs)
    import os

    remote_dir, remote_file = os.path.split(remote_path)
    local_dir, local_file = os.path.split(local_path)

    if is_file_valid(local_path, local_file_checker):
        vprint(f"File exists: {local_path}")
        return local_path

    if not url_exists(remote_path):
        vprint(f"URL does not exist: {remote_path}")
        return False

    command = "wget --quiet --show-progress --timeout=5 --tries=2"
    for opt in wget_args:
        command += f" --{opt}"

    for opt, val in wget_kwargs.items():
        command += f' --{opt}={val}'

    if not os.path.isdir(local_dir):
        os.makedirs(local_dir)

    command += f" --output-document={local_path} "
    command += remote_path

    os.system(command)
    
    
def connect_to_ftp_server(host: str, username: str, password: str):
    import ftplib
    
    ftp = ftplib.FTP(host)
    ftp.login(username, password)
    return ftp


def download_ftp_file(ftp_server, remote_path, local_path, local_file_checker=None, verbose=False):

    def vprint(message, **kwargs):
        if verbose:
            print(message, **kwargs)

    def remote_path_exists(ftp_server, remote_path):
        import ftplib
        remote_dir = os.path.split(remote_path)[0]
        try:
            remote_flist = ftp_server.nlst(remote_dir)
        except ftplib.error_temp:
            vprint(f'Remote folder does not exist {remote_dir}')
            return False

        if remote_path in remote_flist:
            return True
        else:
            vprint(f'Remote file does not exist {remote_path}')
            return False

    if is_file_valid(local_path, local_file_checker):
        vprint(f'Local file exists: {local_path}')
        return True

    local_dir, local_file = os.path.split(local_path)
    if not os.path.isdir(local_dir):
        os.makedirs(local_dir)

    vprint(f'Downloading {local_file}')
    ftp_server.retrbinary('RETR '+ remote_path, open(local_path, 'wb').write)
    return True


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


def unzip(zip_path:str, dest_dir:str=None):

    from zipfile import ZipFile

    if not os.path.isfile(zip_path):
        raise OSError(f'The zip file does not exist: {zip_path}')

    if dest_dir is None:
        dest_dir = os.path.split(zip_path)[0]

    with ZipFile(zip_path, 'r') as zip:
        flist_zip = set(zip.namelist())
        flist_dir = set(os.listdir(dest_dir))

        files_to_extract = list(flist_zip - flist_dir)

        if files_to_extract == []:
            print(f'All files in {zip_path} exist in {dest_dir}')

        zip.extractall(path=dest_dir, members=files_to_extract)


def gunzip(zip_path: str, dest_path:str=None):
    import shutil
    import gzip

    if dest_path is None:
        dest_path = zip_path.replace('.gz', '')

    with gzip.open(zip_path, 'rb') as f_in:
        with open(dest_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
            return f_out


def check_pandas_datetime_index(date_range):
    import pandas as pd

    if isinstance(date_range, pd.DatetimeIndex):
        return
    else:
        date_range_type = str(type(date_range))
        raise TypeError(f'Input date range is {date_range_type}, but should be pandas.DatetimeIndex')


def date_range_to_custom_freq(date_range, freq='1M'):
    import pandas as pd
    return pd.date_range(date_range[0], date_range[-1], freq=freq)

