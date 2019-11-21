
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
    