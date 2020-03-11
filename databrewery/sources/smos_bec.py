
def main():
    import pandas as pd
    date_range = pd.date_range(config['smos_bec']['start'], 
                               config['smos_bec']['end'], 
                               freq="1D")
    
    for func_name in config['smos_bec']['functions']:
        func = globals()[func_name]
        func(date_range)

    
def download(date_range):
    from utils import download_utils as dl
    from tqdm import tqdm
    import pandas as pd
    import pysftp
    import os

    class ProgressBar(tqdm):
        def __init__(self, **kwargs):
            super(ProgressBar, self).__init__(
                unit='B', unit_scale=True, miniters=1, **kwargs)

        def update_to(self, b, tsize):
            if tsize is not None:
                self.total = tsize
            self.update(b/255)

    dl.check_pandas_datetime_index(date_range)

    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    sftp_options = dict(
        host = "becftp.icm.csic.es:27500",
        port = 27500,
        username = "gregorl",
        password = "Eegh7maen7eg7g",
        cnopts = cnopts)

    remote_name = "sftp://becftp.icm.csic.es/data/OCEAN/SSS/SMOS/Global/v1.0/L3/9day/{y1:04d}/BEC_ADVAOA_B_{y1:04d}{m1:02d}{d1:02d}T000000_{y2:04d}{m2:02d}{d2:02d}T000000_025_001.nc"
    local_name = os.path.join(config['path']['raw'], "smos_bec/{y1:04d}/{m1:02d}/BEC_ADVAOA_B_{y1:04d}{m1:02d}{d1:02d}T000000_{y2:04d}{m2:02d}{d2:02d}T000000_025_001.nc")

    with pysftp.Connection(**sftp_options) as sftp:
        remote_dir_previous = ""

        for t1 in date_range:
            t2 = t1 + pd.Timedelta(days=9)

            kwargs = dict(y1=t1.year, m1=t1.month, d1=t1.day,
                          y2=t2.year, m2=t2.month, d2=t2.day)
            remote_path = remote_name.format(**kwargs)
            local_path = local_name.format(**kwargs)

            remote_dir, remote_file = os.path.split(remote_path)
            local_dir, local_file = os.path.split(local_path)

            try:
                if remote_dir != remote_dir_previous:
                    remote_flist = sftp.listdir(remote_dir)
                remote_dir_previous = remote_dir
            except FileNotFoundError:
                print(f'Remote folder does not exist {remote_dir}')
                continue

            if remote_file not in remote_flist:
                print(f'Remote file does not exist: {remote_path}')
                continue

            if os.path.isfile(local_path):
                local_size = os.path.getsize(local_path)
                remote_size = sftp.stat(remote_path).st_size
                if local_size < remote_size:
                    os.remove(local_path)
                    print(f'Local filesize does not match remote {local_file}')
                else:
                    print(f'Local file exists {local_file}')
                    continue

            if not os.path.isdir(local_dir):
                os.makedirs(local_dir)

            size = sftp.stat(remote_path).st_size
            with ProgressBar(desc=f'Downloading {remote_file}') as t:
                sftp.get(remote_path, local_path, callback=t.update_to)

                
def preprocessor_smosbec_025d(xds):
    xds.time.values = xds.time.values.astype('datetime64[D]')
    return xds


def standardise_025_dialy_stored_monthly(date_range):
    from utils.netcdf_utils import daily_netcdf_stored_monthly
    import os

    daily_netcdf_stored_monthly(
        date_range,
        os.path.join(config['path']['raw'],  "smos_bec/{year:04d}/{month:02d}/BEC_ADVAOA_B_{year:04d}{month:02d}*.nc"),
        os.path.join(config['path']['grid'], "daily_025/smosbec_{{0}}/{year}/smosbec_{{0}}_{year:04d}_{month:02d}.nc"),
        ['oa_sss'],
        preprocessor_smosbec_025d)
                

if __name__ == "__main__":
    main()
