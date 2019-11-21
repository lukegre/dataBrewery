from config_utils import load_config
config = load_config()


def main():
    import pandas as pd
    date_range = pd.date_range(config['socat']['start'], 
                               config['socat']['end'], 
                               freq="1D")
    
    
    if 'download' in config['socat']['functions']:
        download()
        
    if 'grid_SOCAT_025d' in config['socat']['functions']:
        grid_SOCAT_025d(date_range)


def download(*args):
    from utils import download_utils as dl
    from pandas import read_parquet
    import os

    source_url   = "https://www.socat.info/socat_files/v2019/SOCATv2019.tsv.zip"
    dest_path    = os.path.join(config['path']['raw'], "socat2019/SOCATv2019.tsv.zip")
    parquet_path = os.path.join(config['path']['raw'], "socat2019/SOCATv2019.pq")

    zipped_name = dl.download_wget(source_url, dest_path)
    unzipped_name = dl.unzip(dest_path)
    
    read_SOCAT_tsv_to_parquet(unzipped_name, parquet_path, )


def read_SOCAT_tsv_to_parquet(fname, sname, data_start_line=5863, n_chunks=100):
    import numpy as np
    import pandas as pd
    from datetime import datetime as dt
    from tqdm import tqdm
    import os
    import warnings

    if os.path.isfile(sname):
        print("File exists: {}".format(sname))
        return

    with open(fname, encoding='latin') as tsv:
        for total_lines, _ in enumerate(tsv):
            pass

    print('Converting file to parquet for quicker access')
    dtypes = {
        'expocode': str,
        'version': str,
        'socat_doi': str,
        'qc_flag': str,
        'yr': str,
        'mon': str,
        'day': str,
        'hh': str,
        'mm': str,
        'ss': float,
        'longitude': float,
        'latitude': float,
        'sample_depth': float,
        'sal': float,
        'sst': float,
        'tequ': float,
        'pppp': float,
        'pequ': float,
        'woa_sss': float,
        'ncep_slp': float,
        'etopo2_depth': float,
        'dist_to_land': float,
        'gvco2': float,
        'fco2rec': float,
        'fco2rec_src': int,
        'fco2rec_flag': int}

    reader = pd.read_csv(
        filepath_or_buffer=fname,
        sep='\t',
        skiprows=data_start_line,  # line number manually estimated from file
        skipinitialspace=True,
        keep_date_col=True,
        dtype=dtypes,
        header=None,
        names=dtypes.keys(),
        low_memory=False,
        chunksize=(total_lines - data_start_line) // n_chunks,
        parse_dates={'time': ['yr', 'mon', 'day', 'hh', 'mm']},
        date_parser=lambda y, m, d, h, n: dt(*[int(s) for s in (y, m, d, h, n)]))

    desc = '{} to {}'.format(fname.split('/')[-1], sname.split('/')[-1])
    progress_reader = tqdm(reader, desc=desc, total=n_chunks, unit=' chunks')
    write_dataframe_iter_to_parquet(progress_reader, sname)


def write_dataframe_iter_to_parquet(iter_object, sname):
    """
    Writes iterable pandas objects to a parquet format.

    Pararmeters
    -----------
    iter_object : pandas.reader object
        must be an iterable pandas object that contains multiple dataframes
        with the same columns. These objects will be written to a parquet file
        as one dataframe. You can also wrap the object in a tqdm progress bar.
    sname : str
        string where the file will be saved to.

    Regt

    """

    import pyarrow as pa
    import pyarrow.parquet as pq

    def append_to_parquet_table(dataframe, filepath=None, writer=None):
        """Method writes/append dataframes in parquet format.

        This method is used to write pandas DataFrame as pyarrow Table in parquet format. If the methods is invoked
        with writer, it appends dataframe to the already written pyarrow table.

        Parameters
        ----------
        dataframe : pd.DataFrame
            to be written in parquet format.
        filepath : str
            target file location for parquet file.
        writer : object
            ParquetWriter object to write pyarrow tables in parquet format.

        Returns
        -------
        ParquetWriter : object
            This can be passed in the subsequenct method calls to append DataFrame
            in the pyarrow Table
        """

        table = pa.Table.from_pandas(dataframe)
        if writer is None:
            writer = pq.ParquetWriter(filepath, table.schema)
        writer.write_table(table=table)
        return writer

    writer = None
    for chunk in iter_object:
        writer = append_to_parquet_table(chunk, sname, writer)
    if writer:
        writer.close()


def grid_SOCAT_025d(date_range):
    from utils.gridding_utils import grid_geo_data
    from utils.netcdf_utils import make_cube, dataset_to_netcdf_per_var
    from utils.download_utils import is_file_valid, date_range_to_custom_freq
    from netCDF4 import Dataset as ncDataset
    import numpy as np
    import pandas as pd
    import xarray as xr
    import os
    
    output_str   = os.path.join(config['path']['grid'], "daily_025/socat_{{0}}/{year}/socat_{{0}}_{year:04d}_{month:02d}.nc")
    parquet_path = os.path.join(config['path']['raw'],  "socat2019/SOCATv2019.pq")
    
    df = None
    
    month_range = date_range_to_custom_freq(date_range)
    for t in month_range:
        output_name = output_str.format(year=t.year, month=t.month)
        month_vars = set(config['socat']['variables'])
        for key in config['socat']['variables']:
            if is_file_valid(output_name.format(key), ncDataset):
                print("File exists:", output_name.format(key))
                month_vars -= set([key])
        if len(month_vars) == 0:
            continue
        if df is None:
            print(f'Loading {parquet_path}')
            df = pd.read_parquet(parquet_path, use_threads=True)
            
        xds = xr.Dataset()
        
        t1 = f"{t.year}-{t.month}-01"
        t2 = f"{t.year}-{t.month}-{t.days_in_month}"
        trange = pd.date_range(t1, t2)

        dft = df.loc[(df.time >= t1) & (df.time <= t2)]
        if dft.shape[0] == 0:
            for key in month_vars:
                xds[key] = make_cube(t.year, t.month, dt=dt, dx=dx, dy=dy) * np.nan
        else:
            dft['lon180'] = dft.longitude.copy()
            i = dft.lon180 >= 180
            dft.loc[i, 'lon180'] -= 360
            for key in month_vars:
                xda = grid_geo_data(
                    dft[key], 
                    dft.time, 
                    dft.latitude, 
                    dft.lon180, 
                     dt='1D', dx=.25, dy=.25, funcs=['mean'])
                xds[key] = xda.reindex(time=trange, method='nearest')
        
        dataset_to_netcdf_per_var(xds, output_name, check_valid=True)
        

if __name__ == "__main__":
    main()
