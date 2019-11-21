from config_utils import load_config
config = load_config()


def main():
    import pandas as pd
    date_range = pd.date_range(config['socat']['start'], 
                               config['socat']['end'], 
                               freq="1D")
    
    download()


def download(*args):
    from utils import download_utils as dl
    from pandas import read_parquet
    import os

    source_url   = "https://www.nodc.noaa.gov/archive/arc0105/0160558/4.4/data/0-data/MPI_SOM-FFN_v2018/spco2_MPI_SOM-FFN_v2018.nc"
    dest_path    = os.path.join(config['path']['raw'], "somffn/spco2_MPI_SOM-FFN_v2018.nc")

    dl.download_wget(source_url, dest_path)

    
def preprocessor_1deg(xds):
    
    
    return xds
    
    

if __name__ == "__main__":
    main()
   
