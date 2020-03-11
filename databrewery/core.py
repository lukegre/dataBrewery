from . utils import BrewError, ConfigError, BrewWarning
from . utils import Path, ObjectDict, URL


class DataKeg:
    def __init__(self, variable_name, config_dict, verbose=2):

        self.name = variable_name
        self.debug = True if verbose == 2 else False
        self.verbose = verbose
        self.__config__ = config_dict

        self._process_dates()
        self._process_paths()
        self._process_login()
        self._process_keywords()
        self.downloaders = []

    def __str__(self):
        name = self.name.upper()
        blank = ' '*2
        L = 80 - (len(name) + 2)
        eq = "="
        txt = f"{eq:=>{L//2-1}}  {name: >2}{blank:=<{L//2-1}}\n"
        n = len(self.name) + 2
        for key in ['url', 'path', 'date', 'login', 'keywords']:
            keyu = key.upper()
            vals = getattr(self, key).__str__()
            if vals == "":
                continue
            L = 80 - (len(key) + 2)
            txt += f"\n{keyu: >2}"  # {blank:-<{L}}\n"
            txt += ("\n" + vals).replace("\n", '\n    ')
            txt += '\n'

        return txt

    def _process_paths(self):
        if 'path' not in self.__config__:
            raise ConfigError(f'Config for {self.name} requires at least a `path` entry '
                              'where files are stored locally.')
        else: 
            self.path = Path(self.__config__['path'])

        if 'url' not in self.__config__:
            raise ConfigError(f'Config for {self.name} requires at least a `url` entry '
                              'where files are stored remotely.')
        else:
            self.url = URL(self.__config__['url'])
            
            from .download import determine_connection_type
            self._downloader = determine_connection_type(self.url)
            
        # assume all non-standard entries are paths
        all_items = set(self.__config__.keys())
        standard_items = set(['path', 'url', 'date', 'login', 'keywords'])
        custom_items = all_items - standard_items
        
        for key in custom_items:
            setattr(self, key, Path(self.__config__[key]))        
                
    def _process_dates(self):
        from pandas import Timestamp
        from warnings import warn
        import re
        from collections import defaultdict
        dates = self.__config__.get('date', {})
        
        today = Timestamp.today()
        start_of_current_year = Timestamp(str(today.year))
        
        dates['start'] = dates.get('start', str(start_of_current_year)[:10])
        dates['end'] = dates.get('end', str(today)[:10])
        
        self.date = ObjectDict(dates)
    
    def dprint(self, *msg):
        if self.debug:
            print(*msg)
    
    def qprint(self, *msg):
        if self.verbose >= 1:
            print(*msg)
    
    def _process_login(self):
        login = self.__config__.get('login', {})
        self.login = ObjectDict(login)

    def _process_keywords(self):
        keywords = self.__config__.get('keywords', tuple([]))

        self.keywords = tuple(keywords)

    def _initiate_connection(self):
        host = self.url.parsed.netloc
        connection = self._downloader(host, **self.login._dict)
        return connection

    def _date_check(self, dates):
        from pandas import Timestamp, DatetimeIndex
        if isinstance(dates, str):
            dates = Timestamp(dates)
        elif isinstance(dates, Timestamp):
            dates = dates
        elif isinstance(dates, DatetimeIndex):
            dates = dates
        else:
            raise ValueError('Something is wrong with the date input. Must be '
                             'str(YYYY-MM-DD) or Timestamp or DatetimeIndex')
            
        return dates
    
    def _date_range_check(self, start, end):
        from warnings import warn
        message = (
            f"The date range in the config file (or automatically set) for "
            f"{self.name}.dates is {self.date.start} to {self.date.start}. "
            f"The range you defined is outside this from {start} to {end}. "
        )
        dstart = self._date_check(start)
        dend = self._date_check(end)
        if (dstart < self.date.start) or (dend > self.date.end):
            warn(message, BrewWarning)
    
    def _download_single_process(self, remote_local_files, queue, verbose):
        downloader = self._initiate_connection()
        downloader.verbose = verbose
        self.downloaders += downloader,
        for remote, local in remote_local_files:
            downloader.download_file(remote, local)

        if queue is not None:
            queue.put(downloader.missing_files)
        else:
            return downloader.missing_files

    def _download_multiple_processes(self, remote_local_files, njobs, verbose=1):
        from multiprocessing import Process, Queue

        paths = remote_local_files
        verbose = 1 if verbose == 2 else verbose

        step = paths.shape[0] // njobs
        processes = []
        split_paths = []
        for i in range(njobs):
            i0 = i * step
            i1 = (i+1) * step
            # split_paths += paths[i0:i1],
            split_paths += paths[i:njobs],

        processes = []
        queue = Queue()
        for i in range(njobs):
            p = Process(target=self._download_single_process,
                        args=(split_paths[i], queue, 1))
            processes += p,
            p.start()

        missing_files = []
        for p in processes:
            missing_files += queue.get()
            p.join()

        return missing_files

    def download_data(self, dates=None, verbose=2, njobs=1):
        from pandas import date_range
        from numpy import unique, sort
        from multiprocessing import cpu_count
        
        if dates is None:
            dates = date_range(self.date.start, self.date.end, freq='1D')
            
        remote_paths = self.url[dates]
        local_paths = self.path[dates]

        if isinstance(remote_paths, str):
            remote_paths = [remote_paths]
            local_paths = [local_paths]

        if len(remote_paths) != len(local_paths):
            raise BrewError(
                f'{self.name}.path.url and {self.name}.path.raw '
                'produce different number of files. Ensure that '
                'the paths are compatible with matching date strings')        
        paths = [a for a in zip(remote_paths, local_paths)]
        paths = unique(paths, axis=0)
        
        ncpus = cpu_count() - 1
        njobs = ncpus if njobs > ncpus else njobs
        
        self.qprint(f'Downloading {paths.shape[0]} {self.name} files with {njobs} jobs')

        if njobs == 1:
            missing_files = self._download_single_process(paths, None, verbose=verbose)
        if njobs > 1:
            missing_files = self._download_multiple_processes(paths, njobs, verbose=verbose)

        self.qprint(f"Missing files ({len(missing_files)}) stored in {self.name}.missing_files")
        self.missing_files = missing_files
    

class Brewery:
    def __init__(self, config_file='./config.yaml', verbose=1):
        from . config import read_config_as_dict
        
        self.verbose = verbose
        self._config_dict = read_config_as_dict(config_file)
        self._create_kegs()
        self.taps = Taps(self, 'path')
        
    def _create_kegs(self):
        
        for key in self._config_dict.keys():
            barrel = DataKeg(key, self._config_dict[key], verbose=self.verbose)
            setattr(self, key, barrel)
            
        if self.verbose:
            print(self)   

    def __str__(self):
        import re

        out = ""
        txt = 'Your Brewery contains the following DataKegs'
        b = "="
        out += f"{txt}\n{b:=>{len(txt)}}\n" 
        for key in self._config_dict.keys():
            barrel = getattr(self, key, None)
            if barrel is None:
                continue

            dates = f"{barrel.date.start} : {barrel.date.end}"
            scheme = barrel.url.parsed.scheme.upper()
            keywords = re.sub("[\'\(\)]", "", str(barrel.keywords))
            out += f"{key: <15}{dates}   {scheme: <8}{keywords}\n"

        out += "\nAccess all local paths via keywords through dataBrewery.MENU"
        return out


class Taps:
    def __init__(self, craft_brewery, attr):
        from collections import defaultdict
        
        barrel_names = craft_brewery._config_dict.keys()
        barrels = [getattr(craft_brewery, k) for k in barrel_names]

        keywords = defaultdict(dict)
        for barrel in barrels:
            for kw in barrel.keywords:
                keywords[kw].update({barrel.name: getattr(barrel, attr)})

        self._kw = keywords
        for kw in keywords:
            setattr(self, kw, ObjectDict(keywords[kw]))

    def __repr__(self):
        out = ""
        keys = sorted(self._kw.keys())
        out += "VARIABLE NAME       DATASET\n"
        rule = "-" * len(out) + "\n"
        out += rule
        for key in keys:
            val = self._kw[key]
            out += f'{key.upper(): <20}'
            for v in val:
                out += f'{v}, '
            out += '\n'
        out += rule
        return out


