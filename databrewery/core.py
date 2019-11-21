

class BrewingError(BaseException):
    pass


class BrewWithCaution(UserWarning, BaseException):
    pass


class BrewingConfigError(BaseException):
    pass


class ObjectDict:
    """
    A dictionary that displays values nicely,
    and keys are accessible as objects on single depth items
    """
    def __init__(self, dictionary):
        self._dict = dictionary
        self.keys = dictionary.keys
        self.values = dictionary.values
        self.line_len = 71
        self.split_chars = '/-_'
        
        for key in dictionary.keys():
            setattr(self, key, dictionary[key])

    def items(self):
        return self._dict.items()

    def __getitem__(self, key):

            return self._dict[key]

    def __contains__(self, key):
        return key in self.keys()

    def __repr__(self):
        return self._dict.__repr__()

    def __str__(self):
        tab = max([len(str(key)) for key in self._dict]) + 2
        txt = ""
        for key in self.keys():
            value = self[key]
            dtype = str(type(value)).split("'")[1].split('.')[-1]
            string = f"{str(value)}"
            string = self._split_string_optimally(str(string))
            txt += f"{key}:"
            txt += " " * (tab - len(str(key)))
            txt += string[0]
            if len(string) > 1:
                txt += ' ...\n'
                space = ' ' * (tab + 5)
                txt += space + (' ...\n' + space).join(string[1:])
            txt += '\n'

        return txt[:-1]

    def _split_string_optimally(self, string):
        import re

        matches = re.finditer(f'[{self.split_chars}]', string)
        splits = [m.start()+1 for m in matches] + [len(string)]

        counter_old = 0
        optimal_splits = [0]
        for i, m in enumerate(splits):
            counter = m // self.line_len
            if counter_old != counter:
                optimal_splits += splits[i-1],
            counter_old = counter

        optimal_splits += len(string) + 1,

        split_string = []
        for c, _ in enumerate(optimal_splits[:-1]):
            i0 = optimal_splits[c]
            i1 = optimal_splits[c+1]
            split_string += string[i0:i1],

        return split_string


class KegPath:
    def __init__(self, path, date_start=None, date_end=None, clip_dates=False):
        from pandas import Timestamp
        self._check_if_date_path(path)

        self.value = path
        self.format = self.value.format
        
        lower = self._try_convert_to_date_format(date_start)
        upper = self._try_convert_to_date_format(date_end)
        lower = Timestamp.today() if lower is None else lower
        upper = Timestamp.today() if upper is None else upper
        self.date_lower_limit = self.low = lower
        self.date_upper_limit = self.upp = upper
        self.clip_dates = clip_dates
    
    def __call__(self, *args, **kwargs):
        return self.value
    
    def __format__(self, format_spec):
        return self.value.__format__(format_spec)
    
    def __add__(self, value):
        return self.value.__add__(value)
    
    def __str__(self):
        return self.value.__str__()
    
    def __repr__(self):
        return self.value
    
    def __getitem__(self, key):
        from pandas import Timestamp
        
        # use string getting if keys are none or ints
        if isinstance(key, int):
            return self.value[key]
        if isinstance(key, slice):
            inds = [key.start, key.stop, key.step]
            if all(isinstance(k, (int, type(None))) for k in inds):
                return self.value[key]
        
        key = self._try_convert_to_date_format(key)
        
        if isinstance(key, Timestamp):
            return self.format(t=key)
        
        if isinstance(key, slice):
            return self._process_slice(key)
        
        raise ValueError(f"{key} is not yet supported for indexing")
                 
    def _check_if_date_path(self, path):
        has_date_fmt = ('%Y' in path) | ('%m' in path) | ('%d' in path)
        has_braces = ('{' in path) & ('}' in path)
        has_t_fmt = ('{t:' in path)

        if has_braces and has_date_fmt and not has_t_fmt:
            raise NameError(
                f"Your path ({path}) is not correctly defined. "
                "You need to define date block as {t:%...} for the "
                "date formatting to work. ")
    
    def _try_convert_to_date_format(self, key):
        from pandas import  Timestamp
        
        message = ('\nYou are trying to index with an invalid date. Must be:'
                   '\n - YYYY-MM-DD string'
                   '\n - pandas.Timestamp'
                   '\n - pandas.DatetimeIndex'
                   '\n - None (defaults to today)'
                   '\n or slice object with the same format')
        
        if not isinstance(key, (type(None), slice, str, Timestamp)):
            raise IndexError(message)
        
        if isinstance(key, str):
            try:
                return Timestamp(key)
            except ValueError:
                raise IndexError(message)
        
        if isinstance(key, slice):
            new_slice_args = []
            for attr in ['start', 'stop']:
                new_slice_args += self._try_convert_to_date_format(getattr(key, attr)),
            return slice(*new_slice_args)
        
        return key
    
    def _process_slice(self, key):
        
        t0 = self.date_lower_limit if key.start is None else key.start
        t1 = self.date_upper_limit if key.stop  is None else key.stop
        ts = '1D' if key.step is None else self._check_freq_string(key.step)
    
        return self._make_string_range(t0, t1, ts)

    def _check_freq_string(self, step):
        import re
        matches = re.findall('[0-9]{,3}[hDMA]', str(string))
        if len(matches) == 1:
            return step
        else:
            raise IndexError(f'Slice step ({string}) is not a valid pandas time offset value')

    def _make_string_range(self, start, stop, step):
        from pandas import date_range
        from numpy import unique
        
        if self.clip_dates:
            start = self._clip_date_to_limits(start)
            stop  = self._clip_date_to_limits(stop)
            
        dates = date_range(start, stop, freq=step)
        paths = [self.format(t=d) for d in dates]
        paths = unique(paths)
        
        return paths
    
    def _clip_date_to_limits(self, date):
        date = self.upp if date > self.upp else date
        date = self.low if date < self.low else date
        return date
    
    def _is_date_in_bounds(self, date):
        if (date > self.date_lower_limit) & (date < self.date_upper_limit):
            return True
        elif not self.confine_dates_to_limits:
            from warnings import warn
            d = date.strftime('%Y-%m-%d')
            dL = self.date_lower_limit.strftime('%Y-%m-%d')
            dU = self.date_upper_limit.strftime('%Y-%m-%d')
            warn(f'\n{d} is outside of date config bounds ({dL} to {dU})\n\n', BrewWithCaution)
        return False
    
    @property
    def parsed(self):
        from urllib.parse import urlparse
        return urlparse(self.value)


class DataKeg:
    def __init__(self, variable_name, config_dict, verbose=2):

        self.name = variable_name
        self.debug = True if verbose == 2 else False
        self.__config__ = config_dict

        self._process_dates()
        self._process_paths()
        self._process_login()
        self._process_keywords()
        self.downloaders = []

    def __repr__(self):
        name = self.name.upper()
        blank = ' '*2
        L = 80 - (len(name) + 2) 
        txt = f"{name: >2}{blank:=<{L}}\n"
        n = len(self.name) + 2

        for key in ['path', 'date', 'login', 'keywords']:
            keyu = key.upper()
            vals = getattr(self, key).__str__()
            L = 80 - (len(key) + 2) 
            txt += f"\n{keyu: >2}{blank:-<{L}}\n"
            txt += vals
            txt += '\n'

        return txt

    def _process_paths(self):
        paths = ObjectDict(self.__config__.get('path', {}))
        if paths is {}:
            raise BrewingConfigError(f'Config for {self.name} requires a `path` entry')

        t0 = self.date.start
        t1 = self.date.end
        
        paths = {k: KegPath(v, t0, t1) for k, v in paths.items()}
        
        if 'raw' not in paths:
            raise BrewingConfigError(f'You need at least path.raw for {self.name} config')

        if 'url' in paths:
            if paths['url'].parsed.scheme == '':
                msg = f'{self.name}.path.url must start with [ftp, http, sftp, cds]'
                raise BrewingConfigError(msg)
            else:
                from .download import determine_connection_type
                self._downloader = determine_connection_type(paths['url'].value)
                
        
        self.path = ObjectDict(paths)

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
    
    def _process_login(self):
        login = self.__config__.get('login', {})
        self.login = ObjectDict(login)

    def _process_keywords(self):
        keywords = self.__config__.get('keywords', tuple([]))

        self.keywords = tuple(keywords)

    def _initiate_connection(self):
        host = self.path.url.parsed.netloc
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
            warn(message, BrewWithCaution)
    
    def _download_single_process(self, remote_local_files, queue, verbose):
        downloader = self._initiate_connection()
        downloader.verbose = 1
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
            split_paths += paths[i0:i1],

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
        from numpy import unique, sort
        from multiprocessing import cpu_count
        
        if dates is None:
            dates = slice(self.date.start, self.date.end, '1D')
            
        remote_paths = self.path.url[dates]
        local_paths = self.path.raw[dates]

        if isinstance(remote_paths, str):
            remote_paths = [remote_paths]
            local_paths = [local_paths]

        if len(remote_paths) != len(local_paths):
            raise BrewingError(
                f'{self.name}.path.url and {self.name}.path.raw '
                'produce different number of files. Ensure that '
                'the paths are compatible with matching date strings')        
        paths = [a for a in zip(remote_paths, local_paths)]
        paths = unique(paths, axis=0)
        
        ncpus = cpu_count() - 1
        njobs = ncpus if njobs > ncpus else njobs
        
        print(f'Downloading {paths.shape[0]} files with {njobs} jobs')

        if njobs == 1:
            missing_files = self._download_single_process(paths, None, verbose=verbose)
        if njobs > 1:
            missing_files = self._download_multiple_processes(paths, njobs, verbose=verbose)

        print(f"Missing files ({len(missing_files)}) stored in {self.name}.missing_files")
        self.missing_files = missing_files
    

class CraftBrewery:
    def __init__(self, config_file='./config.yaml'):
        from . config import read_config_as_dict
        
        self._config_dict = read_config_as_dict(config_file)
        self._create_kegs()
        
    def _create_kegs(self):
        import re
        
        txt = 'Loading the following DataKegs'
        b = "="
        print(f"{txt}\n{b:=>{len(txt)}}")
        for key in self._config_dict.keys():
            keg = DataKeg(key, self._config_dict[key], verbose=1)
            setattr(self, key, keg)
            
            dates = f"{keg.date.start} : {keg.date.end}"
            scheme = keg.path.url.parsed.scheme.upper()
            keywords = re.sub("[\'\(\)]", "", str(keg.keywords))
            txt = f"{key: <15}{dates}   {scheme: <8}{keywords}"
            print(txt)
    