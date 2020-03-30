
class Record:
    def __init__(self, record_name, config_dict, verbose=2):
        from .utils import DictObject

        self.name = record_name
        self.debug = True if verbose == 2 else False
        self.verbose = verbose

        self.meta = DictObject(dict(
            description=config_dict.pop('description'),
            doi=config_dict.pop('doi')))

        self.config = DictObject(config_dict)

        if hasattr(self.config, 'pipelines'):
            for key in self.config.pipelines:
                pipe = PipeFiles(key, self, self.config.pipelines[key])
                setattr(self, key, pipe)

        self.download_results = {'remote_not_exist': []}

    def __str__(self):
        name = self.name.upper()
        blank = ' '*2
        L = 80 - (len(name) + 2)
        eq = "="
        txt = f"{eq:=>{L//2-1}}  {name: >2}{blank:=<{L//2-1}}\n"
        # n = len(self.name) + 2
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

    def _print(self, *msg):
        if self.verbose >= 1:
            print(*msg)

    def _initiate_connection(self):
        from .download import determine_connection_type

        url = self.config['remote']['url']

        self._downloader = determine_connection_type(url)

        host = url.parsed.netloc
        login_dict = self.config.remote.__dict__.copy()
        login_dict.pop('url')
        connect = self._downloader(host, **login_dict)

        return connect

    def _download_single_process(self, remote_local_files):
        import os
        from warnings import warn
        downloader = self._initiate_connection()
        downloader.verbose = self.verbose

        msg_decipher = {0: 'downloaded',
                        1: 'remote_not_exist',
                        2: 'local_exists'}
        outmsg = {k: [] for k in msg_decipher.values()}
        for remote, local in remote_local_files:
            try:
                msg = downloader.download_file(remote, local)
                outmsg[msg_decipher[msg]] += remote,
            except (Exception, KeyboardInterrupt) as e:
                if os.path.isfile(local):
                    os.remove(local)
                    warn('\n\n' + '#' * 40 +
                         f'\nRemoved partially downloaded file {local}\n'
                         + '#' * 40 + '\n')
                raise e

        downloader.close_connection()

        return outmsg

    def _download_multiple_processes(self, remote_local_files, njobs):
        from multiprocessing import Process, Queue
        # TODO: Currently broken. Will be better to use joblib

        paths = remote_local_files
        verbose = self.verbose
        verbose = 1 if verbose == 2 else verbose

        # step = paths.shape[0] // njobs
        processes = []
        split_paths = []
        for i in range(njobs):
            # i0 = i * step
            # i1 = (i+1) * step
            # split_paths += paths[i0:i1],
            split_paths += paths[i:njobs],

        processes = []
        queue = Queue()
        for i in range(njobs):
            p = Process(target=self._download_single_process,
                        args=(split_paths[i]))
            processes += p,
            p.start()

        # FIXME single download no longer returns to queue
        missing_files = []
        for p in processes:
            missing_files += queue.get()
            p.join()

        return missing_files

    def _download_data(self, file_pairs, njobs=1):
        from multiprocessing import cpu_count
        from .utils import DictObject
        from numpy import array

        file_pairs = array(file_pairs)

        ncpus = cpu_count() - 1
        njobs = ncpus if njobs > ncpus else njobs

        self._print(f'Downloading {file_pairs.shape[0]} '
                    f'{self.name} files with {njobs} jobs')

        if njobs == 1:
            out = self._download_single_process(file_pairs)

        # TODO: parallel downloads are currently broken. Could improve
        if njobs > 1:
            out = self._download_multiple_processes(file_pairs, njobs)

        self.download_results = DictObject(out)

    def download_data(self, dates, njobs=1):

        paths = self._make_paths(
            dates,
            self.config['remote']['url'],
            self.config['local_store'])

        self._download_data(paths, njobs=njobs)

    def local_files(self, dates):
        from .utils import is_file_valid
        paths = self._make_paths(
            dates,
            self.config['remote']['url'],
            self.config['local_store'])

        avail = []
        download_pairs = []
        for path_remote, path_local in paths:
            if is_file_valid(path_local):
                avail += path_local,
            # download results contains missing URLs - prevents loop download
            elif path_remote not in self.download_results['remote_not_exist']:
                download_pairs += (path_remote, path_local),

        if download_pairs != []:
            n = len(download_pairs)
            choice = input(f'{n} missing files to download. Continue [y/n]: ')
            if choice.lower().startswith('y'):
                self._download_data(download_pairs)
                return self.local_files(dates)
            else:
                return avail
        elif avail is []:
            raise FileNotFoundError('No files returned for dates')
        else:
            return avail

    @classmethod
    def _make_paths(cls, dates, *date_paths):
        from .utils import Path, URL
        from numpy import array

        path_list = []
        for date_path in date_paths:
            fname_list = date_path[dates]

            if isinstance(fname_list, (Path, URL)):
                fname_list = [fname_list]

            path_list += fname_list,

        if len(date_paths) > 1:
            lengths = set([len(file_list) for file_list in path_list])
            if len(lengths) > 1:
                msg = (
                    'Given paths produce different number of files. '
                    'Paths should produce the same number of output files. \n')
                msg += '\n'.join([p for p in date_paths])
                raise AssertionError(msg)

        path_pairs = array([p for p in zip(*path_list)])

        return path_pairs


class PipeFiles:
    def __init__(self, name, parent, pipe_dict):
        self._parent = parent
        self._funcs = pipe_dict['functions']
        self._data_path = pipe_dict['data_path']

    def __call__(self, dates):
        """
        Gets the file names for the given date range.
        If not present, downloads the files.
        """
        paths = self._parent._make_paths(
            dates,
            self._parent.config.remote.url,
            self._parent.config.local_store,
            self._data_path)

        # TODO: find missing files for pipe and local_store

        return paths

    def _get_file_opener(self, name):
        if name.endswith('.nc'):
            from xarray import open_dataset
            return open_dataset
        else:
            return open

    def _get_file_closer(self, obj):
        from xarray import Dataset, DataArray
        from pandas import Series, DataFrame

        if isinstance(obj, (Dataset, DataArray)):
            return lambda s: obj.to_netcdf(s)
        elif isinstance(obj, (DataFrame, Series)):
            return lambda s: obj.to_hdf(s, key='main')

    def _pipeline(self, file_pairs):
        from . import preprpocess as prep

        for file_raw, file_process in file_pairs:
            opener = self._get_file_opener(file_raw)

            pipeline = [opener] + self._funcs
            processed = prep.apply_process_pipeline(file_raw, pipeline)
            closer = self._get_file_closer(processed)
            closer(file_process)
