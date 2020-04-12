class Record:
    """
    Stores the information for an entry in the catalog file
    """

    def __init__(self, record_name, config_dict, verbose=2):
        """
        Should be called by the Catalog class as requires a
        preformatted catalog dictionary (config_dict)
        """
        from .utils import DictObject

        self.name = record_name
        self.debug = True if verbose == 2 else False
        self.verbose = verbose

        self.description = config_dict.pop('description')
        self.doi = config_dict.pop('doi')

        self.config = DictObject(config_dict)

        if hasattr(self.config, 'pipelines'):
            for key in self.config.pipelines:
                pipe = PipeFiles(key, self, self.config.pipelines[key])
                setattr(self, key, pipe)
        self._reset_download_results()

    def _reset_download_results(self):
        self.download_results = {
            'remote_not_exist': [],
            'local_exists': [],
            'downloaded': [],
        }

    def __str__(self):
        return str(self.config)

    def _print(self, *msg):
        if self.verbose >= 1:
            print(*msg)

    def _initiate_connection(self):
        """
        Function makes a connection to the server. This is done per download
        thread and NOT per file. This approach is quicker. The host type and
        download protocol is also determined here.

        Returns a downloader object that can then download specified files
        from the server
        """
        from .download import determine_connection_type

        url = self.config['remote']['url']

        self._downloader = determine_connection_type(url)

        host = url.parsed.netloc
        login_dict = self.config.remote.__dict__.copy()
        login_dict.pop('url')
        connect = self._downloader(host, **login_dict)

        return connect

    def _download_single_process(self, remote_local_files):
        """
        Downloads files on a single process using a db.Downloader instance.

        Parameters
        ----------
        remote_local_files: list
            a list of file pairs, where each pair is the remote and local
            save paths to the files.

        Returns
        -------
        download_status: dict
            a dictionary with files assigned to the following categories:
            - downloaded
            - remote_not_exist
            - local_exists
        """
        import os
        from warnings import warn

        downloader = self._initiate_connection()
        downloader.verbose = self.verbose

        msg_decipher = {
            0: 'downloaded',
            1: 'remote_not_exist',
            2: 'local_exists',
        }
        download_status = {k: [] for k in msg_decipher.values()}
        for remote, local in remote_local_files:
            # download_file returns a code that is described by the
            # msg_decipher codes above
            try:
                msg = downloader.download_file(remote, local)
                download_status[msg_decipher[msg]] += (local,)
            except (Exception, KeyboardInterrupt) as error:
                # catches any exception so that file is deleted if incomplete
                if os.path.isfile(local):
                    os.remove(local)
                    warn(
                        '\n\n'
                        + '#' * 40
                        + f'\nRemoved partially downloaded file {local}\n'
                        + '#' * 40
                        + '\n'
                    )
                # downloader connection closed to avoid too many connections
                downloader.close_connection()
                # raises caught error at the end
                raise error

        # close connection at the end of the downloading
        downloader.close_connection()

        return download_status

    def _download_multiple_processes(self, remote_local_files, njobs):
        from multiprocessing import Process, Queue

        # TODO: Will be better to use joblib and

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
            split_paths += (paths[i:njobs],)

        processes = []
        queue = Queue()
        for i in range(njobs):
            p = Process(
                target=self._download_single_process, args=(split_paths[i])
            )
            processes += (p,)
            p.start()

        # TODO single download no longer returns to queue
        missing_files = []
        for p in processes:
            missing_files += queue.get()
            p.join()

        return missing_files

    def _download_data(self, file_pairs, njobs=1):
        from multiprocessing import cpu_count
        from numpy import array

        file_pairs = array(file_pairs)

        ncpus = cpu_count() - 1
        njobs = ncpus if njobs > ncpus else njobs

        self._print(
            f'Downloading {file_pairs.shape[0]} '
            f'{self.name} files with {njobs} jobs'
        )

        if njobs == 1:
            out = self._download_single_process(file_pairs)

        # FIXME: parallel downloads are currently broken.
        if njobs > 1:
            out = self._download_multiple_processes(file_pairs, njobs)

        self.download_results = out

    def download_data(self, dates, njobs=1):
        """
        Download files for given dates.

        Parameters
        ==========
        dates: date-like string or object
            Can be one of the following:
                1) a single datelike string or pandas.Timestamp
                2) slice of datelike string or pandas.Timestamp
                3) a pandas.DatetimeIndex object made with pandas.date_range
        njobs: int
            number of parallel connections to download with. Be carefuly,
            some servers do not accept a large amount of connections.

        Returns
        =======
        download_results: dict
            a dictionary containing information about downloaded, existing
            and files that could not be downloaded.
        """
        from .utils import make_date_path_pairs

        paths = make_date_path_pairs(
            dates, self.config['remote']['url'], self.config['local_store']
        )

        self._download_data(paths, njobs=njobs)

        return self.download_results

    def local_files(self, dates, njobs=1, auto_download=False):
        """
        A wrapper around download_data that returns the names of
        local files. If the local files do not exist, then tries
        to download them using download_data function.

        Parameters
        ==========
        dates: date-like string or object
            Can be one of the following:
                1) a single datelike string or pandas.Timestamp
                2) slice of datelike string or pandas.Timestamp
                3) a pandas.DatetimeIndex object made with pandas.date_range
        njobs: int (1)
            number of parallel connections to download with. Be carefuly,
            some servers do not accept a large amount of connections.
        auto_download: bool (False)
            will automatically download files if set to True, if False
            will ask for confirmation

        Returns
        =======
        local_file_names: list
            a list of file names of files that exist locally. Note that
            these files are exact replicas of the remote files and no
            processing has been applied.
        """
        from .utils import is_file_valid, make_date_path_pairs, DictObject

        paths = make_date_path_pairs(
            dates, self.config['remote']['url'], self.config['local_store']
        )

        # the current structure is perhaps not the best for a DAG workflow
        # this is a slightly hacky solution to the problem
        exists_locally = []
        download_pairs = []
        for path_remote, path_local in paths:
            if is_file_valid(path_local):
                self.download_results['local_exists'] += (path_local,)
                exists_locally += (path_local,)
            # download results contains missing URLs - prevents loop download
            elif path_remote not in self.download_results['remote_not_exist']:
                download_pairs += ((path_remote, path_local),)

        if download_pairs != []:
            n = len(download_pairs)
            if auto_download:
                choice = 'y'
            else:
                choice = input(f'{n} missing files. Download? [y/n]: ')
            if choice.lower().startswith('y'):
                self._download_data(download_pairs, njobs)
                return self.local_files(dates)
            else:
                return exists_locally
        elif exists_locally is []:
            raise FileNotFoundError('No files returned for dates')
        else:
            results = self.download_results
            summary = (
                f'\nDownload report for {self.name}:\n'
                + '-' * 22
                + '\n'
                + '\n'.join(
                    [f'{k: <18} {len(v): >3}' for k, v in results.items()]
                )
                + '\n'
                + '=' * 22
                + '\n'
            )
            if self.verbose == 1:
                print(summary)
            elif self.verbose > 1:
                print(summary)
                print(DictObject(results))
            self._reset_download_results()

            return exists_locally


class PipeFiles:
    """
    UNDER DEVELOPMENT

    Will be the class that is used to process the pipelines.
    Still need to think about how custom functions can be
    incorporated into the pipeline
    """

    def __init__(self, name, parent, pipe_dict):
        self._parent = parent
        self._funcs = pipe_dict['functions']
        self._data_path = pipe_dict['data_path']

    def __call__(self, dates):
        """
        Gets the file names for the given date range.
        If not present, downloads the files.
        """
        from .utils import make_date_path_pairs

        paths = make_date_path_pairs(
            dates,
            self._parent.config.remote.url,
            self._parent.config.local_store,
            self._data_path,
        )

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
