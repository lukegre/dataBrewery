
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
        connect = self._downloader(host, **self.config.remote.login.__dict__)

        return connect

    def _download_single_process(self, remote_local_files):
        downloader = self._initiate_connection()
        downloader.verbose = self.verbose

        msg_decipher = {0: 'downloaded',
                        1: 'remote_not_exist',
                        2: 'local_exists'}
        outmsg = {k: [] for k in msg_decipher.values()}
        for remote, local in remote_local_files:
            msg = downloader.download_file(remote, local)
            outmsg[msg_decipher[msg]] += remote,

        downloader.close_connection()

        return outmsg

    def _download_multiple_processes(self, remote_local_files, njobs):
        from multiprocessing import Process, Queue

        paths = remote_local_files
        verbose = self.verbose
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

    def _make_paths(self, dates, *date_paths):
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
                    'Ensure that the paths are compatible. \n'
                    '\n'.join([p for p in date_paths]))
                raise AssertionError(msg)

        path_pairs = array([p for p in zip(*path_list)])

        return path_pairs

    def _download_data(self, file_pairs, njobs=1):
        from multiprocessing import cpu_count
        from .utils import DictObject
        from numpy import array

        file_pairs = array(file_pairs)

        ncpus = cpu_count() - 1
        njobs = ncpus if njobs > ncpus else njobs

        self._print(f'Downloading {file_pairs.shape[0]} {self.name} files with {njobs} jobs')

        if njobs == 1:
            out = self._download_single_process(file_pairs)
        if njobs > 1:
            out = self._download_multiple_processes(file_pairs, njobs)

        self.download_results = DictObject(out)

    def download_data(self, dates, njobs=1):

        paths = self._make_paths(
            dates,
            self.config['remote']['url'],
            self.config['local_store'])

        self._download_data(paths, njobs=njobs)

