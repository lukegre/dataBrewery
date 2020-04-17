import pprint
from warnings import warn

import pandas as pd


class DictObject(object):
    """
    Convert a dictionary item to an object that can be browsed interactively
    """

    def __init__(self, d):
        """
        Converts dictionary to DictObject. You can use dictionary notation
        on the DictObject class.

        Parameters
        ==========
        d: dict
            can be a nested dictionary
        """

        for a, b in d.items():
            if isinstance(b, (list, tuple)):
                setattr(
                    self,
                    a,
                    [DictObject(x) if isinstance(x, dict) else x for x in b],
                )
            else:
                setattr(self, a, DictObject(b) if isinstance(b, dict) else b)

    def __repr__(self):
        return pprint.pformat(self.__dict__)

    def __getitem__(self, key):
        if isinstance(key, str):
            return self.__dict__[key]
        else:
            raise IndexError(
                'The given index key type is not supported. '
                'Only string accepted'
            )

    def __iter__(self):
        return self.__dict__.__iter__()

    def __next__(self):
        return self.__dict__.__next__()


class DatePath(str):
    """
    Base class for other functions that convert a specially formatted path
    string into a path where dates are filled out.
    """

    def __repr__(self):
        return f'{self.__class__.__name__}({self})'

    def __getitem__(self, getter):
        acceptable = (pd.Timestamp, pd.DatetimeIndex)
        dates = get_dates(getter)

        if isinstance(dates, pd.Timestamp):
            return self.format(t=dates)

        elif isinstance(dates, pd.DatetimeIndex):
            filelist = [self[d] for d in dates]
            uniquelist = []
            for d in filelist:
                if d not in uniquelist:
                    uniquelist += (d,)
            filelist = uniquelist
            if filelist == []:
                warn('No files returned (check input range)', UserWarning)
            return filelist

        else:
            msg = (
                f'{type(getter)} not supported for DatePath, only '
                f"{[str(a).split('.')[-1][:-2] for a in acceptable]}."
            ).replace("'", '')
            raise TypeError(msg)

    def format(self, *args, **kwargs):
        path_fmt = self.__str__().format(*args, **kwargs)
        return self.__class__(path_fmt)

    def today(self):
        return self[pd.Timestamp.today()]


class URL(DatePath):
    """
    Very similar to DatePath but has the *parsed* property that
    returns a URL parsed object
    """

    @property
    def parsed(self):
        from urllib.parse import urlparse

        return urlparse(str(self))


class Path(DatePath):
    """
    A special Path class that has some functionality from the pathlib.Path
    class. The difference from pathlib.Path, is that it accepts a special
    date formatting
    """

    def __init__(self, string):
        """
        Create a DatePath instance

        Parameters
        ==========
        string: str
            A string that represents a local path. Can have ~ to represent
            HOME or can have a date format placeholder. The date formatter
            must have the following structure = {t:<datetime formats>}. e.g.
            {t:%Y%m%d} will become 20200228 if
            Path.format(t=pd.Timestamp("2020-02-28")) or indexed with
            Path["2020-02-28"]. Also supports pd.Timestamp, pd.DatetimeIndex,
            slice of date range in string or pd.Timestamp, where the step is a
            string denoting the time step.
        """
        from pathlib import _windows_flavour, _posix_flavour, Path as _Path
        import os

        self._flavour = _windows_flavour if os.name == 'nt' else _posix_flavour

        path_funcs = [
            'parent',
            'name',
            'glob',
            'exists',
            'mkdir',
            'is_dir',
            'is_file',
        ]

        for func_name in path_funcs:
            func = getattr(_Path(self).expanduser(), func_name)
            setattr(self, func_name, func)

        assert self.is_writable(), 'Given path is not writable'

    @property
    def globbed(self):
        """
        IF the base input contains an asterisk (*), the * will be replaced
        with matching files.
        """
        import pathlib

        path = pathlib.Path(self).expanduser()
        parent = path.parent
        child = path.name
        globbed = parent.glob(str(child))
        flist = [Path(f) for f in globbed]
        if flist is []:
            return None
        elif len(flist) == 1:
            return flist[0]
        else:
            return flist

    def is_writable(self):
        """
        Checks if the given path (at any level) can be written to.
        For example, if `/` is given, will return False (unless in sudo).
        """
        import pathlib
        import os

        path = pathlib.Path(self)
        writable = os.access(path, os.W_OK)
        parent = path.parent

        if writable:
            return True
        elif not os.path.ismount(str(parent)):
            parent = Path(parent)
            result = parent.is_writable()
        else:
            return False
        return result


def get_dates(date_like):
    """
    A helper function for DatePath that will always return a pandas.Timestamp
    or pandas.DatetimeIndex or return an error. Used for slicing with date-like
    strings
    """
    from pandas import Timestamp, DatetimeIndex

    if isinstance(date_like, (Timestamp, DatetimeIndex)):
        dates = date_like
    elif isinstance(date_like, str):
        dates = Timestamp(date_like)
    elif isinstance(date_like, slice):
        dates = slice_to_date_range(date_like)
    elif isinstance(date_like, (list, tuple, set)):
        dates = pd.DatetimeIndex([get_dates(d) for d in date_like])
    else:
        raise ValueError(
            'Something is wrong with the date input. Must be '
            'str(YYYY-MM-DD) or Timestamp or DatetimeIndex'
        )

    return dates


def slice_to_date_range(slice_obj):
    """
    Helper function for get_dates that converts a slice to a
    pandas.DatetimeIndex range.
    """
    import pandas as pd

    t0, t1, ts = slice_obj.start, slice_obj.stop, slice_obj.step

    if t0 is None:
        raise IndexError('You cannot have time slices that are none')
    if t1 is None:
        t1 = pd.Timestamp.today()
    if ts is None:
        ts = '1D'
    if not isinstance(ts, str):
        raise IndexError(
            'The slice step must be a frequency string '
            'parsable by pd.Timestamp'
        )

    t0, t1 = [pd.Timestamp(str(t)) for t in [t0, t1]]
    date_range = pd.date_range(t0, t1, freq=ts)

    return date_range


def is_file_valid(local_path):
    """
    Helper function that checks if a file can be opened.
    If valid, returns True, else False.

    Currently supports netCDF or Zip files.
    """
    from os.path import isfile

    if not isfile(local_path):
        return False

    # has an opener been passed, if not assumes file is valid
    if local_path.endswith('.nc'):
        from netCDF4 import Dataset as opener

        error = OSError
    elif local_path.endswith('.zip'):
        from zipfile import ZipFile as opener, BadZipFile as error
    else:
        error = BaseException

        def opener(p):
            return None  # dummy opener

    # tries to open the path, if it fails, not valid, if it passes, valid
    try:
        with opener(local_path):
            return True
    except error:
        return False


def make_date_path_pairs(dates, *date_paths):
    """
    Helper function that creates paired paths from a given date range and
    DatePath type objects.

    Parameters
    ==========
    dates: list-like
        Must be an iterable that contains pd.Timestamps
    date_paths: DatePath objects
        can be any number of DatePath objects that will return a formatted
        path string if sliced with a date-like string or pd.Timestamp. The
        date_paths must have the same formatting so that the same number of
        file paths are returned.

    Returns
    =======
    date_path_pairs
        Couplets (if two date paths) of file paths that have the same date.
    """
    from .utils import Path, URL
    from numpy import array

    path_list = []
    for date_path in date_paths:
        fname_list = date_path[dates]

        if isinstance(fname_list, (Path, URL)):
            fname_list = [fname_list]

        path_list += (fname_list,)

    if len(date_paths) > 1:
        lengths = set([len(file_list) for file_list in path_list])
        if len(lengths) > 1:
            msg = (
                'Given paths produce different number of files. '
                'Paths should produce the same number of output files. \n'
            )
            msg += '\n'.join([p for p in date_paths])
            raise AssertionError(msg)

    path_pairs = array([p for p in zip(*path_list)])

    return path_pairs
