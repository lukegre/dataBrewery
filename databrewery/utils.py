from pathlib import Path as _Path_, _windows_flavour, _posix_flavour
import os
import pandas as pd
import xarray as xr
from warnings import warn
import pprint


class BrewError(BaseException):
    pass


class BrewWarning(UserWarning, BaseException):
    pass


class ConfigError(BaseException):
    pass


class PrettyPrinter(pprint.PrettyPrinter):
    def _format(self, object, *args, **kwargs):
        if isinstance(object, (str, Path)):
            object = str(object)
            if len(object) > 80:
                object = object[:80] + '...'
        return pprint.PrettyPrinter._format(self, object, *args, **kwargs)


class ObjectDict(object):
    def __init__(self, d):
        self.__catalog__ = d

        for a, b in d.items():
            if isinstance(b, (list, tuple)):
                setattr(self, a, [ObjectDict(x) if isinstance(x, dict) else x for x in b])
            else:
                setattr(self, a, ObjectDict(b) if isinstance(b, dict) else b)

    def __repr__(self):
        pprinter = PrettyPrinter().pprint
        return pprinter(self.__catalog__)


class DatePath(str):

    def __repr__(self):
        return f"{self.__class__.__name__}({self})"

    def __getitem__(self, getter):
        acceptable = (pd.Timestamp, pd.DatetimeIndex)
        dates = get_dates(getter)

        if isinstance(dates, pd.Timestamp):
            return self.format(t=dates)

        elif isinstance(dates, pd.DatetimeIndex):
            filelist = [self[d] for d in dates]
            if filelist == []:
                warn("No files returned (check input range)", BrewWarning)
            return filelist

        else:
            msg = (f"{type(getter)} not supported for DatePath, only "
                   f"{[str(a).split('.')[-1][:-2] for a in acceptable]}."
                   ).replace("'", '')
            raise TypeError(msg)

    def format(self, *args, **kwargs):
        path_fmt = self.__str__().format(*args, **kwargs)
        return self.__class__(path_fmt)

    def today(self):
        return self[pd.Timestamp.today()]


class URL(DatePath):
    @property
    def parsed(self):
        from urllib.parse import urlparse
        return urlparse(str(self))


class Path(DatePath):

    def __init__(self, string):
        import pathlib
        path_funcs = ['parent', 'name', 'glob', 'exists',
                      'mkdir', 'is_dir', 'is_file']

        for func_name in path_funcs:
            func = getattr(pathlib.Path(self).expanduser(), func_name)
            setattr(self, func_name, func)

    @property
    def globbed(self):
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



class NetCDFloader:
    """
    A class that loads netCDF data for a given date, but will
    not load the data if already loaded.
    Takes a DataBrew.Path object and a set of functions can be applied
    to the loaded netCDF.
    """
    def __init__(self, db_name, prep_funcs=[], varlist=[],
                 decode_times=True, verbose=1):
        self.name = db_name
        self.data = None
        self.file = ''
        self.prep_funcs = prep_funcs
        self.varlist = varlist
        self.decode_times = decode_times
        self.verbose = verbose

    def get_data(self, time, raise_error=True):
        fname = self._get_nearest_fname(time)
        if fname is None:
            return DummyNetCDF()
        if fname != self.file:
            if self.verbose == 1:
                print('.', end='')
            elif self.verbose == 2:
                print(f'Loading: {fname}')
            self.file = fname
            self.data = self._load_netcdf(fname, time=time)
        return self.data

    def _get_nearest_fname(self, time, max_dist_time=5):
        fname = self.name.set_date(time)
        dt = 1

        if '*' in str(fname):
            from glob import glob
            flist = glob(str(fname))
            return flist

        while (not fname.exists()) & (dt <= max_dist_time):
            fname = self.name.set_date(time + pd.Timedelta(f'{dt}D'))
            if fname.exists():
                continue
            else:
                fname = self.name.set_date(time - pd.Timedelta(f'{dt}D'))
            dt += 1

        if not fname.exists():
            return None

        return fname

    def _load_netcdf(self, fname, time=None):
        from xarray import open_dataset
        if not isinstance(fname, (list, str)):
            fname = str(fname)

        xds = xr.open_mfdataset(fname,
                                decode_times=self.decode_times,
                                combine='nested',
                                concat_dim='time',
                                parallel=True)
        if self.varlist:
            var_key = self.varlist
        else:
            var_key = list(xds.data_vars.keys())
        xds = xds[var_key]

        if (time is not None) and ('time' in xds.dims):
            if isinstance(time, (int, float)):
                xds = xds.isel(time=time).drop('time')
            else:
                xds = xds.mean('time')

        if self.prep_funcs:
            xds = self._apply_process_pipeline(xds, self.prep_funcs)

        return xds.load()

    def _apply_process_pipeline(self, xds, pipe):
        xds.load()
        for func in pipe:
            xds = func(xds)

        return xds


class DummyNetCDF:
    def sel_points(self, *args, **kwargs):
        return pd.Series([None])

    def __getitem__(self, key):
        return DummyNetCDF()


def get_dates(date_like):
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
        raise ValueError('Something is wrong with the date input. Must be '
                         'str(YYYY-MM-DD) or Timestamp or DatetimeIndex')

    return dates


def slice_to_date_range(slice_obj):
    import pandas as pd
    t0, t1, ts = slice_obj.start, slice_obj.stop, slice_obj.step

    if t0 is None:
        raise IndexError('You cannot have time slices that are none')
    if t1 is None:
        t1 = pd.Timestamp.today()
    if ts is None:
        ts = '1D'
    if not isinstance(ts, str):
        raise IndexError('The slice step must be a frequency string '
                         'parsable by pd.Timestamp')

    t0, t1 = [pd.Timestamp(str(t)) for t in [t0, t1]]
    date_range = pd.date_range(t0, t1, freq=ts)

    return date_range
