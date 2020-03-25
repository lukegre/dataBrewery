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


class Path(_Path_):
    _flavour = _windows_flavour if os.name == 'nt' else _posix_flavour
    @property
    def str(self):
        return self.expanduser().__str__()

    def set_date(self, date):
        if not date:
            date = pd.Timestamp.today()
        return self.format(t=date)

    def format(self, *args, **kwargs):
        path_fmt = self.str.format(*args, **kwargs)
        return Path(path_fmt)

    def _process_slice_to_date_range(self, slice_obj):
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

    def __getitem__(self, getter):
        acceptable = (pd.Timestamp, pd.DatetimeIndex)

        # first try to convert string to Timestamp
        if isinstance(getter, str):
            getter = pd.Timestamp(getter)

        if isinstance(getter, slice):
            getter = self._process_slice_to_date_range(getter)

        if isinstance(getter, pd.Timestamp):
            return self.set_date(getter)

        elif isinstance(getter, pd.DatetimeIndex):
            filelist = [self[d].str for d in getter]  # if self[d].exists()]
            if filelist == []:
                warn("No files returned (check input range)", BrewWarning)
            return filelist

        msg = (f"{type(getter)} not supported for dPath, only "
               f"{[str(a).split('.')[-1][:-2] for a in acceptable]}."
               ).replace("'", '')
        raise TypeError(msg)

    def today(self):
        return self[pd.Timestamp.today()]

    @property
    def str(self):
        return str(self)

    @property
    def globbed(self):
        parent = self.parent
        child = self.name
        globbed = parent.glob(str(child))
        flist = list(globbed)
        if flist is []:
            return None
        elif len(flist) == 1:
            return flist[0]
        else:
            return flist

    @property
    def parsed(self):
        from urllib.parse import urlparse
        return urlparse(self.str)


class URL(str):
    def __getitem__(self, getter):
        acceptable = (pd.Timestamp, pd.DatetimeIndex,)

        if isinstance(getter, pd.Timestamp):
            return self.format(t=getter)

        elif isinstance(getter, pd.DatetimeIndex):
            return [self[d] for d in getter]

        elif isinstance(getter, slice):
            return self.__str__()[getter]

        msg = f"{type(getter)} not supported for URL, only {acceptable}."
        raise TypeError(msg)

    def today(self):
        return self[pd.Timestamp.today()]

    def year(self, year):
        from numpy import unique
        dates = pd.date_range(f"{year}-01-01", f"{year}-12-31")
        return unique(self[dates])

    @property
    def parsed(self):
        from urllib.parse import urlparse
        return urlparse(self)


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
