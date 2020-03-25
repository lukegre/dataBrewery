import os
from importlib import import_module
import validators
import pprint
from schema import Schema, Optional, And, Use
from pathlib import Path as _Path_, _windows_flavour, _posix_flavour
import pandas as pd


class Catalog(object):
    def __init__(self, d):
        self.__catalog__ = d

        for a, b in d.items():
            if isinstance(b, (list, tuple)):
                setattr(self, a, [Catalog(x) if isinstance(x, dict) else x for x in b])
            else:
                setattr(self, a, Catalog(b) if isinstance(b, dict) else b)

    def __repr__(self):
        printer = PrettyPrinter().pformat
        return printer(self.__catalog__)


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
                warn("No files returned (check input range)", NameError)
            return list(set(filelist))

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


class PrettyPrinter(pprint.PrettyPrinter):
    def _format(self, object, *args, **kwargs):
        if isinstance(object, (str, Path, URL)):
            object = str(object)
            if len(object) > 80:
                object = object[:80] + '...'
        return pprint.PrettyPrinter._format(self, object, *args, **kwargs)


def path_has_write_access(path):
    from pathlib import Path
    path = Path(path)
    writable = os.access(path, os.W_OK)
    parent = path.parent

    if writable:
        return True
    elif not parent.is_mount():
        result = path_has_write_access(path.parent)
    else:
        return False
    return result


def get_modules_from_list(list_of_module_names):
    def get_module_from_string(module_name_str):

        mod = import_module(module_name_str.split('.')[0])

        for sub in module_name_str.split('.')[1:]:
            if hasattr(mod, sub):
                mod = getattr(mod, sub)
            else:
                raise ImportError(f'`{module_name_str}` does not exist')
        return mod

    modules = []
    for name in list_of_module_names:
        modules += get_module_from_string(name),

    return modules


def validate_catalog(catalog_dict):
    validated_catalog = {}
    for key in catalog_dict:
        record = catalog_dict[key]
        validated_catalog[key] = schema.validate(record)

    return validated_catalog


def read_catalog(catalog_fname):
    import yaml

    catalog_dict = yaml.full_load(open(catalog_fname))
    validated = validate_catalog(catalog_dict)

    return validated


schema = Schema({
        'description': str,
        'doi': And(validators.url, str, error='DOI must be a URL'),
        'variables': list,
        'remote': {'url': Use(URL),
                   Optional('keyring'): {'service': str, 'username': str},
                   #  Optional('login'): {'username': str, 'password': str},
                   Optional('port'): int},
        'local_store': Use(Path),  # And(str, path_has_write_access),
        Optional('pipelines'): {str: {'data_path': Use(Path),
                                      'functions': Use(get_modules_from_list)}}
        })


if __name__ == "__main__":
    pass
