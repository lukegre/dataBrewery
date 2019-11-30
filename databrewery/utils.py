from pathlib import Path as _Path_, _windows_flavour, _posix_flavour
import os
import pandas as pd


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
        self._keys = dictionary.keys
        self._values = dictionary.values
        self._line_len = 71
        self._split_chars = '/-_'
        
        for key in dictionary.keys():
            setattr(self, key, dictionary[key])

    def _items(self):
        return self._dict.items()

    def __getitem__(self, key):

            return self._dict[key]

    def __contains__(self, key):
        return key in self._keys()

    def __repr__(self):
        return self._dict.__repr__()

    def __str__(self):
        if self._dict == {}:
            return ""
        tab = max([len(str(key)) for key in self._dict]) + 2
        txt = ""
        for key in self._keys():
            value = self[key]
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
            counter = m // self._line_len
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

    def __getitem__(self, getter):
        acceptable = (pd.Timestamp, pd.DatetimeIndex)

        if isinstance(getter, pd.Timestamp):
            return self.set_date(getter).str

        elif isinstance(getter, pd.DatetimeIndex):
            return [self[d] for d in getter]
        
        msg = (f"{type(getter)} not supported for KegPath, only "
               f"{[str(a).split('.')[-1][:-2] for a in acceptable]}."
               ).replace("'", '')
        raise TypeError(msg)

    def today(self):
        return self[pd.Timestamp.today()]
    
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

