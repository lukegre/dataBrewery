from . record import Record


class Catalog:
    def __init__(self, catalog_file='./config.yaml', verbose=1):
        from . config import read_catalog

        self.verbose = verbose
        self._config_dict = read_catalog(catalog_file)
        self._create_records()
        # self.taps = VariableCollection(self, 'path')

    def _create_records(self):

        for key in self._config_dict.keys():
            record = Record(key, self._config_dict[key], verbose=self.verbose)
            setattr(self, key, record)

    def __str__(self):
        import re

        out = ""
        txt = 'Your Brewery contains the following Records'
        b = "="
        out += f"{txt}\n{b:=>{len(txt)}}\n"
        for key in self._config_dict.keys():
            barrel = getattr(self, key, None)
            if barrel is None:
                continue

            dates = f"{barrel.date.start} : {barrel.date.end}"
            scheme = barrel.url.parsed.scheme.upper()
            keywords = re.sub(r"['()]", "", str(barrel.keywords))
            out += f"{key: <15}{dates}   {scheme: <8}{keywords}\n"

        out += "\nAccess all local paths via keywords through dataBrewery.MENU"
        return out


class VariableCollection:
    def __init__(self, craft_brewery, attr):
        from collections import defaultdict
        from .utils import DictObject

        barrel_names = craft_brewery._config_dict.keys()
        barrels = [getattr(craft_brewery, k) for k in barrel_names]

        keywords = defaultdict(dict)
        for barrel in barrels:
            for kw in barrel.keywords:
                keywords[kw].update({barrel.name: getattr(barrel, attr)})

        self._kw = keywords
        for kw in keywords:
            setattr(self, kw, DictObject(keywords[kw]))

    def __repr__(self):
        out = ""
        keys = sorted(self._kw.keys())
        out += "Record NAME       DATASET\n"
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
