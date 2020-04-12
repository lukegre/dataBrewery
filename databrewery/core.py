from .record import Record


class Catalog:
    def __init__(self, catalog_file='./config_template.yaml', verbose=1):
        from .config import read_catalog

        self.verbose = verbose
        self._config_dict = read_catalog(catalog_file)
        self._create_records()
        self.VARS = VariableAccess(self)

    def _create_records(self):

        for key in self._config_dict.keys():
            record = Record(key, self._config_dict[key], verbose=self.verbose)
            setattr(self, key, record)

    def __str__(self):
        import re

        out = ''
        txt = 'Your Catalog contains the following Records'
        b = '='
        out += f'{txt}\n{b:=>{len(txt)}}\n'
        for key in self._config_dict.keys():
            record = getattr(self, key, None)
            if record is None:
                continue

            scheme = record.config.remote.url.parsed.scheme.upper()
            variables = re.sub(r"['()]", '', str(record.config.variables))
            out += f'{key: <15} {scheme: <8}{variables}\n'

        out += '\nAccess all local paths via keywords through dataBrewery.MENU'
        return out


class VariableAccess:
    def __init__(self, catalog):
        from collections import defaultdict
        from .utils import DictObject

        record_names = catalog._config_dict.keys()
        records = [getattr(catalog, k) for k in record_names]

        keywords = defaultdict(dict)
        for record in records:
            for kw in record.config.variables:
                keywords[kw].update({record.name: record})

        self._kw = keywords
        for kw in keywords:
            setattr(self, kw, DictObject(keywords[kw]))

    def __repr__(self):
        out = ''
        keys = sorted(self._kw.keys())
        out += 'Record NAME       DATASET\n'
        rule = '-' * len(out) + '\n'
        out += rule
        for key in keys:
            val = self._kw[key]
            out += f'{key.upper(): <20}'
            for v in val:
                out += f'{v}, '
            out += '\n'
        out += rule
        return out
