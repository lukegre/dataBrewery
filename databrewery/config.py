from os import path as _path


path = _path.join(_path.expanduser('~'), '.dataBrewery.yaml')


def read_config_as_dict(filepath):
    from yaml import safe_load
    from collections import defaultdict

    file = open(filepath)
    config_dict = safe_load(file)

    return defaultdict(defaultdict, config_dict)


class dataBreweryConfig:

    def __init__(self, config_file_path=path):
        from .utils.core import Object
        import os

        config_dict = self._read_config_as_dict(config_file_path)

        self.filepath = os.path.realpath(config_file_path)
        self.sources = Object(config_dict)
        self.keyword = self._generate_keyword_obj(config_dict)

    def __repr__(self):
        txt = f"dataBreweryConfig ({self.filepath})\n"
        txt += "=" * (len(txt)-1)
        txt += self.sources.__repr__()
        return txt

    @staticmethod
    def _read_config_as_dict(filepath):
        from yaml import safe_load

        file = open(filepath)
        config_dict = safe_load(file)

        return config_dict

    @staticmethod
    def _generate_keyword_obj(config_dict):
        from .utils.core import Object

        def generate_keyword_dict(config_dict):
            from collections import defaultdict
            keywords = defaultdict(list)
            for source in config_dict:
                kw = config_dict[source].get('keywords', [])
                for key in kw:
                    keywords[key] += source,
            return keywords

        keyword_dict = generate_keyword_dict(config_dict)
        keyword_obj = {}
        for kw in keyword_dict:
            keyword_obj[kw] = {}
            for src in keyword_dict[kw]:
                keyword_obj[kw][src] = config_dict[src]
        return Object(keyword_obj)
