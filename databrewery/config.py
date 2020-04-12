from importlib import import_module

import validators
from schema import And, Optional, Or, Schema, Use

from .utils import URL, Path


class ConfigError(BaseException):
    pass


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
        modules += (get_module_from_string(name),)

    return modules


def check_datepaths(record):
    from .utils import make_date_path_pairs
    import pandas as pd

    random_dates = pd.DatetimeIndex(
        ['2010-04-03', '2010-03-23', '2014-01-01', '2014-01-02']
    )

    paths = [record['remote']['url'], record['local_store']]

    if 'pipelines' in record:
        for key in record['pipelines']:
            pipe = record['pipelines'][key]
            paths += (pipe['data_path'],)
    try:
        make_date_path_pairs(random_dates, *paths)
    except AssertionError:
        raise ConfigError(
            'The given paths in the config file do not produce '
            'the same number of output files; e.g. there may be '
            'more URLs than LOCAL_PATHSs. Please check the date '
            'formatting of the following paths: \n' + '\n'.join(paths)
        )


def validate_catalog(catalog_dict):

    validated_catalog = {}
    for key in catalog_dict:
        record = catalog_dict[key]
        valid_record = schema.validate(record)
        validated_catalog[key] = valid_record

        check_datepaths(valid_record)

    return validated_catalog


def read_catalog(catalog_fname):
    import yaml

    cat_dict = yaml.safe_load(open(catalog_fname))

    path_dict = {k: v for k, v in cat_dict.items() if k.upper() == k}

    raw = open(catalog_fname).read()
    for key in path_dict:
        raw = raw.replace(f'{{{key}}}', path_dict[key])

    catalog_dict = yaml.safe_load(raw)
    for key in path_dict:
        catalog_dict.pop(key)

    validated = validate_catalog(catalog_dict)

    return validated


schema = Schema(
    {
        'description': And(
            str,
            lambda s: len(s) > 30,
            error='Description length must be > 40 characters',
        ),
        'doi': And(
            validators.url,
            str,
            error='DOI must be a URL linking to the orginal paper',
        ),
        'variables': list,
        'remote': {
            'url': Use(URL),
            Optional('username'): str,
            Optional(Or('service', 'password', only_one=True)): str,
            Optional('port'): int,
        },
        'local_store': Use(Path),
        Optional('pipelines'): {
            str: {
                'data_path': Use(Path, error='data_path must be a valid path'),
                'functions': Use(get_modules_from_list),
            }
        },
    }
)


if __name__ == '__main__':
    pass
