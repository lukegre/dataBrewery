from importlib import import_module
import validators
from schema import Schema, Optional, And, Use, Or
from . utils import Path, URL


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
                   Optional('login'): {
                       'username': str,
                                       Or('service', 'password', only_one=True): str},
                   Optional('port'): int},
        'local_store': Use(Path),
        Optional('pipelines'): {str: {'data_path': Use(Path),
                                      'functions': Use(get_modules_from_list)}}
        })


if __name__ == "__main__":
    pass
