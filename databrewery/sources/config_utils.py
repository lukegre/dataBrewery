import os

def load_config():
    import os 
    import yaml
    
    package_path = '/'.join(os.path.realpath(__file__).split('/')[:-2])
    config_path = os.path.join(package_path, 'config.yaml')
    
    with open(config_path) as file:
        config = yaml.safe_load(file)
    
    for key in config:
        if key is 'paths':
            continue
        
        if isinstance(config[key], dict):
            config[key]['functions'] = config[key].get(
                'functions', 
                ['download', 'standardise_025_dialy_stored_monthly'])
        
            config[key]['start'] = config[key].get('start', '2018-01-01')
            config[key]['end']   = config[key].get('end',   '2018-12-31')

    return config


class Paths:
    None
    def _make_dict(self):
        objs = dir(self)
        paths = {}
        for item in objs:
            if not item.startswith('_'):
                paths[item] = getattr(self, item)
        return paths
    
    def __repr__(self):
        paths = self._make_dict()
        txt = "\n".join([f"{k: <8}{v}" for k,v in paths.items()])
        
        return txt
    
paths = Paths()
paths.base = load_config()['abspath']
paths.raw  = os.path.join(paths.base, "raw/{source}/{{year:04d}}/{{month:02d}}/{fname}")
paths.grid = os.path.join(paths.base, "{{res}}/{source}_{{var}}/{{year:04d}}/{source}_{{res}}_{{var}}_{{year:04d}}{{month:02d}}.nc")


locals().update(load_config())


# daystar = '98765'