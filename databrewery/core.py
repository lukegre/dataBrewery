from .record import Record


class Catalog:
    """ EXAMPLE CATALOG STRUCTURE

    # ####### Path placeholder #########
    # must be all caps - will be replaced if in curly braces {ANY_CAPS_NAME}
    # Used as replacements in other entries that will be replaced at runtime
    # ~ (tilde) can be used to represent the $HOME directory
    DATA_PATH: ~/Desktop

    # ####### Record entry ##########
    entry_name:
        description:
            the description tells the user about the product and can be
            written over mutliple lines
        doi: "https://doi.org/must_add_doi_URL_to_pulication
        variables:  # makes it easier for the user to find the data they want
            - variable_names
            - used_for_variable_access
        remote:
            # URL is where the data is downloaded from.
            # These URLs take a special syntax where {t:} denotes a date
            # use the datetime syntax in python to fill these placeholders
            # %Y = four digit year; %y = two digit year;
            # %m = two digit month;
            # %j = three digit day of the year;
            # %d = two digit day of the month
            url: "http://data.com/path/year_folder_{t:%Y}/fname_{t:%Y%m}*.nc"
            service: "CMEMS"  # the name of the service on the system keyring
            username: "lgregor1"  # for the service
            password: "cannot be defined if service is defined"
            port: 22001  # optional port number if required
        # local_store is where data is cloned to
        # remote.url and local_store must result in the same number of files
        local_store: "{DATA_PATH}/path/year_folder_{t:%Y}/fname_{t:%Y%m}.nc"
        # pipelines are currently just an idea
        # SHOULD allow the user to access PROCESSED data at the given location.
        # if that data does not exist then process or download the data
        pipelines:  # not compulsory
            mon_1deg:  # this is the name of the pipeline - this is NB
                data_path: "{DATA_PATH}/project/{t:%Y}/fname_{t:%Y%m}.nc"
                functions:  # functions applied to the pipeline xds --> xds
                    - package.module.function1
                    - package.module.function2
    """

    def __init__(self, catalog_file='./config_template.yaml', verbose=1):
        """
        Creates an interactive catalog to access data locally.
        If not present, data is downloaded from given URL.

        Parameters
        ==========
        catalog_file: str
            A YAML file with the input configuration. See the
            config_template.yaml for more information.
        verbose: int
            sets the verbosity of the operations
            0 = silent
            1 = basic outputs
            2 = very verbose with many outputs

        Returns
        =======
        Catalog object with Records, where a record contains the information
        given in the config.yaml file
        """
        from .config import read_catalog

        self.verbose = verbose
        self._config_dict = read_catalog(catalog_file)
        self._create_records()
        self.VARS = VariableAccess(self)

    def _create_records(self):
        """
        Runs through the catalog dictionary (YAML file)
        and creates an instance for each catalog entry (record)
        """
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
    """
    Used to access records by variable rather than the record name
    This is useful for later access when you might have forgotten
    what record names mean.
    """

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
