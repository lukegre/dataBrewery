import pandas as pd

from databrewery import CraftBrewery

t = pd.Timestamp('2010-02-02')
db = CraftBrewery('./catalog_template.yaml')
