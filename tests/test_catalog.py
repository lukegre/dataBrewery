from databrewery import Catalog


def test_high_level():

    db = Catalog('./catalog_template.yaml')

    assert db is not None
