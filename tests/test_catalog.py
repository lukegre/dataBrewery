from databrewery import Catalog


def test_catalog_load():

    db = Catalog('./catalog_template.yaml')

    assert db is not None


def test_catalog_print():

    db = Catalog('./catalog_template.yaml')

    print(db)


def test_record_print():

    db = Catalog('./catalog_template.yaml')

    print(db.sst_oi_bulk)


def test_download_http():
    import os

    db = Catalog('./catalog_template.yaml')

    flist = db.sst_oi_bulk.local_files('2010-01-01', auto_download=True)
    dl_report = db.sst_oi_bulk.download_results

    assert isinstance(dl_report, dict)
    assert isinstance(flist, list)
    assert len(flist) == 1

    os.remove(flist[0])

    flist1 = db.sst_oi_bulk.local_files('2010-01-01', auto_download=True)
    flist2 = db.sst_oi_bulk.local_files('2010-01-01', auto_download=True)

    assert flist1 == flist2


def test_download_http_range():

    db = Catalog('./catalog_template.yaml')

    date_range = slice('2009-01-01', '2009-01-03')
    flist = db.sst_oi_bulk.local_files(date_range, auto_download=True)

    assert isinstance(flist, list)
    assert len(flist) == 3


def test_download_ftp_wo_password():
    import os

    db = Catalog('./catalog_template.yaml', verbose=0)

    flist = db.smos_cci.local_files('2012-01-01', auto_download=True)

    assert isinstance(flist, list)
    assert len(flist) == 1
    assert isinstance(flist[0], str)

    os.remove(flist[0])

    flist1 = db.smos_cci.local_files('2012-01-01', auto_download=True)
    flist2 = db.smos_cci.local_files('2012-01-01', auto_download=True)

    assert flist1 == flist2


def test_download_ftp_password():
    import os

    db = Catalog('./catalog_template.yaml', verbose=0)

    flist = db.oc_cci.local_files('2012-01-01', auto_download=True)

    assert isinstance(flist, list)
    assert len(flist) == 1
    assert isinstance(flist[0], str)

    os.remove(flist[0])

    flist1 = db.oc_cci.local_files('2012-01-01', auto_download=True)
    flist2 = db.oc_cci.local_files('2012-01-01', auto_download=True)

    assert flist1 == flist2
