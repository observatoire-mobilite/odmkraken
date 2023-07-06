import pytest
from odmkraken.busspeeds.extract import *
from pathlib import Path
import dagster
import pandas as pd


def test_extract_from_csv(mocker):
    fake_edmo = mocker.patch('odmkraken.resources.edmo.busdata.EDMOVehData', autospec=True)
    fake_of = mocker.patch('odmkraken.busspeeds.extract.open_file')
    fake_handle = mocker.Mock()
    fake_of.return_value = 10000, fake_handle
    fake_cc = mocker.patch('odmkraken.busspeeds.extract.compute_checksum')
    fake_cc.return_value = b'moin'
    fake_if = mocker.patch('odmkraken.busspeeds.extract.infer_format')
    fake_if.return_value = {'sep': '?', 'date': 'DATE'}
    fake_file = '/some/where/a/file.csv.zip'
    fake_uuid = mocker.patch('uuid.uuid4')
    fake_uuid.return_value='abcdefgh'
    
    ctx = dagster.build_op_context(
        resources={'edmo_vehdata': fake_edmo},
        config={'file': '/some/where/a/file.csv.zip'}
    )
    
    fake_edmo.check_file_already_imported.return_value = False
    extract_from_csv(ctx)

    fake_of.assert_called_once_with(Path(fake_file))
    fake_if.assert_called_once_with(fake_handle)
    fake_cc.assert_called_once_with(fake_handle)
    fake_edmo.check_file_already_imported.assert_called_once_with(b'moin')
    fake_edmo.import_csv_file.assert_called_once_with(fake_handle, sep='?', table='_import_abcdefgh')

    fake_of.return_value = 0, fake_handle
    extract_from_csv(ctx)

    fake_edmo.check_file_already_imported.return_value = True
    with pytest.raises(FileAlreadyImportedError):
        extract_from_csv(ctx)



def test_adjust_format_unexpected_format():
    """just a table layout that has nothing to do with what we'd expect
    
    Might happen e.g. if CSV files are read with the wrong separator.
    """
    dta = pd.DataFrame({'moin': [1, 2, 3, 4]})
    with pytest.raises(dagster.Failure, match=r'Input malformed') as excinfo:
        adjust_format(dta)
    

@pytest.fixture
def adjust_format_fields():
    """Short-hand for the actually required fields"""
    return ['TYP', 'DATUM', 'SOLLZEIT', 'ZEIT', 'FAHRZEUG', 'LINIE', 'UMLAUF',
            'FAHRT', 'HALT', 'LATITUDE', 'LONGITUDE', 'EINSTEIGER', 'AUSSTEIGER']



def test_adjust_format_incomplete_headers(adjust_format_fields):
    """Missing headers
    
    Provides a partial file with only two headers (but the correcto ones)
    """
    dta = pd.DataFrame({f: [1, 2, 3, 4] for f in adjust_format_fields[:2]})
    with pytest.raises(dagster.Failure):
        adjust_format(dta)
    
    
def test_adjust_format_false_headers(adjust_format_fields):
    """simulates a typo in one header"""
    dta = pd.DataFrame({(k + 'Z' if i==5 else ''): [1, 2, 3, 4] for i, k in enumerate(adjust_format_fields)})
    with pytest.raises(dagster.Failure):
        adjust_format(dta)


def test_adjust_format_all_numbers(adjust_format_fields):
    """correct headers, but will numbers; passes, even though result-datatypes don't make sense"""
    #TODO: add better input file validation
    dta = pd.DataFrame({k: [1, 2, 3, 4] for k in adjust_format_fields})
    _check_types(dta)


def test_adjust_format_real_data():
    """try with the testing data use in live-tests.yaml"""
    dta = pd.read_csv('tests/testdata.csv.zip', sep=';')
    _check_types(dta)


def _check_types(dta):
    res = adjust_format(dta)    
    for ks, t in {
        ('type', 'vehicle', 'line', 'sortie', 'run', 'stop'): 'category',
        ('time', 'expected_time'): 'datetime64[ns]',
        ('latitude', 'longitude'): 'float',
        ('count_people_boarding', 'count_people_disembarking'): 'Int16'
    }.items():
        for k in ks:
            assert res.dtypes[k] == t


def test_find_duplicates():
    """Basic functional test."""
    dta = pd.DataFrame({'vehicle': [1, 1, 1, 1], 'time': [1, 1, 2, 3], 'type': [-1, 0, -1, -1]})
    dupl = find_duplicates(dta)
    