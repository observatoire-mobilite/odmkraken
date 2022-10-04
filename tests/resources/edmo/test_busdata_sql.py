import pytest
from odmkraken.resources.edmo.busdata.sql import *


def test_query_init(mocker):
    s = mocker.patch('odmkraken.resources.edmo.busdata.sql.SQL', autospec=True)
    q = Query('test')
    s.assert_called_once_with('test')


@pytest.fixture
def fake_sql(mocker):
    fake_sql = mocker.Mock()
    fake_sql.format = mocker.MagicMock()
    s = mocker.patch('odmkraken.resources.edmo.busdata.sql.SQL', autospec=True)
    s.return_value = fake_sql
    return fake_sql


def test_query_call(fake_sql):
    
    # generate dummy query
    q = Query('moin')
    assert q.sql == fake_sql
    
    # call it with some arguments
    q(schema='schema', test1=2, test2=Identifier('eddi'), test3='moien',
      test4='\'; drop database current_database;', staging_table='stage',
      vehicles_table='vroum')

    # this must trigger a call to `.format`
    fake_sql.format.assert_called_once()
    
    # within this call, we expect a whole bunch of arguments
    callargs = fake_sql.format.call_args_list[0].kwargs
    for tbl in ('lines', 'stops', 'runs', 'pings', 'pings_from_halts', 'halts', 'data_files', 'data_file_timeframes'):
        assert callargs[f'{tbl}_table'] == Identifier('schema', tbl)
    assert callargs['vehicles_table'] == Identifier('schema', 'vroum')
    assert callargs['test1'] == Literal(2)
    assert callargs['test2'] == Identifier('eddi')
    assert callargs['test3'] == Literal('moien')
    assert callargs['test4'] == Literal('\'; drop database current_database;')
    assert callargs['staging_table'] == Identifier('schema', 'stage')


def test_query_defaults(fake_sql):
    q = Query('eddi', test1='hoi', test2='wi gaats', test4_table='chuchichäschtli',
              test5_table='seich')
    q(schema='schema', test2='guet', test3='siech', test5_table='soen')
    
    fake_sql.format.assert_called_once()
    callargs = fake_sql.format.call_args_list[0].kwargs
    for tbl in ('lines', 'stops', 'runs', 'pings', 'pings_from_halts', 'halts', 'data_files', 'data_file_timeframes'):
        assert callargs[f'{tbl}_table'] == Identifier('schema', tbl)
    assert callargs['test1'] == Literal('hoi')
    assert callargs['test2'] == Literal('guet')
    assert callargs['test3'] == Literal('siech')
    assert callargs['test4_table'] == Identifier('schema', 'chuchichäschtli')
    assert callargs['test5_table'] == Identifier('schema', 'soen')


def test_adjust_date(mocker):
    assert adjust_date._defaults['system_srid'] == Literal(2169)
    assert adjust_date._defaults['input_srid'] == Literal(4326)
    q = adjust_date(schema='test', staging_table='stage', date_format='%Y%m%d')


def test_extract_vehicles(mocker):
    extract_vehicles(schema='test', staging_table='stage')
    
    