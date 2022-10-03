import pytest
from odmkraken.resources.edmo.busdata.sql import *


def test_query_init():
    q = Query('test')
    assert q.sql == SQL('test')

    
def test_query_call(mocker):
    fake_sql = mocker.Mock()
    fake_sql.format = mocker.MagicMock()
    s = mocker.patch('odmkraken.resources.edmo.busdata.sql.SQL', autospec=True)
    s.return_value = fake_sql

    q = Query('moin')
    assert q.sql == fake_sql
    q(schema='moin', test1=2, test2=Identifier('eddi'))
    fake_sql.format.assert_called_once()
    call = fake_sql.format.call_args_list[0]
    assert 'pings_table' in call.kwargs
    assert call.kwargs['pings_table'] == Identifier('moin', 'pings')
    assert call.kwargs['test1'] == Literal(2)
    assert call.kwargs['test2'] == Identifier('eddi')


def test_query_table(mocker):
    fake_sql = mocker.Mock()
    fake_sql.format = mocker.MagicMock()
    s = mocker.patch('odmkraken.resources.edmo.busdata.sql.SQL', autospec=True)
    s.return_value = fake_sql

    q = Query('moin')
    q(schema='moin', staging_table='stage')
    fake_sql.format.assert_called_once()
    call = fake_sql.format.call_args_list[0]
    assert call.kwargs['staging_table'] == Identifier('moin', 'stage')


def test_extract_halts(mocker):
    assert 'insert into {halts_table}' in extract_halts.sql.string

# TODO: actually test SQL queries
