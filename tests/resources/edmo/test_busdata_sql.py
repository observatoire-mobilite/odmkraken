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


def test_extract_halts(mocker):
    assert 'insert into {halts_table}' in extract_halts.sql.string

# TODO: actually test SQL queries
