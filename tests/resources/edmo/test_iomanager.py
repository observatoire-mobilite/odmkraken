"""These are trivial tests checking data flows, nothing functional."""
import pytest
from odmkraken.resources.edmo.busdata.iomanager import *
import dagster


@pytest.fixture
def fake_pgc(mocker):
    fake_pgc = mocker.Mock()
    fake_pgc.execute_batch = mocker.MagicMock()
    fake_pgc.fetchall = mocker.MagicMock(return_value='data')
    return fake_pgc


def test_handle_output(fake_pgc, mocker):
    sql = mocker.patch('odmkraken.resources.edmo.busdata.sql.add_results', autospec=True)
    sql.return_value = 'test'

    ctx = dagster.build_init_resource_context(
        resources={'local_postgres': fake_pgc}, config={}
    )
    io = edmo_mapmatching_results(ctx)

    ctx = dagster.build_output_context()
    io.handle_output(ctx, ['testing', 'this', 'thing'])
    fake_pgc.execute_batch.assert_called_once_with('test', ['testing', 'this', 'thing'])
    sql.assert_called_once_with(schema='vehdata')


def test_load_input(fake_pgc, mocker):
    sql = mocker.patch('odmkraken.resources.edmo.busdata.sql.get_results', autospec=True)
    sql.return_value = 'test2'

    ctx = dagster.build_init_resource_context(
        resources={'local_postgres': fake_pgc}, config={}
    )
    io = edmo_mapmatching_results(ctx)

    ctx = dagster.build_input_context()
    ret = io.load_input(ctx)
    assert ret == 'data'
    fake_pgc.fetchall.assert_called_once_with('test2')
    sql.assert_called_once_with(schema='vehdata')



