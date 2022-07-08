import pytest
from odmkraken.resources.edmo.busdata import *
import dagster
import pendulum
import uuid


@pytest.fixture
def fake_pgc(mocker):
    fake_pgc = mocker.Mock()
    fake_cur = mocker.MagicMock()
    fake_cur.__enter__ = mocker.Mock(return_value=[0, 1, 2, 3])
    fake_pgc.query = mocker.Mock(return_value=fake_cur)
    fake_data = range(4)
    return fake_pgc


def test_edmo_bus_data(fake_pgc):
    ctx = dagster.build_init_resource_context(resources={'postgres_connection': fake_pgc})
    data = edmo_bus_data(ctx)
    assert isinstance(data, EDMOData)
    assert isinstance(data, EDMOBusData)
    assert data.store == fake_pgc


def test_edmobusdata(fake_pgc):
    data = EDMOBusData(fake_pgc)
    assert data.store == fake_pgc

def test_edmobusdata_get_timeframes_by_file(fake_pgc):
    data = EDMOBusData(fake_pgc)
    file_id = uuid.uuid4()
    tf = list(data.get_timeframes_by_file(file_id))
    fake_pgc.query.assert_called_once_with(SQL_TIMEFRAMES_FILE, file_id)
    assert tf == list(range(4))
    
def test_edmobusdata_get_timeframes_on(fake_pgc):
    t = pendulum.now().at(hour=4)
    dt = pendulum.duration(days=2)
    data = EDMOBusData(fake_pgc)
    
    tf = list(data.get_timeframes_on(t, dt=dt))
    fake_pgc.query.assert_called_once_with(SQL_TIMEFRAMES_PERIOD, t, t.add(days=2))
    assert tf == list(range(4))
