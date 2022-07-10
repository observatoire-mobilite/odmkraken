import pytest
from odmkraken.resources.edmo.busdata import *
import dagster
import pendulum
import uuid
from datetime import datetime


@pytest.fixture
def fake_pgc(mocker):
    fake_pgc = mocker.Mock()
    fake_cur = mocker.MagicMock()
    fake_data = [
        (uuid.uuid4(), 0, datetime(2020, 1, 1, 4), datetime(2020, 1, 2, 4)),
        (uuid.uuid4(), 1, datetime(2020, 1, 1, 4), datetime(2020, 1, 2, 4)),
        (uuid.uuid4(), 2, datetime(2020, 1, 1, 4), datetime(2020, 1, 2, 4)),
        (uuid.uuid4(), 0, datetime(2020, 1, 2, 4), datetime(2020, 1, 3, 4))
    ]
    fake_cur.__enter__ = mocker.Mock(return_value=fake_data)
    fake_cur.rowcount = len(fake_data)
    fake_pgc.query = mocker.Mock(return_value=fake_cur)
    fake_pgc.callproc = mocker.Mock(return_value=fake_cur)
    fake_pgc._fake_data = fake_data
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
    assert [t.id for t in tf] == [t[0] for t in fake_pgc._fake_data]

    
def test_edmobusdata_get_timeframes_on(fake_pgc):
    t = pendulum.now().at(hour=4)
    dt = pendulum.duration(days=2)
    data = EDMOBusData(fake_pgc)
    
    tf = list(data.get_timeframes_on(t, t + dt))
    fake_pgc.query.assert_called_once_with(SQL_TIMEFRAMES_PERIOD, t, t.add(days=2))
    assert [t.id for t in tf] == [t[0] for t in fake_pgc._fake_data]


def test_edmobusdata_get_pings(fake_pgc, mocker):
    data = EDMOBusData(fake_pgc)
    t1, t2 = datetime(2022, 1, 1, 4), datetime(2022, 1, 2, 4)
    tf = VehicleTimeFrame(uuid.uuid4(), 1, t1, t2)
    pings = list(data.get_pings(tf))
    fake_pgc.callproc.called_once_with('bus_data.get_pings', 1, t1, t2)

    fake_cur = mocker.Mock()
    fake_cur.rowcount = 1
    fake_pgc.callproc = mocker.Mock(return_value=fake_cur)
    with pytest.raises(RuntimeError):
        list(data.get_pings(tf))


def test_edmobusdata_get_edgelist(fake_pgc):
    data = EDMOBusData(fake_pgc)
    data.get_edgelist()
    fake_pgc.callproc.assert_called_once_with('bus_data.get_edgelist')


def test_edmobusdata_extract_halts(fake_pgc):
    t1, t2 = datetime(2022, 1, 1, 4), datetime(2022, 1, 2, 4)
    tf = VehicleTimeFrame(uuid.uuid4(), 1, t1, t2)
    data = EDMOBusData(fake_pgc)
    list(data.extract_halts(tf))
    fake_pgc.query.assert_called_once_with(SQL_HALTS, tf.vehicle_id, t1, t2)