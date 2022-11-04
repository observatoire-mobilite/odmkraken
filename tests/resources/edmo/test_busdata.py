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
        (uuid.uuid4(), 0, datetime(2020, 1, 2, 4), datetime(2020, 1, 3, 4)),
    ]
    fake_cur.__enter__ = mocker.Mock(return_value=fake_cur)
    fake_cur.__iter__.return_value = fake_data
    fake_cur.rowcount = len(fake_data)
    fake_pgc.cursor = mocker.Mock(return_value=fake_cur)
    fake_pgc.query = mocker.Mock(return_value=fake_cur)
    fake_pgc.callproc = mocker.MagicMock(return_value=fake_cur)
    fake_pgc.fetchone = mocker.Mock(return_value=[100])
    fake_pgc._fake_data = fake_data
    return fake_pgc


def test_edmo_vehdata(fake_pgc):
    ctx = dagster.build_init_resource_context(
        resources={'local_postgres': fake_pgc}, config={}
    )
    data = edmo_vehdata(ctx)
    assert isinstance(data, EDMOData)
    assert isinstance(data, EDMOVehData)
    assert data.store == fake_pgc


def test_edmovehdata(fake_pgc):
    data = EDMOVehData(fake_pgc)
    assert data.store == fake_pgc


def test_shortest_path_engine(mocker):
    fake_edmo = mocker.patch(
        'odmkraken.resources.edmo.busdata.EDMOVehData', autospec=True
    )
    fake_cur = mocker.MagicMock(spec=['__enter__', '__exit__'])
    fake_cur.__enter__.return_value = fake_cur
    fake_edmo.get_edgelist.return_value = fake_cur
    fake_spe = mocker.patch(
        'odmkraken.resources.edmo.busdata.NXPathFinderWithLocalCache', autospec=True
    )
    ctx = dagster.build_init_resource_context(resources={'edmo_vehdata': fake_edmo})
    spe = shortest_path_engine(ctx)
    fake_edmo.get_edgelist.assert_called_once()
    assert spe == fake_spe(fake_cur)


def test_edmobusdata_get_timeframes_by_file(fake_pgc, mocker):
    sql = mocker.patch(
        'odmkraken.resources.edmo.busdata.sql.vehicle_timeframes_for_file',
        autospec=True,
    )
    sql.return_value = 'hello'

    data = EDMOVehData(fake_pgc)
    file_id = uuid.uuid4()
    tf = list(data.get_timeframes_by_file(file_id))
    fake_pgc.query.assert_called_once_with('hello', file_id)
    assert [t.id for t in tf] == [t[0] for t in fake_pgc._fake_data]


def test_edmobusdata_get_timeframes_on(fake_pgc, mocker):
    sql = mocker.patch(
        'odmkraken.resources.edmo.busdata.sql.vehicle_timeframes_for_period',
        autospec=True,
    )
    sql.return_value = 'period'

    t = pendulum.now().at(hour=4)
    dt = pendulum.duration(days=2)
    data = EDMOVehData(fake_pgc)

    tf = list(data.get_timeframes_on(t, t + dt))
    fake_pgc.query.assert_called_once_with('period', t, t + dt) 
    assert [t.id for t in tf] == [t[0] for t in fake_pgc._fake_data]


def test_edmobusdata_get_pings(fake_pgc, mocker):
    data = EDMOVehData(fake_pgc)
    t1, t2 = datetime(2022, 1, 1, 4), datetime(2022, 1, 2, 4)
    tf = VehicleTimeFrame(uuid.uuid4(), 1, t1, t2)
    with data.get_pings(tf) as cur:
        fake_pgc.callproc.called_once_with('vehdata.get_pings', 1, t1, t2)

    fake_cur = mocker.Mock()
    fake_cur.__enter__ = mocker.MagicMock(return_value=fake_cur)
    fake_cur.__exit__ = mocker.MagicMock()
    fake_cur.rowcount = 1
    fake_pgc.callproc = mocker.Mock(return_value=fake_cur)
    with pytest.raises(RuntimeError):
        with data.get_pings(tf) as cur:
            pass
    fake_cur.__exit__.assert_called_once()


def test_edmobusdata_get_edgelist(fake_pgc):
    data = EDMOVehData(fake_pgc)
    data.get_edgelist()
    fake_pgc.callproc.assert_called_once_with('network.get_edgelist')


def test_edmobusdata_extract_halts(fake_pgc, mocker):
    sql = mocker.patch(
        'odmkraken.resources.edmo.busdata.sql.extract_halts', autospec=True
    )
    sql.return_value = 'halts'

    t1, t2 = datetime(2022, 1, 1, 4), datetime(2022, 1, 2, 4)
    tf = VehicleTimeFrame(uuid.uuid4(), 1, t1, t2)
    data = EDMOVehData(fake_pgc)
    data.extract_halts(tf)
    fake_pgc.run.assert_called_once_with('halts', tf.vehicle_id, t1, t2)


def test_edmobusdata_get_nearby_roads(fake_pgc):
    t = datetime.now()
    x, y = 100.4, 100.5
    data = EDMOVehData(fake_pgc)
    pings = list(data.get_nearby_roads(t, x, y, radius=1001))
    assert len(pings) == 4
    fake_pgc.callproc.assert_called_once_with('network.nearby_roads', x, y, 1001.)


def test_check_file_already_imported(fake_pgc):
    checksum = b'moin'
    data = EDMOVehData(fake_pgc)
    assert data.check_file_already_imported(checksum)


def test_import_csv_file(fake_pgc, mocker):
    # TODO: incomplete, but for now just useful to check the method is executable
    sql = mocker.patch('odmkraken.resources.edmo.busdata.sql', autospec=True)
    sql.create_staging_table = mocker.Mock(return_value='adjust_date')
    sql.len_staging_table = mocker.Mock(return_value='len_st')

    fake_handle = mocker.Mock()
    checksum = b'checkthesum'

    data = EDMOVehData(fake_pgc)
    data.import_csv_file(fake_handle, sep='?', table='moin')

    fake_pgc.run.assert_called_once_with('adjust_date')
    fake_pgc.copy_from.assert_called_with(
        fake_handle, ('vehdata', 'moin'), separator='?'
    )
    fake_pgc.fetchone.assert_called_once_with('len_st')
    sql.create_staging_table.assert_called_once_with(
        staging_table='moin', schema='vehdata'
    )
    sql.len_staging_table.assert_called_once_with(
        staging_table='moin', schema='vehdata'
    )
