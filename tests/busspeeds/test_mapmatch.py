import pytest
#from odmkraken.busspeeds.mapmatch import load_vehicle_timeframes, most_likely_path, VehicleTimeFrame
import dagster
import uuid


@pytest.fixture
def fake_edmo(mocker):
    fake_edmo = mocker.Mock()
    
    def fake_tf(i):
        tf = mocker.Mock()
        tf.id = uuid.uuid4()
        return tf
    
    fake_data = [fake_tf(i) for i in range(4)]
    fake_edmo._data = fake_data
    fake_edmo.get_timeframes_on = mocker.Mock(return_value=fake_data)
    fake_edmo.get_edgelist = mocker.MagicMock()
    fake_cur = mocker.MagicMock()
    fake_cur.__enter__ = mocker.Mock(return_value=[(1, 1, 1), (2, 2, 2), (3, 3, 3)])
    fake_cur.rowcount = 3
    fake_edmo.get_pings = mocker.Mock(return_value=fake_cur)
    
    return fake_edmo


def test_load_vehicle_timeframes(fake_edmo, mocker):
    t0, t1 = '2022-1-1 04:00', '2022-1-2 04:00'
    ctx = dagster.build_op_context(resources={'edmo_bus_data': fake_edmo},
                                   config={'date_from': t0, 'date_to': t1})
    dyn = list(load_vehicle_timeframes(ctx))
    fake_edmo.get_timeframes_on.assert_called_once_with(t0, t1)
    assert len(dyn) == 4
    assert isinstance(dyn[0], dagster.DynamicOutput)
    assert dyn[0].mapping_key == fake_edmo._data[0].id.hex


@pytest.fixture
def fake_tf(mocker):
    tf = mocker.Mock(VehicleTimeFrame)
    tf.id = uuid.uuid4()
    tf.time_from = datetime(2022, 1, 1, 4)
    tf.time_to = datetime(2022, 1, 2, 4)
    tf.vehicle_id = 4321
    return tf


def test_most_likely_path(fake_edmo, fake_tf, mocker):
    fake_ping = mocker.MagicMock(name='Ping')
    fake_ping.seconds_since = mocker.Mock(return_value=5)
    mocker.patch('odmkraken.busspeeds.mapmatch.Ping', new=fake_ping)
    ctx = dagster.build_op_context(resources={'edmo_bus_data': fake_edmo})
    most_likely_path(ctx, fake_tf)
