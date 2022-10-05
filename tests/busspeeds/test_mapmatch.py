import pytest
from odmkraken.busspeeds.mapmatch import *
import dagster
import uuid
from datetime import datetime


@pytest.fixture
def fake_edmo(mocker):
    fake_edmo = mocker.Mock()
    
    def fake_tf(i):
        tf = mocker.Mock()
        tf.id = uuid.uuid4()
        return tf
    
    fake_data = [fake_tf(i) for i in range(4)]
    fake_edmo._data = fake_data
    fake_edmo.get_timeframes_on = mocker.MagicMock(return_value=fake_data)
    fake_edmo.get_edgelist = mocker.MagicMock()
    fake_fixes = [(1, 2, 3.4, 0.1, 0, 1.), (2, 3, 10.1, 0.9, 0, 8.)]
    fake_edmo.get_nearby_roads = mocker.MagicMock(return_value=fake_fixes)
    fake_pings = [(1, 1, 1), (2, 2, 2), (3, 3, 3)]
    fake_cur = mocker.MagicMock()
    fake_cur.__enter__ = mocker.Mock(return_value=fake_pings)
    fake_cur.rowcount = 3
    fake_edmo.get_pings = mocker.Mock(return_value=fake_cur)
   
    fake_edmo._cur = fake_cur
    fake_edmo._pings = fake_pings
    fake_edmo._fixes = fake_fixes
    return fake_edmo


def test_load_vehicle_timeframes(fake_edmo, mocker):
    t0, t1 = '2022-1-1 04:00', '2022-1-2 04:00'
    ctx = dagster.build_op_context(resources={'edmo_vehdata': fake_edmo},
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
    # faking components of the mapmatcher library
    fake_path = mocker.Mock(name='fake_path')
    fake_recon = mocker.patch('odmkraken.busspeeds.mapmatch.reconstruct_optimal_path', autospec=True)
    fake_recon.return_value = fake_path

    fake_cand = mocker.patch('odmkraken.busspeeds.mapmatch.candidate_solution', autospec=True)
    fake_cand.side_effect = lambda *d: d
    
    fake_proj = mocker.patch('odmkraken.busspeeds.mapmatch.ProjectLinear', autospec=True)
    fake_proj_inst = fake_proj.return_value
    fake_proj_inst.project.return_value = [(1, 2, 0.0, 4.1), (2, 3, 4.1, 2.)]

    fake_spe = mocker.patch('mapmatcher.pathfinder.nx.NXPathFinderWithLocalCache', autospec=True)
    res = {'edmo_vehdata': fake_edmo,
           'shortest_path_engine': fake_spe}
    ctx = dagster.build_op_context(resources=res)

    res = most_likely_path(ctx, fake_tf)
    # all this does is call bits of `mapmatcher` in the right order, namely:
    #   1. `vehicle_timeframe` goes as sole input to `get_pings`
    fake_edmo.get_pings.assert_called_once_with(fake_tf)
    #   2. its results go to `reconstruct_optimal_path` (along with the scorer and `nearby_roads` callback)
    #      note: this low-level approach is necessary because of the `nearby_roads` closure
    #      tge tests make sure that whatever data we enter, it is passed through (even if not as the correct type)
    fake_recon.assert_called()
    assert fake_recon.call_args.kwargs['shortest_path_engine'] == fake_spe
    assert fake_recon.call_args.args[0] == fake_edmo._pings
    nearby_roads = fake_recon.call_args.args[1]
    roads = nearby_roads(datetime(2020, 1, 1, 4), 0.1, 0.2)
    fake_edmo.get_nearby_roads.called_once_with(datetime(2020, 1, 1, 4), 0.1, 0.2)
    assert fake_cand.call_count == 2
    assert roads == [(*f, datetime(2020, 1, 1, 4)) for f in fake_edmo._fixes]
    #   3. a `ProjectLinear` instances is created and the optimal path goes to its `ProjetLinear.project`
    fake_proj.assert_called_once_with()
    fake_proj_inst.project.assert_called_once_with(fake_path)
    #   4. its return gets expanded to a list of tuples
    assert res == [(4321, 1, 2, 0.0, 4.1), (4321, 2, 3, 4.1, 2.)]


def test_extract_halts(fake_edmo, fake_tf):
    ctx = dagster.build_op_context(resources={'edmo_vehdata': fake_edmo})
    extract_halts(ctx, fake_tf)
    fake_edmo.extract_halts.assert_called_once_with(fake_tf)


def test_mapmatch_config(mocker):
    t0 = datetime(2022, 4, 12, 4)
    t1 = datetime(2022, 4, 13, 4)
    res = mapmatch_config(t0, t1)
    conf = res['ops']['load_vehicle_timeframes']['config']
    assert conf['date_from'] == t0.strftime('%Y-%m-%d %H:%M')
    assert conf['date_to'] == t1.strftime('%Y-%m-%d %H:%M')
