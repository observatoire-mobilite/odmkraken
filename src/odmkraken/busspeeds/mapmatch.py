import dagster
import typing
from datetime import datetime, timedelta
from mapmatcher import Itinerary, reconstruct_optimal_path, ProjectLinear, Scorer, candidate_solution
import mapmatcher.errors
from odmkraken.resources.edmo.busdata import VehicleTimeFrame
from functools import partial
import multiprocessing as mp
from dataclasses import dataclass
import uuid


# fields: vehicle_id, from_node, to_node, t_enter, dt_travers
RESULT_LIST = typing.List[typing.Tuple[int, int, int, datetime, timedelta]]


def get_nearby_roads(t: datetime, x: float, y: float, context: dagster.OpExecutionContext):
    for r in (100, 200, 500, 1000):
        roads = context.resources.edmo_vehdata.get_nearby_roads(t, x, y, radius=r)
        roads = [candidate_solution(*r, t) for r in roads]
        if roads:
            break
    if (r > 100):
        print(f'Increased search radius to {r:.1f}m around (x={x:.1f}, y={y:.1f}) at t={t}')
    return roads


class MapMatcher:

    def __init__(self, dsn: str):
        

    @property
    def vehicle_id(self) -> int:
        return self.timeframe.vehicle_id

    def run(self, context: dagster.OpExecutionContext):
        try:
            self._mapmatch(context, self.timeframe)
        except Exception as e:
            self.success = False
        
    def _mapmatch(context, tf) -> MapMatchResult:
        scorer = Scorer()
        spe = context.resources.shortest_path_engine
        nearby_roads = partial(get_nearby_roads, context=context)
        store = context.resources.edmo_vehdata.store_results
        
        with context.resources.edmo_vehdata.get_pings(tf) as cur:
            path = reconstruct_optimal_path(cur, nearby_roads, shortest_path_engine=spe, scorer=scorer)

        # reproject in the perspective of the road segments
        projector = ProjectLinear()
        pathway = projector.project(path)

        results = [(tf.vehicle_id, *p) for p in pathway]
        store(results)


@dagster.asset(
    required_resource_keys={'edmo_vehdata', 'shortest_path_engine'}
)
def most_likely_path(context: dagster.OpExecutionContext, vehicle_timeframe: VehicleTimeFrame) -> typing.List[typing.Tuple['uuid.UUID', bool, str]]
    
    # retrieve timeframes
    dates = context.op_config['date_from'], context.op_config['date_to']
    frames = context.resources.edmo_vehdata.get_timeframes_on(*dates)

    # map to multi-prcessing
    with mp.Pool() as pool:
        res = pool.map(MapMatchResult, frames)
    return log


@dagster.op(required_resource_keys={'edmo_vehdata'})
def extract_halts(context: dagster.OpExecutionContext, vehicle_timeframe: VehicleTimeFrame) -> None:
    context.resources.edmo_vehdata.extract_halts(vehicle_timeframe)


@dagster.daily_partitioned_config(start_date=datetime(2020, 1, 1), hour_offset=4)
def mapmatch_config(start: datetime, end: datetime):
    config = {'date_from': start.strftime('%Y-%m-%d %H:%M'), 
              'date_to': end.strftime('%Y-%m-%d %H:%M')}
    return {'ops': {'load_vehicle_timeframes': {'config': config }}}


@dagster.graph(out={
    'most_likely_path': dagster.GraphOut(), 
    'extract_halts': dagster.GraphOut()
})
def mapmatch_bus_data() -> typing.Tuple[typing.List[VehicleTimeFrame], typing.List[bool]]:
    timeframes = load_vehicle_timeframes()
    r1 = timeframes.map(most_likely_path)
    r2 = timeframes.map(extract_halts)
    return {'most_likely_path': r1, 'extract_halts': r2}


#mapmatched_data = dagster.AssetsDefinition.from_graph(mapmatch_bus_data)

