import dagster
import typing
from datetime import datetime, timedelta
from mapmatcher import Itinerary, reconstruct_optimal_path, ProjectLinear, Scorer, candidate_solution
from odmkraken.resources.edmo.busdata import VehicleTimeFrame


@dagster.op(out=dagster.DynamicOut(), required_resource_keys={'edmo_vehdata'},
            config_schema={'date_from': str, 'date_to': str})
def load_vehicle_timeframes(context: dagster.OpExecutionContext) -> typing.Iterator[dagster.DynamicOutput[VehicleTimeFrame]]:
    dates = context.op_config['date_from'], context.op_config['date_to']
    for tf in context.resources.edmo_vehdata.get_timeframes_on(*dates):
        yield dagster.DynamicOutput(tf, mapping_key=str(tf.id.hex))


@dagster.op(required_resource_keys={'edmo_vehdata', 'shortest_path_engine'})
def most_likely_path(context: dagster.OpExecutionContext, vehicle_timeframe: VehicleTimeFrame) -> typing.List[typing.Tuple[int, int, int, datetime, timedelta]]:

    # lazy nearby road detection
    def nearby_roads(t: datetime, x: float, y: float):
        roads = context.resources.edmo_vehdata.get_nearby_roads(t, x, y)
        roads = [candidate_solution(*r, t) for r in roads]
        return roads

    # set detection parameters
    scorer = Scorer()
    spe = context.resources.shortest_path_engine

    # reconstruct path over road segments
    with context.resources.edmo_vehdata.get_pings(vehicle_timeframe) as cur:
        path = reconstruct_optimal_path(cur, nearby_roads, shortest_path_engine=spe, scorer=scorer)
    
    # reproject in the perspective of the road segments
    projector = ProjectLinear()
    pathway = projector.project(path)
    return [(vehicle_timeframe.vehicle_id, *p) for p in pathway]


@dagster.op(required_resource_keys={'edmo_vehdata'})
def extract_halts(context: dagster.OpExecutionContext, vehicle_timeframe: VehicleTimeFrame):
    context.resources.edmo_vehdata.extract_halts(vehicle_timeframe)


@dagster.daily_partitioned_config(start_date=datetime(2020, 1, 1), hour_offset=4)
def mapmatch_config(start: datetime, end: datetime):
    config = {'date_from': start.strftime('%Y-%m-%d %H:%M'), 
              'date_to': end.strftime('%Y-%m-%d %H:%M')}
    return {'ops': {'load_vehicle_timeframes': {'config': config }}}


@dagster.graph()
def mapmatch_bus_data():
    timeframes = load_vehicle_timeframes()
    timeframes.map(most_likely_path)
    timeframes.map(extract_halts)
