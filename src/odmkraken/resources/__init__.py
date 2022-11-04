import dagster
from .postgres import postgres_connection
from .edmo.busdata import edmo_vehdata, shortest_path_engine
from .edmo.busdata.iomanager import edmo_mapmatching_results
import os


RESOURCES = {
    'local_postgres': postgres_connection,
    'edmo_vehdata': edmo_vehdata,
    'shortest_path_engine': shortest_path_engine,
    'edmo_mapmatching_results': edmo_mapmatching_results
}