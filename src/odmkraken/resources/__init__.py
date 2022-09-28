import dagster
from .postgres import postgres_connection
from .edmo.busdata import edmo_vehdata, shortest_path_engine
import os


RESOURCES = {
    'local_postgres': postgres_connection,
    'edmo_vehdata': edmo_vehdata,
    'shortest_path_engine': shortest_path_engine
}