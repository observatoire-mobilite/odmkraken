import dagster
from .postgres import postgres_connection
from .edmo.busdata import edmo_vehdata, shortest_path_engine
import os


RESOURCES_TEST = {
    'postgres_connection': postgres_connection.configured({'dsn': os.environ.get('DSN_EDMO_AOO', '')}),
    'edmo_vehdata': edmo_vehdata,
    'shortest_path_engine': shortest_path_engine
}