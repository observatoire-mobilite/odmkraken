import dagster
from .postgres import postgres_connection
from .edmo.busdata import edmo_bus_data, shortest_path_engine
import os


RESOURCES_TEST = {
    'postgres_connection': postgres_connection.configured({'dsn': os.environ.get('DSN', 'postgres://postgres:postgres@localhost:5432/mapmatcher')}),
    'edmo_bus_data': edmo_bus_data,
    'shortest_path_engine': shortest_path_engine
}
