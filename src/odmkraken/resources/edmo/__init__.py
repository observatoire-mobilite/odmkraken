import typing
import dagster
from ..postgres import PostgresConnector


class EDMOData:

    def __init__(self, conn, vehdata_schema: str='vehdata', network_schema: str='network'):
        self.store: PostgresConnector = conn
        self.vehdata_schema = str(vehdata_schema)
        self.network_schema = str(network_schema)