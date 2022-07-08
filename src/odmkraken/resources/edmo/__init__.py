import typing
import dagster
from ..postgres import PostgresConnector


class EDMOData:

    def __init__(self, conn):
        self.store: PostgresConnector = conn

