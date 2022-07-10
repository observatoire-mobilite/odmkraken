import typing
import dagster
from . import EDMOData
import pendulum
import uuid
import pandas as pd
from datetime import datetime
from dataclasses import dataclass
import uuid


SQL_TIMEFRAMES_FILE='select id, vehicle_id, time_start, time_end from bus_data.data_file_timeframes where file_id=%s'
SQL_TIMEFRAMES_PERIOD='select id, vehicle_id, time_start, time_end from bus_data.data_file_timeframes where time_start between %s and %s'
SQL_HALTS = 'insert into "bus_data"."halts" select * from "bus_data"."get_vehicle_halts"(%s, %s, %s)'

TIMEFRAME_ROW = typing.Tuple[uuid.UUID, int, datetime, datetime]


@dataclass
class VehicleTimeFrame:
    id: uuid.UUID
    vehicle_id: int
    time_from: datetime
    time_to: datetime

    __slots__ = ('id', 'vehicle_id', 'time_from', 'time_to')

    def flat(self) -> typing.Tuple[int, datetime, datetime]:
        return (self.vehicle_id, self.time_from, self.time_to)


class EDMOBusData(EDMOData):
    
    def get_timeframes_by_file(self, file_id: uuid.UUID) -> typing.Iterator[VehicleTimeFrame]:
        """Return all vehicle timeframes contained in a given file."""
        with self.store.query(SQL_TIMEFRAMES_FILE, file_id) as cur:
            return (VehicleTimeFrame(*row) for row in cur)

    def get_timeframes_on(self, t0: pendulum.DateTime, tf: pendulum.DateTime) -> typing.Iterator[VehicleTimeFrame]:
        """Return all vehicle timeframes contained in a given file."""
        with self.store.query(SQL_TIMEFRAMES_PERIOD, t0, tf) as cur:
            return (VehicleTimeFrame(*row) for row in cur)

    def get_pings(self, tf: VehicleTimeFrame):
        """Get all pings making up `tf`."""
        cur = self.store.callproc('bus_data.get_pings', *tf.flat())
        if cur.rowcount < 2:
            raise RuntimeError('record has less than 2 pings')
        yield from cur

    def get_edgelist(self):
        """Retrieve the entire network as `nx`-style list of edges."""
        return self.store.callproc('bus_data.get_edgelist')

    def extract_halts(self, tf: VehicleTimeFrame):
        """Get all declared halts performed during `tf`."""
        with self.store.query(SQL_HALTS, *tf.flat()) as cur:
            yield from cur

    def get_nearby_roads(self, t: datetime, x: float, y: float):
        """Get roads physically clos to given point at given time"""
        with self.store.callproc('road_network.nearby_roads', x, y) as cur:
            yield from cur


@dagster.resource(required_resource_keys={'postgres_connection'})
def edmo_bus_data(init_context: dagster.InitResourceContext) -> EDMOBusData:
    return EDMOBusData(init_context.resources.postgres_connection)
