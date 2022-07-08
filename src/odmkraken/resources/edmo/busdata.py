import typing
import dagster
from . import EDMOData
import pendulum
import uuid
import pandas as pd
from datetime import datetime


SQL_TIMEFRAMES_FILE='''
select
    id, vehicle_id, time_start, time_end
from bus_data.data_file_timeframes
where file_id=%s
'''

SQL_TIMEFRAMES_PERIOD='''
select
    id, vehicle_id, time_start, time_end
from bus_data.data_file_timeframes
where time_start between %s and %s
'''

TIMEFRAME_ROW = typing.Tuple[uuid.UUID, int, datetime, datetime]


class EDMOBusData(EDMOData):
    
    def get_timeframes_by_file(self, file_id: uuid.UUID) -> typing.Iterator[TIMEFRAME_ROW]:
        """Return all vehicle timeframes contained in a given file."""
        with self.store.query(SQL_TIMEFRAMES_FILE, file_id) as cur:
            yield from cur

    def get_timeframes_on(self, t: pendulum.DateTime, dt: pendulum.Duration=pendulum.duration(days=1)) -> typing.Iterator[TIMEFRAME_ROW]:
        """Return all vehicle timeframes contained in a given file."""
        with self.store.query(SQL_TIMEFRAMES_PERIOD, t, t + dt) as cur:
            yield from cur


@dagster.resource(required_resource_keys={'postgres_connection'})
def edmo_bus_data(init_context: dagster.InitResourceContext) -> EDMOBusData:
    return EDMOBusData(init_context.resources.postgres_connection)
