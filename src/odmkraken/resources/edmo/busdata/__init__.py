import typing
import dagster
from .. import EDMOData
from . import sql
import pendulum
import uuid
from datetime import datetime
from dataclasses import dataclass
import uuid
from mapmatcher.pathfinder.nx import NXPathFinderWithLocalCache
import pathlib
from contextlib import contextmanager

TIMEFRAME_ROW = typing.Tuple[uuid.UUID, int, datetime, datetime]


@dataclass
class VehicleTimeFrame:
    id: uuid.UUID
    vehicle_id: int
    time_from: datetime
    time_to: datetime

    __slots__ = ('id', 'vehicle_id', 'time_from', 'time_to')

    def __post_init__(self):
        if not isinstance(self.id, uuid.UUID):
            self.id = uuid.UUID(self.id)

    def flat(self) -> typing.Tuple[int, datetime, datetime]:
        return (self.vehicle_id, self.time_from, self.time_to)


class EDMOVehData(EDMOData):
    def get_timeframes_by_file(
        self, file_id: uuid.UUID
    ) -> typing.List[VehicleTimeFrame]:
        """Return all vehicle timeframes contained in a given file."""
        with self.store.query(
            sql.vehicle_timeframes_for_file(schema=self.vehdata_schema), str(file_id)
        ) as cur:
            return [VehicleTimeFrame(*row) for row in cur]

    def get_timeframes_on(
        self, t0: pendulum.DateTime, tf: pendulum.DateTime
    ) -> typing.List[VehicleTimeFrame]:
        """Return all vehicle timeframes contained in a given file."""
        with self.store.query(
            sql.vehicle_timeframes_for_period(schema=self.vehdata_schema), t0, tf
        ) as cur:
            return [VehicleTimeFrame(*row) for row in cur]

    @contextmanager
    def get_pings(self, tf: VehicleTimeFrame):
        """Get all pings making up `tf`."""
        # TODO: remove hardcoding
        with self.store.callproc('vehdata.get_pings', *tf.flat()) as cur:
            if cur.rowcount < 2:
                raise RuntimeError('record has less than 2 pings')
            yield cur

    def get_edgelist(self):
        """Retrieve the entire network as `nx`-style list of edges."""
        # TODO: remove schema hardcoding
        return self.store.callproc('network.get_edgelist')

    def extract_halts(self, tf: VehicleTimeFrame):
        """Get all declared halts performed during `tf`."""
        self.store.run(sql.extract_halts(schema=self.vehdata_schema), *tf.flat())

    def get_nearby_roads(self, t: datetime, x: float, y: float, radius: float=90):
        """Get roads physically clos to given point at given time"""
        with self.store.callproc('network.nearby_roads', x, y, float(radius)) as cur:
            yield from cur

    def check_file_already_imported(self, checksum: bytes):
        with self.store.query(
            sql.file_by_checksum(schema=self.vehdata_schema), checksum
        ) as cur:
            return cur.rowcount > 0

    def import_csv_file(self, handle, sep: str = ',', table: str = 'raw_data'):
        self.store.run(
            sql.create_staging_table(staging_table=table, schema=self.vehdata_schema)
        )
        self.store.copy_from(handle, (self.vehdata_schema, table), separator=sep)
        return self.store.fetchone(
            sql.len_staging_table(staging_table=table, schema=self.vehdata_schema)
        )[0]

    def adjust_date(self, date: str = 'YYYY-MM-DD', table: str = 'raw_data'):
        self.store.run(sql.adjust_date(date_format=date, staging_table=table))

    def transform_data(self, file: pathlib.Path, checksum: bytes, table='raw_data') -> uuid.UUID:
        with self.store.cursor() as cur:
            print('extracting vehicles ...')
            cur.execute(
                sql.extract_vehicles(staging_table=table, schema=self.vehdata_schema)
            )
            new = ', '.join(f'{v[1]}-{v[2]} (id={v[0]})' for v in cur.fetchall())
            new = '(none)' if len(new) == 0 else new
            print(f'  new vehicles detected: {new}')

            print('extracting lines ...')
            cur.execute(
                sql.extract_lines(staging_table=table, schema=self.vehdata_schema)
            )
            new = ', '.join(f'{v[1]} (id={v[0]})' for v in cur.fetchall())
            new = '(none)' if len(new) == 0 else new
            print(f'  new lines detected: {new}')

            cur.execute(
                sql.extract_stops(staging_table=table, schema=self.vehdata_schema)
            )
            cur.execute(
                sql.extract_runs_with_timeframes(
                    staging_table=table, schema=self.vehdata_schema
                )
            )
            timeframes = cur.fetchall()

            cur.execute(
                sql.extract_pings(staging_table=table, schema=self.vehdata_schema)
            )

            cur.execute(
                sql.add_data_file(schema=self.vehdata_schema), (str(file), checksum)
            )
            file_id = cur.fetchone()[0]

            self.store.execute_batch(
                sql.add_file_timeframes(schema=self.vehdata_schema),
                [(str(file_id), *t) for t in timeframes],
                cursor=cur,
            )

            cur.execute(
                sql.drop_staging_table(staging_table=table, schema=self.vehdata_schema)
            )

        return file_id


@dagster.resource(required_resource_keys={'local_postgres'})
def edmo_vehdata(init_context: dagster.InitResourceContext) -> EDMOVehData:
    return EDMOVehData(init_context.resources.local_postgres)


@dagster.resource(required_resource_keys={'edmo_vehdata'})
def shortest_path_engine(
    context: dagster.InitResourceContext,
) -> NXPathFinderWithLocalCache:
    # set up shortest path engine over entire road graph
    with context.resources.edmo_vehdata.get_edgelist() as cur:
        return NXPathFinderWithLocalCache(cur)
