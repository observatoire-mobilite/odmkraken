import typing
import dagster
from . import EDMOData
import pendulum
import uuid
from datetime import datetime
from dataclasses import dataclass
import uuid
from mapmatcher.pathfinder.nx import NXPathFinderWithLocalCache
import pathlib

#TODO: if the deployment script allows chaning the schema names, they should be dynamic here as well
#TODO: harmonize style -> either put all SQL here, redistribute in code or convert to functions
SQL_TIMEFRAMES_FILE='select id, vehicle_id, time_start, time_end from vehdata.data_file_timeframes where file_id=%s'
SQL_TIMEFRAMES_PERIOD='select id, vehicle_id, time_start, time_end from vehdata.data_file_timeframes where time_start between %s and %s'
SQL_HALTS = 'insert into "vehdata"."halts" select * from "vehdata"."get_vehicle_halts"(%s, %s, %s)'

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


class EDMOVehData(EDMOData):
    
    def get_timeframes_by_file(self, file_id: uuid.UUID) -> typing.Iterator[VehicleTimeFrame]:
        """Return all vehicle timeframes contained in a given file."""
        with self.store.query(SQL_TIMEFRAMES_FILE, file_id) as cur:
            return [VehicleTimeFrame(*row) for row in cur]

    def get_timeframes_on(self, t0: pendulum.DateTime, tf: pendulum.DateTime) -> typing.Iterator[VehicleTimeFrame]:
        """Return all vehicle timeframes contained in a given file."""
        with self.store.query(SQL_TIMEFRAMES_PERIOD, t0, tf) as cur:
            return [VehicleTimeFrame(*row) for row in cur]

    def get_pings(self, tf: VehicleTimeFrame):
        """Get all pings making up `tf`."""
        cur = self.store.callproc('vehdata.get_pings', *tf.flat())
        if cur.rowcount < 2:
            raise RuntimeError('record has less than 2 pings')
        yield from cur

    def get_edgelist(self):
        """Retrieve the entire network as `nx`-style list of edges."""
        return self.store.callproc('vehdata.get_edgelist')

    def extract_halts(self, tf: VehicleTimeFrame):
        """Get all declared halts performed during `tf`."""
        with self.store.query(SQL_HALTS, *tf.flat()) as cur:
            yield from cur

    def get_nearby_roads(self, t: datetime, x: float, y: float):
        """Get roads physically clos to given point at given time"""
        with self.store.callproc('network.nearby_roads', x, y) as cur:
            yield from cur

    def check_file_already_imported(self, checksum: bytes):
        SQL = 'select * from vehdata.data_files where checksum=%s'
        with self.store.query(SQL, checksum) as cur:
            return cur.rowcount > 0

    def import_csv_file(self, handle, sep: str=','):
        self.store.run('select * from vehdata.create_staging_table()')
        self.store.copy_from(handle, ('vehdata', 'raw_data'), separator=sep)
        return self.store.fetchone('select count(*) from vehdata.raw_data')[0]
        
    def adjust_date(self, date: str='YYYY-MM-DD'):
        self.store.run('select * from vehdata.adjust_format(%s)', date)

    def transform_data(self, file: pathlib.Path, checksum: bytes):
        with self.store.cursor() as cur:
            print('extracting vehicles ...')
            cur.execute('select veh_id, veh_code, veh_plate from vehdata.extract_vehicles()')
            new = ', '.join(f'{v[1]}-{v[2]} (id={v[0]})' for v in cur.fetchall())
            new = '(none)' if len(new) == 0 else new
            print(f'  new vehicles detected: {new}')

            print('extracting lines ...')
            cur.execute('select line_id, line_code from vehdata.extract_lines()')
            new = ', '.join(f'{v[1]} (id={v[0]})' for v in cur.fetchall())
            new = '(none)' if len(new) == 0 else new
            print(f'  new lines detected: {new}')

            cur.execute('select * from vehdata.extract_stops()')
            cur.execute('select * from vehdata.extract_runs_with_timeframes();')
            timeframes = cur.fetchall()

            cur.execute('select * from vehdata.extract_pings()')
            
            sql = 'insert into "vehdata"."data_files" (id, filename, imported_on, checksum) values (gen_random_uuid(), %s, now(), %s) returning id'
            cur.execute(sql, (str(file), checksum))
            file_id = cur.fetchone()[0]
        
            sql = 'insert into "vehdata"."data_file_timeframes"(id, file_id, vehicle_id, time_start, time_end) values (gen_random_uuid(), %s, %s, %s, %s);'
            self.store.execute_batch(sql, [(str(file_id), *t) for t in timeframes], cursor=cur)
    
            cur.execute(f'drop table if exists "vehdata"."raw_data";')     
        

@dagster.resource(required_resource_keys={'postgres_connection'})
def edmo_vehdata(init_context: dagster.InitResourceContext) -> EDMOVehData:
    return EDMOVehData(init_context.resources.postgres_connection)


@dagster.resource(required_resource_keys={'edmo_vehdata'})
def shortest_path_engine(context: dagster.InitResourceContext) -> NXPathFinderWithLocalCache:
    # set up shortest path engine over entire road graph
    with context.resources.edmo_vehdata.get_edgelist() as cur:
        return NXPathFinderWithLocalCache(cur)
