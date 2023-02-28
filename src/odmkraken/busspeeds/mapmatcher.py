import typing
from datetime import datetime, timedelta
import time
from mapmatcher import Itinerary, reconstruct_optimal_path, ProjectLinear, Scorer, candidate_solution
from mapmatcher.pathfinder.nx import NXPathFinderWithLocalCache
import mapmatcher.errors
from odmkraken.resources.edmo.busdata import VehicleTimeFrame
from .common import busdata_partition
from functools import partial
import multiprocessing as mp
from dataclasses import dataclass
import uuid
import psycopg2
from contextlib import contextmanager
import dagster
import pandas as pd
from pathlib import Path
import tempfile


class MPContext:
    """Singleton class/object to store other objects.

    This is a little hack to transfer objects created by `multiprocessing.Pool`'s `initializer`
    to the later worker processes. It has a very simple interface: when an object is first
    initialized from `MPContext`, that object, along with any values passed as keyword parameters,
    will be stored as class variables. Any subsequent calls to `MPContext`'s `__init__` will
    always return that first object, which now has the original keyword attributes (and their
    values) as attributes (resp. their values).
    """

    _singleton: typing.Optional['MPContext']=None

    def __new__(cls, **kwargs):
        if MPContext._singleton is None:
            ctx = super().__new__(cls)
            for arg, val in kwargs.items():
                setattr(ctx, arg, val)
            cls._singleton = ctx
        return cls._singleton


class DB:
    """A lean database wrapper.

    This is more or less a re-implementation of the `edmo` `DB` class, but which greatly simplifies
    running individual workers with database access as their own processes. It provides the same
    API, including a self-resetting `cursor` function with optional named-cursor support, `callproc`
    and access to commonly required SQL calls.
    """

    def __init__(self, *args, proc_edgelist: str='network.get_edgelist', 
                 proc_nearby_roads: str='network.nearby_roads', **kwargs):
        """Creates a new database connection.

        All arguments and keywords are piped through straight to `psycopg2.connect`,
        so see its documentation for API, with the exception of:

        Arugments:
            - proc_edgelist: name (str) of the stored procedure returining the routable graph
                for use with the `networkx` shortest-path finder.
            - proc_nearby_road: name (str) of the stored procedure returning the set of candidate
                roads next to a specified point (= current ping).
        """
        self.conn = psycopg2.connect(*args, **kwargs)
        self.proc_edgelist = str(proc_edgelist)
        self.proc_nearby_roads = str(proc_nearby_roads)

    @contextmanager
    def cursor(self, name: typing.Optional[str]=None):
        """Get a cursor with error handling."""
        cur = self.conn.cursor(name=name)
        try:
            yield cur
        except psycopg2.Error:
            self.conn.rollback()
            raise
        else:
            self.conn.commit()
        finally:
            cur.close()

    @contextmanager
    def query(self, sql: str, *args, **kwargs):
        """Get a cursor and execute a query on it."""
        with self.cursor(**kwargs) as cur:
            cur.execute(sql, args)
            yield cur

    @contextmanager
    def callproc(self, proc: str, *args, **kwargs):
        """Get a cursor and execute a stored procedure on it."""
        with self.cursor(**kwargs) as cur:
            cur.callproc(proc, args)
            yield cur

    def get_edgelist(self):
        """Retrieve the entire network as `nx`-style list of edges."""
        return self.callproc(self.proc_edgelist)

    def get_nearby_roads(self, t: datetime, x: float, y: float, radius: float=90):
        """Get roads physically clos to given point at given time"""
        roads, r = [], 0  # just to please pyright
        for r in (100, 200, 500, 1000):
            with self.callproc(self.proc_nearby_roads, float(x), float(y), float(radius)) as cur:
                roads = [candidate_solution(*r, t) for r in cur]
            if roads:
                break
        if (r > 100):
            print(f'Increased search radius to {r:.1f}m around (x={x:.1f}, y={y:.1f}) at t={t}')
        return roads


def init_worker(dsn: str, pings: pd.DataFrame, outputpath: Path):
    """Initialize DB connection and pre-load road network."""
    try:
        db = DB(dsn)
        with db.get_edgelist() as cur:
            path_finder = NXPathFinderWithLocalCache(cur)
        MPContext(db=db, path_finder=path_finder, pings=pings, outputpath=outputpath)
    except Exception as e:
        print(f'ERROR: initializing workers failed: {e}')
        # this is somewhat of a hack, to avoid `multiprocessing.Pool`'s poor exception
        # within the `initializer` callback.
        MPContext(error=str(e))


def mapmatch_timeframe(vehicle: str):
    """Mapmatch pings within one given timeframe."""

    # hack
    if hasattr(MPContext(), 'error'):
        print(f'WARN: skipping {vehicle} since worker initialization failed')
        return (None, vehicle, 0)

    t0 = time.perf_counter()
    pings = MPContext().pings.query('vehicle==@vehicle') 
    # turns out that reading and then filtering is about 10 times slower on actual ping
    # files than would be partitioned files. However, that is still just 0.4 seconds.
    # Reading in all partitions is much, much slower

    # do the actual mapmatching
    path = reconstruct_optimal_path(
        pings,
        MPContext().db.get_nearby_roads, 
        shortest_path_engine=MPContext().path_finder, 
        scorer=Scorer()
    )

    # reproject in the perspective of the road segments
    projector = ProjectLinear()
    pathway = pd.DataFrame(projector.project(path),
                        columns=['node_from', 'node_to', 'time', 'dt_stay'])

    # write to disk, just to be safe
    outfile = (MPContext().outputpath / vehicle).with_suffix('.parquet')
    pathway.to_parquet(outfile)

    # return some summary stats
    return (outfile, vehicle, time.perf_counter() - t0)
    


@dagster.asset(
    partitions_def=busdata_partition, 
    config_schema={
        'network_db_dsn': dagster.Field(
            dagster.StringSource, 
            dscription='DSN to the PostGIS enabled database providing the `network.get_edgelist` and `network.get_nearby_roads` procedures.'
        )
    }
)
def most_likely_path(context: dagster.OpExecutionContext, pings: pd.DataFrame) -> dagster.Output[pd.DataFrame]:
    """Reconstruct most likely path through road network."""
    
    # retrieve timeframes
    dsn = context.op_config['network_db_dsn']
    vehicles = pings.groupby('vehicle').agg({'time': 'max', 'latitude': len}).rename(columns={'time': 'time_of_last_ping', 'latitude': 'number_of_pings'})
    
    # map to multi-prcessing
    with tempfile.TemporaryDirectory() as temp:
        
        # create result files and keep them in local temp directory
        filepath = Path(temp)
        with mp.Pool(initializer=init_worker, initargs=(dsn, pings, filepath)) as pool:
            outcome = pool.map(mapmatch_timeframe, vehicles.index)
            # TODO: replace by an own implementation with proper exception handling
            # on initialization (db inavailability is a concern)
        
        # rearrange outcome
        files = {vehicle: resfile for resfile, vehicle, _ in outcome if resfile}
        comptimes = pd.Series({vehicle: compute_time for _, vehicle, compute_time in outcome})
        if not files:
            raise dagster.Failure(
                description=('The workers did not return a single file. This typically happens if the '
                             'input file indeed contains no data or no valid data (check metadata) '
                             'or because initializing the workers failed.'), 
                metadata={
                    'number_of_vehicles': len(comptimes),
                }
            )

        # gather all results
        res = (pd.concat({vehicle: pd.read_parquet(resfile) for vehicle, resfile in files.items()},
                        names=['vehicle', 'index'])
            .reset_index().drop('index', axis=1))

    stats = vehicles.merge(
        res.groupby('vehicle')
        .agg({'time': 'max', 'node_from': 'count'})
        .rename(columns={'time': 'time_of_last_mapmatched', 'node_from': 'number_of_matched_pings'}),
        left_index=True, right_index=True
    ).reset_index()
    stats['compute_time'] = comptimes
    stats['time_of_last_ping'] = stats['time_of_last_ping'].astype(str)
    stats['time_of_last_mapmatched'] = stats['time_of_last_mapmatched'].astype(str)
    stats = stats.reset_index().to_json(orient='records')
    
    return dagster.Output(
        value=res, 
        metadata={
            'report': stats
        }
    )
