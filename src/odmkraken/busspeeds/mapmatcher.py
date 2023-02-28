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


class MPContext:

    _singleton: typing.Optional['MPContext']=None

    def __new__(cls, **kwargs):
        if MPContext._singleton is None:
            ctx = super().__new__(cls)
            for arg, val in kwargs.items():
                setattr(ctx, arg, val)
            cls._singleton = ctx
        return cls._singleton


class DB:

    def __init__(self, *args, **kwargs):
        self.conn = psycopg2.connect(*args, **kwargs)

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

    @contextmanager
    def get_pings(self, tf: VehicleTimeFrame):
        """Get all pings making up `tf`."""
        with self.callproc('vehdata.get_pings', *tf.flat()) as cur:
            if cur.rowcount < 2:
                raise RuntimeError('Invalid data frame: has less than 2 pings')
            yield cur

    def get_edgelist(self):
        """Retrieve the entire network as `nx`-style list of edges."""
        return self.callproc('network.get_edgelist')

    def get_nearby_roads(self, t: datetime, x: float, y: float, radius: float=90):
        """Get roads physically clos to given point at given time"""
        roads, r = [], 0  # just to please pyright
        for r in (100, 200, 500, 1000):
            with self.callproc('network.nearby_roads', float(x), float(y), float(radius)) as cur:
                roads = [candidate_solution(*r, t) for r in cur]
            if roads:
                break
        if (r > 100):
            print(f'Increased search radius to {r:.1f}m around (x={x:.1f}, y={y:.1f}) at t={t}')
        return roads


def init_worker(dsn: str):
    db = DB(dsn)
    with db.get_edgelist() as cur:
        path_finder = NXPathFinderWithLocalCache(cur)
    MPContext(db=db, path_finder=path_finder)


def mapmatch_timeframe(tf: VehicleTimeFrame):

    db = MPContext().db
    t0 = time.perf_counter()

    # do the actual mapmatching
    with db.get_pings(tf) as cur:
        path = reconstruct_optimal_path(cur, db.get_nearby_roads, 
            shortest_path_engine=MPContext.path_finder, 
            scorer=Scorer()
        )

    # reproject in the perspective of the road segments
    projector = ProjectLinear()
    pathway = projector.project(path)

    # convert results and store
    results = [(tf.vehicle_id, *p) for p in pathway]
    db.store_results(results)

    return (tf.vehicle_id, tf.time_from, tf.time_to, path.times[-1], time.perf_counter() - t0)


@dagster.asset(partitions_def=busdata_partition, config_schema={'edmo_dsn': dagster.StringSource})
    
    # retrieve timeframes
    dates = context.op_config['date_from'], context.op_config['date_to']
    dsn = context.op_config['edmo_dsn']
    frames = vehicle_timeframes.itertuples(index=False)
    
    # map to multi-prcessing
    with mp.Pool(initializer=init_worker, initargs=(dsn, )) as pool:
        res = pool.starmap(mapmatch_timeframe, frames)
    return pd.DataFrame(res, columns=('vehicle_id', 'time_from', 'time_to', 
                                      'last_matched', 'computation_time'))


