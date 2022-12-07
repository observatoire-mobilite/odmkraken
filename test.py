import datetime
import io
import os
import typing

import dagster
import pandas as pd

from pathlib import Path


data_file = dagster.SourceAsset(
    key=dagster.AssetKey('icts_data_dump'),
    description='pings received from vehicle telemetry systems',
    metadata={'format': 'csv'},
    partitions_def=dagster.DailyPartitionsDefinition(start_date="2020-01-01")
)


class PandasCSVIOManager(dagster.IOManager):
    """Translates between Pandas DataFrames and CSVs on the local filesystem."""

    def _get_fs_path(self, asset_key: dagster.AssetKey, partition_key: typing.Optional[str]=None) -> Path:
        rpath = None
        if partition_key:
            t = datetime.datetime.fromisoformat(partition_key)
            t += datetime.timedelta(days=5)
            for i in range(1, 3):
                rpath = Path(t.strftime(f'%Y%m%d18000{i}_LUXGPS.csv.zip'))
                #rpath = Path(r'P:\PM\Proj\18005 - Vitesses Bus RGTR\Doc\data-init-bus-hors-service') / rpath
                rpath = Path('/Users/ggeorges/Documents/staat/bus_data') / rpath
                if rpath.exists():
                    break
        else:
            rpath = Path(os.path.join(*asset_key.path) + ".csv")
        if not rpath.exists():
            raise dagster.Failure(
                description='Unable to locate source file',
                metadata={
                    'problem': 'Source file does not exist',
                    'path': str(rpath),
                    'asset key': asset_key,
                    'partition key': partition_key
            })
        dagster.get_dagster_logger().info(f"using path of: {rpath} for asset_key: {asset_key}")
        return rpath.absolute()

    def handle_output(self, context: dagster.OutputContext, obj: pd.DataFrame):
        """This saves the dataframe as a CSV."""
        fpath = self._get_fs_path(context.asset_key, context.asset_partition_key if context.has_partition_key else None)
        obj.to_csv(fpath, index=False)

    def load_input(self, context: dagster.InputContext) -> pd.DataFrame:
        """This reads a dataframe from a CSV."""
        file = self._get_fs_path(context.asset_key, context.asset_partition_key if context.has_partition_key else None)
        return pd.read_csv(file, low_memory=False, sep=';', memory_map=True)


@dagster.io_manager
def pandas_csv_io_manager():
    return PandasCSVIOManager()


@dagster.multi_asset(
    ins={'dta': dagster.AssetIn('icts_data_dump', input_manager_key='pandas_csv_io_manager')},
    outs={
        'runs': dagster.AssetOut(
            dagster_type=pd.DataFrame,
            io_manager_key='pandas_postgres_io_manager'
        ), 'pings': dagster.AssetOut(
            dagster_type=pd.DataFrame,
            io_manager_key='pandas_postgres_io_manager'
        ), 'pings_from_stops': dagster.AssetOut(
            dagster_type=pd.DataFrame,
            io_manager_key='pandas_postgres_io_manager'
        ), 'duplicate_pings': dagster.AssetOut(
            dagster_type=pd.DataFrame, is_required=False,
            io_manager_key='pandas_csv_io_manager'
        )
    },
    partitions_def=dagster.DailyPartitionsDefinition(start_date="2020-01-01")
)
def normalized_ping_record(context: dagster.OpExecutionContext, dta: pd.DataFrame) -> typing.Iterator[dagster.Output]:
    
    # adjust raw format to something meaningful
    dta = adjust_format(dta)

    # report any duplicate records
    duplicates = find_duplicates(dta)
    if len(duplicates) > 0:
        context.log.warn(f'Found {len(duplicates)} duplicate records; '
                         'a record of all concerned events will be kept.')
        yield dagster.Output(
            value=duplicates, output_name='duplicate_pings', 
            metadata={
                'number of duplicate events':  len(duplicates),
                'total number of events': len(dta),
                'vehicles with duplicates': len(duplicates.index.get_level_values(0).unique())
        })

    # just drop duplicates
    dta.drop_duplicates(['vehicle', 'time'], keep='last', inplace=True)

    # export table of location pings
    pings = dta.query('type==-1')[['vehicle', 'time', 'longitude', 'latitude']]
    yield dagster.Output(
        value=pings,
        output_name='pings',
        metadata={
            'number of records': len(pings),
            'earliest record': str(pings['time'].min()),
            'latest record': str(pings['time'].max()),
            'number of vehicles': len(pings.vehicle.unique())
        }
    )

    # export table of pings at stops
    pings_from_stops = dta.query('type!=-1')[['vehicle', 'time', 'type', 'longitude', 'latitude', 'stop', 'expected_time', 'count_people_boarding', 'count_people_disembarking']]
    yield dagster.Output(
        value=pings_from_stops,
        output_name='pings_from_stops',
        metadata={
            'number of records': len(pings_from_stops),
            'earliest record': str(pings_from_stops['time'].min()),
            'latest record': str(pings_from_stops['time'].max()),
            'number of vehicles': len(pings_from_stops.vehicle.unique())
        }        
    )

    # export mission meta data
    runs = runs_table(dta)
    yield dagster.Output(
        value=runs,
        output_name='runs',
        metadata={
            'number or runs': len(runs),
            'earliest record': str(runs['time_start'].min()),
            'latest record': str(runs['time_end'].max()),
            'number of vehicles': len(runs.vehicle.unique())
        }
    )


def adjust_format(dta: pd.DataFrame):
    """Convert data read from CSV to something meaningful.
    
    The provided CSV files have annoying formatting issues, primarily due to
    the usage of European number and date formats. That is why inputs in `dta`
    are mostly raw `str` that are then interpreted by this function. It also
    convertes repetitive columns, such as those identifying vehicles, to
    `category` for improved memory efficiency.
    
    Arguments:
        dta: the input data read as read from CSV
    """
    # rename fields to system names
    fields = {
        'TYP': 'type',
        'DATUM': 'date',
        'SOLLZEIT': 'expected_time',
        'ZEIT': 'time',
        'FAHRZEUG': 'vehicle',
        'LINIE': 'line',
        'UMLAUF': 'sortie',
        'FAHRT': 'run',
        'HALT': 'stop',
        'LATITUDE': 'latitude',
        'LONGITUDE': 'longitude',
        'EINSTEIGER': 'count_people_boarding',
        'AUSSTEIGER': 'count_people_disembarking'
    }
    dta = dta.rename(columns=fields)

    # extract date
    datum = pd.to_datetime(dta.pop('date'), dayfirst=True)
    for field in ('time', 'expected_time'):
        dta[field] = datum + pd.to_timedelta(dta[field])

    # reduce to categoricals
    for field in ('type', 'vehicle', 'line', 'sortie', 'run', 'stop'):
        dta[field] = dta[field].astype('category')

    # adjust for European number format
    for field in ('latitude', 'longitude'):
        dta[field] = dta[field].str.replace(',', '.').astype('float')

    # adjust format (should already have been done, but just to be safe)
    for field  in ('count_people_boarding', 'count_people_disembarking'):
        dta[field] = dta[field].astype('Int16')

    # sort
    return dta.sort_values(['vehicle', 'time'])


def find_duplicates(dta: pd.DataFrame) -> pd.DataFrame:
    """Detect duplicate entries for the same vehicle."""
    d = dta.groupby(['vehicle', 'time'], observed=True)[['type']].count().query('type > 1')
    return dta.set_index(['vehicle', 'time']).loc[d.index]


def identify_runs(dta: pd.DataFrame) -> pd.Series:
    """Add a unique identify for every vehicle run.
    
    In this context, a run is defined as a period of time during which
    the `sortie`, `run` and `line` values remain unchanged for a given
    vehicle. This function returns a `pd.Series` that identifies runs
    for a given vehicle. The identifier is an integer, that starts at
    one and increments every time any of the three aforementioned 
    variables changes from one ping to the next (i.e. if the columns
    revert back to a combination of values that was seen before,
    there will be a new run id).
    """
    # extract run-id for every vehicle
    # kind-of-equivalent to using sequences in SQL
    def seq(r):
        d = r[['line', 'sortie', 'run']]
        d = d.ne(d.shift()).any(axis=1).cumsum()
        return d
    
    return (dta.groupby('vehicle', group_keys=False, observed=True)
            .apply(seq).astype('category'))


def runs_table(dta: pd.DataFrame) -> pd.DataFrame:
    """Derives the `runs` table from a `dta` pings record.
    
    The `runs` table describes the operational state of a vehicle
    while driving, namely the `sortie`, `run` and `line` numbers
    it had at a particular time. Since those change rarely, at least
    compared to the frequency of pings, it is more efficient to store 
    the period of time during which a particular combination of values 
    were active, than store the values for every single ping. This
    function produces such a table from the ping data record `dta`.
    """

    dta['run_id'] = identify_runs(dta)

    # `observed` is important as otherwise to avoid unused combinations of run, vehicle and line
    # `dropna` must be deactivated to include runs with run and line NaN (if sortie=RGTR|CFL|TICE0)
    runs = (dta.groupby(['vehicle', 'run_id', 'sortie', 'run', 'line'], observed=True, dropna=False)
            .agg({'time': ['min', 'max']})
            .droplevel(0, axis=1)
            .rename(columns={'min': 'time_start', 'max': 'time_end'})
            .reset_index()
            .drop('run_id', axis=1)
    )
    
    # adjust types
    for field in ('vehicle', 'line', 'sortie'):
        runs[field] = runs[field].astype('str')
    runs['run'] = runs['run'].astype('Int32')
    
    # make sortie field numeric
    r = runs['sortie'].str.extract('(?P<sortie_flag>[A-Z]*)(?P<sortie>[0-9]*)')
    runs['sortie_flag'] = r['sortie_flag'].astype('category')
    runs['sortie'] = r['sortie'].astype('int32')
    return runs


class PandasPostgresIOManager(dagster.IOManager):
    """Translates between Pandas DataFrames and CSVs on the local filesystem."""
    def __init__(self, store: 'odmkraken.resources.postgres.PostgresConnector', *args, **kwargs):
        self.store = store

    def handle_output(self, context: dagster.OutputContext, obj: pd.DataFrame):
        """This saves the dataframe as a CSV."""
        table = context.asset_key
        with io.BytesIO() as handle:
            obj.to_csv(handle, header=False, index=False, mode='wb')
            self.store.copy_from(handle, table)

    def load_input(self, context: dagster.InputContext) -> pd.DataFrame:
        """This reads a dataframe from a CSV."""
        raise NotImplementedError()


@dagster.io_manager(required_resource_keys={'local_postgres'})
def pandas_postgres_io_manager(init_context: dagster.InitResourceContext):
    return PandasPostgresIOManager(init_context.resources.local_postgres)


class Pg:

    def copy_from(self, handle, table):
        print(handle, table)

@dagster.resource
def dummy_postgres():
    return Pg()


@dagster.repository
def repo():
    return dagster.with_resources(
        [normalized_ping_record, data_file], 
        resource_defs={
            'pandas_csv_io_manager': pandas_csv_io_manager,
            'pandas_postgres_io_manager': pandas_postgres_io_manager,
            'local_postgres': dummy_postgres
        }
    )