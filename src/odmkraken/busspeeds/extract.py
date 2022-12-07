"""Load INIT CSV files."""
import typing
import dagster
import pandas as pd


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
def normalized_ping_record(
    context: dagster.OpExecutionContext,
    dta: pd.DataFrame
) -> typing.Iterator[dagster.Output]:
    """Normalize ICTS input data."""
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
    fields = ['vehicle', 'time', 'type', 'longitude', 'latitude',
              'stop', 'expected_time', 'count_people_boarding',
              'count_people_disembarking']
    pings_from_stops = dta.query('type!=-1')[fields]
    pings_from_stops['type'].replace({
        0: 'geplante Haltestelle + Fahrplanpunkt',
        1: 'Bedarfshaltestelle (geplant)',
        2: 'ungeplante Haltestelle + Tür offen',
        3: 'Störungspunkt',
        4: 'Durchfahrt ohne Fahrgastaufnahme',
        5: 'Haltestelle + kein Fahrplanpunkt',
        6: 'Durchfahrt ohne Fahrgastaufnahme oder Fahrplanpunkt'
    }, inplace=True)
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
    try:
        dta = dta.rename(columns=fields, errors='raise')
    except KeyError as err:
        raise dagster.Failure(
            description='Input malformed: one or several columns missing.',
            metadata={
                'missing columns': str(err),
                'detected columns': dta.columns
            }
        )

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
    def seq(group):
        d = group[['line', 'sortie', 'run']]
        return d.ne(d.shift()).any(axis=1).cumsum()

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
    # permanently add run_id to `dta` to do the following in one simple group-by
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

    # don't need it anymore (just in case)
    dta = dta.drop('run_id', axis=1)

    # adjust types (category doesn't save that much anymore)
    for field in ('vehicle', 'line', 'sortie'):
        runs[field] = runs[field].astype('str')
    runs['run'] = runs['run'].astype('Int32')

    # make sortie numeric
    r = runs['sortie'].str.extract('(?P<sortie_flag>[A-Z]*)(?P<sortie>[0-9]*)')
    runs['sortie_flag'] = r['sortie_flag'].astype('category')
    runs['sortie'] = r['sortie'].astype('int32')

    return runs

