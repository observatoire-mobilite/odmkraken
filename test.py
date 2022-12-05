import typing
import dagster
import pandas as pd
import os
from pathlib import Path
import datetime

data_file = dagster.SourceAsset(
    key=dagster.AssetKey('icts_data_dump'),
    description='pings received from vehicle telemetry systems',
    metadata={'format': 'csv'},
    partitions_def=dagster.DailyPartitionsDefinition(start_date="2020-01-01")
)

@dagster.asset(
    ins={'dta': dagster.AssetIn('icts_data_dump', input_manager_key='pandas_csv_io_manager')},
    partitions_def=dagster.DailyPartitionsDefinition(start_date="2020-01-01")
)
def normalized_ping_record(context: dagster.OpExecutionContext, dta: pd.DataFrame) -> pd.DataFrame:
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
    context.log.debug('adjusting date format ...')
    datum = pd.to_datetime(dta.pop('date'))
    for field in ('time', 'expected_time'):
        dta[field] = datum + pd.to_timedelta(dta[field])

    # reduce to categoricals
    context.log.debug('adjusting column types ...')
    for field in ('type', 'vehicle', 'line', 'sortie', 'run', 'stop'):
        dta[field] = dta[field].astype('category')

    # adjust for European number format
    for field in ('latitude', 'longitude'):
        dta[field] = dta[field].str.replace(',', '.').astype('float')

    # adjust format (should already have been done, but just to be safe)
    for field  in ('count_people_boarding', 'count_people_disembarking'):
        dta[field] = dta[field].astype('Int16')

    return dta


class PandasCSVIOManager(dagster.IOManager):
    """Translates between Pandas DataFrames and CSVs on the local filesystem."""

    def _get_fs_path(self, asset_key: dagster.AssetKey, partition_key: typing.Optional[str]=None) -> Path:
        rpath = None
        if partition_key:
            t = datetime.datetime.fromisoformat(partition_key)
            t += datetime.timedelta(days=5)
            for i in range(1, 3):
                rpath = Path(t.strftime(f'%Y%m%d18000{i}_LUXGPS.csv.zip'))
                rpath = Path(r'P:\PM\Proj\18005 - Vitesses Bus RGTR\Doc\data-init-bus-hors-service') / rpath
                if rpath.exists():
                    break
        else:
            rpath = Path(os.path.join(*asset_key.path) + ".csv")
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


@dagster.repository
def repo():
    return [*dagster.with_resources([normalized_ping_record], resource_defs={'pandas_csv_io_manager': pandas_csv_io_manager}), data_file]