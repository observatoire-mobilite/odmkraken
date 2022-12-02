import typing
import dagster
import pandas as pd
import os
from pathlib import Path
import datetime

data_file = dagster.SourceAsset(
    key=dagster.AssetKey('raw_icts_data'),
    description='pings received from vehicle telemetry systems',
    metadata={'format': 'csv'},
    partitions_def=dagster.DailyPartitionsDefinition(start_date="2020-01-01")
)

@dagster.asset(
    ins={'dta': dagster.AssetIn('raw_icts_data', input_manager_key='pandas_csv_io_manager')},
    partitions_def=dagster.DailyPartitionsDefinition(start_date="2020-01-01")
)
def ping_record(context: dagster.OpExecutionContext, dta: pd.DataFrame) -> pd.DataFrame:
    return dta


class PandasCSVIOManager(dagster.IOManager):
    """Translates between Pandas DataFrames and CSVs on the local filesystem."""

    def _get_fs_path(self, asset_key: dagster.AssetKey, partition_key: typing.Optional[str]=None) -> Path:
        if partition_key:
            t = datetime.datetime.fromisoformat(partition_key)
            t += datetime.timedelta(days=5)
            rpath = Path(t.strftime('%Y%m%d180001_LUXGPS.csv.zip'))
            rpath = Path(r'P:\PM\Proj\18005 - Vitesses Bus RGTR\Doc\data-init-bus-hors-service') / rpath
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
    return [*dagster.with_resources([ping_record], resource_defs={'pandas_csv_io_manager': pandas_csv_io_manager}), data_file]