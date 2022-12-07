import typing
import pandas as pd
import dagster
from datetime import datetime, timedelta
from pathlib import Path


class ICTSDataManager(dagster.IOManager):
    """Translates between Pandas DataFrames and CSVs on the local filesystem."""

    def _get_partition_filename(self, partition_key: str, i: int=1, extension: str='.csv.zip', dt: int=5) -> Path:
        """Convert partition key to file name using Init's naming convention.
        
        Data for the day referred to by `partition_key` is stored in 
        a file whose name starts with 8 digits that represent presumably
        the day of the file's creation, which seems to be 5 days after
        the data were recorded, i.e. `partition_key` + 5 days. That is
        followed by digits $180000 + i$ where $i$ is an integer and $i > 0$,
        where $i$ is usually $1$ but can take other values, either because
        there are several files for a date, or it sometimes just is 2.
        Finally, there is the constant suffix '_LUXGPS.csv.zip' 
        is the day on which the desired data were recorded (usually 3-4am
        to 4-5am the next day). The file naming con
        """
        t = datetime.fromisoformat(partition_key) + timedelta(days=dt)
        return Path(t.strftime(f'%Y%m%d{180000 + i}_LUXGPS')).with_suffix(extension)

    def _get_fs_path(self, asset_key: dagster.AssetKey, partition_key: typing.Optional[str]=None) -> Path:
        rpath = None
        if partition_key:
            t = datetime.fromisoformat(partition_key)
            t += timedelta(days=5)
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


class PandasCSVManager(dagster.IOManager):
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