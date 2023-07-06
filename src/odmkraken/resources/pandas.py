import typing
import pandas as pd
import dagster
from datetime import datetime, timedelta
from pathlib import Path


class ICTSDataManager(dagster.IOManager):
    """Translates between Pandas DataFrames and CSVs on the local filesystem."""

    def __init__(
        self, base_path: typing.Union[Path, str]=Path('.'),
        min_i: int=1, max_i: int=10,
        extension: str='.csv.zip',
        dt: int=5
    ):
        """Create new ICTSDataManager instance.
        
        Arguments:
            base_path: directory to search for data files (beware: no recursion!)
            min_i: the lowest index of any file in a partition (default = 1)
            max_i: the largest index to consider (default = 10)
            extension: file extension of data file (default = .csv.zip)
            dt: the `partition_key` corresponds to the date of the first
                ping in the data files. However, files are labeled with
                their export date. `dt` is the number of days between 
                those two dates (default=5).
        """
        super().__init__()
        self.root = Path(base_path)
        self.min_i = int(min_i)
        self.max_i = int(max_i)
        self.extension = str(extension)
        self.dt = int(dt)

    def handle_output(self, context: dagster.OutputContext, obj: pd.DataFrame):
        """Output is not implemented."""
        raise NotImplementedError('code was never intended to reproduce datafiles')

    def load_input(self, context: dagster.InputContext) -> pd.DataFrame:
        """This reads a dataframe from a CSV.
        
        Data for the day referred to by `context._asset_partition_key` is 
        stored in  a file whose name starts with 8 digits that represent 
        presumably the day of the file's creation, which seems to be 5 days after
        the data were recorded. That is followed by some code whose meaning
        I do not understand. Finally, there is the constant suffix 
        '_LUXGPS.csv.zip'. This just does `Path.glob` for that.
        """
        if not context.has_partition_key:
            raise NotImplementedError('code was never intended to be used without partitions')

        t = datetime.fromisoformat(context.asset_partition_key) + timedelta(days=self.dt)
        pattern = t.strftime(f'%Y%m%d*_LUXGPS{self.extension}')
        dta = [pd.read_csv(file, low_memory=False, sep=';', memory_map=True) for file in self.root.glob(pattern)]

        if not dta:
            raise dagster.Failure(
                description='No files found matching specified partition',
                metadata={
                    'problem': 'No files found matching specified partition',
                    'asset key': '/'.join(context.asset_key.path),
                    'partition key': str(context.asset_partition_key),
                    'search path': self.root.absolute(),
                    'search pattern': pattern
            })

        return pd.concat(dta)


@dagster.io_manager(config_schema={'base_path': dagster.Field(dagster.StringSource, is_required=False)})
def icts_data_manager(init_context: dagster.InitResourceContext):
    """Create a configured ICTS data manager."""
    root = init_context.resource_config.get('base_path', '.')
    return ICTSDataManager(base_path=root)


class PandasParquetManager(dagster.IOManager):
    """Saves and retrieves Pandas dataframes as parquet from disk."""
    def __init__(self, base_path: typing.Union[Path, str]=Path('.')):
        self.base_path = Path(base_path).absolute()

    def filename(self, asset_key: dagster.AssetKey, partition_key: typing.Optional[str]=None) -> Path:
        """Return target path for specified asset."""
        parts = list(asset_key.path)
        if partition_key:
            t = datetime.fromisoformat(partition_key)
            parts.append(t.strftime('%Y%m%d'))
        file = Path(*parts)
        return self.base_path / file.with_suffix('.parquet')

    def handle_output(self, context: dagster.OutputContext, obj: pd.DataFrame):
        """This saves the dataframe as a CSV."""
        partition = context.asset_partition_key if context.has_partition_key else None
        file = self.filename(context.asset_key, partition)
        file.parent.mkdir(parents=True, exist_ok=True)
        obj.to_parquet(file)

    def load_input(self, context: dagster.InputContext) -> pd.DataFrame:
        """This reads a dataframe from a CSV."""
        partition = context.asset_partition_key if context.has_partition_key else None
        return pd.read_parquet(self.filename(context.asset_key, partition))


@dagster.io_manager(config_schema={'base_path': dagster.Field(str, is_required=False)})
def pandas_parquet_manager(init_context: dagster.InitResourceContext):
    """Create a configured ICTS data manager."""
    root = init_context.resource_config.get('base_path', '.')
    return PandasParquetManager(base_path=root)
