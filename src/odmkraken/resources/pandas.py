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

    def files_in_partition(self, partition_key: str, must_exist: bool=True) -> typing.Iterator[Path]:
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
        to 4-5am the next day).

        Arguments:
            partition_key: the intended partition
            must_exist: if False, yield also files that do not exist
        """
        for i in range(self.min_i, self.max_i + 1):
            t = datetime.fromisoformat(partition_key) + timedelta(days=self.dt)
            file = Path(t.strftime(f'%Y%m%d{180000 + i}_LUXGPS'))
            file = self.root / file.with_suffix(self.extension)
            if must_exist and not file.exists():
                continue
            yield file

    def handle_output(self, context: dagster.OutputContext, obj: pd.DataFrame):
        """Output is not implemented."""
        raise NotImplementedError('code was never intended to reproduce datafiles')

    def load_input(self, context: dagster.InputContext) -> pd.DataFrame:
        """This reads a dataframe from a CSV."""
        if not context.has_partition_key:
            raise NotImplementedError('code was never intended to be used without partitions')

        dta = [pd.read_csv(file, low_memory=False, sep=';', memory_map=True)
               for file in self.files_in_partition(context.asset_partition_key)]

        if not dta:
            files = list(self.files_in_partition(context.asset_partition_key, must_exist=False))
            raise dagster.Failure(
                description='No files found matching specified partition',
                metadata={
                    'problem': 'No files found matching specified partition',
                    'asset key': '/'.join(context.asset_key.path),
                    'partition key': str(context.asset_partition_key),
                    'search path': self.root.absolute(),
                    'first file checked': files[0].name,
                    'last file checked': files[-1].name
            })

        return pd.concat(dta)


@dagster.io_manager(config_schema={'base_path': dagster.Field(str, is_required=False)})
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
