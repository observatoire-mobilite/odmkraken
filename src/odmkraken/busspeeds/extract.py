"""Load INIT CSV files."""
import typing
import pathlib
import zipfile
import re
import hashlib
import math
import uuid
from datetime import datetime
import dagster
from dataclasses import dataclass

TBL = typing.List[typing.Tuple[int, datetime, datetime]]
CSV_REQUIRED_FIELDS = {"TYP", "DATUM", "SOLLZEIT", "ZEIT", "FAHRZEUG", "LINIE", "UMLAUF", "FAHRT", "HALT", "LATITUDE", "LONGITUDE", "EINSTEIGER", "AUSSTEIGER"}
CSV_KNOWN_SEPARATORS = [',', ';', '\t']

class FileAlreadyImportedError(Exception):

    def __init__(self, file: pathlib.Path, checksum: typing.Optional[bytes]=None):
        self.file = file
        self.checksum = checksum
        msg = f'File with identical checksum as `{file}` was already successfully imported'
        super().__init__(msg)


@dataclass
class NewData:
    file: pathlib.Path
    checksum: bytes
    sep: str
    date: str
    lines: typing.Optional[int] = None


@dagster.op(required_resource_keys={'edmo_vehdata'}, config_schema={'file': str})
def extract_from_csv(context: dagster.OpExecutionContext) -> NewData:
    """Import vehicle data from a zipped CSV data-file.

    The task copies over all of the raw data onto a temporary
    table on the database server.

    Note that if any exception occurs at any stage, the entire process
    is rolled back, ensuring the integrity of the existing data.
    Also note that one of the actions triggering such a rollback is
    re-importing already imported data. 
    """
    file = pathlib.Path(context.op_config['file'])
    n_bytes, handle = open_file(file)
    if n_bytes == 0:
        context.log.warn(f'file is either empty or not a zip-file.')
    else:
        context.log.debug(f'file size: {human_readable_bytes(n_bytes)}')
    format = infer_format(handle)
    msg = ', '.join(f'{k}=`{v}`' for k, v in format.items())
    context.log.debug(f'inferred format spec: {msg}')
    checksum = compute_checksum(handle)
    context.log.debug(f'checksum: {checksum}')
    nd = NewData(file, checksum, **format)

    # ensure we are importing a thusfar unknown file
    if context.resources.edmo_vehdata.check_file_already_imported(nd.checksum):
        raise FileAlreadyImportedError(file, checksum)

    # dump contents of file into a newly created table
    context.log.info('loading raw data into staging table ...')
    nd.lines = context.resources.edmo_vehdata.import_csv_file(handle, sep=nd.sep)
    context.log.info(f'ingested {nd.lines} lines')

    return nd


@dagster.op(required_resource_keys={'edmo_vehdata'})
def adjust_dates(context: dagster.OpExecutionContext, nd: NewData) -> NewData:
    context.log.info('adjusting staging table\'s date columns ...')
    context.resources.edmo_vehdata.adjust_date(nd.date)
    return nd


@dagster.op(required_resource_keys={'edmo_vehdata'})
def load_data(context: dagster.OpExecutionContext, nd: NewData):
    context.log.info('loading data into analytical tables ...')
    context.resources.edmo_vehdata.transform_data(nd.file, nd.checksum)


@dagster.graph()
def icts_data():
    load_data(adjust_dates(extract_from_csv()))


class UnusableZipFile(Exception):

    def __init__(self, file: pathlib.Path, n_files: int):
        self.file = file
        self.n_files = int(n_files)
        super().__init__(f'zip-file `{self.file} contains {n_files} files, but expected was exactly one')


def open_file(file: pathlib.Path) -> typing.Tuple[int, typing.IO]:
    if not zipfile.is_zipfile(file):
        return 0, file.open('rb')

    archive = zipfile.ZipFile(file)
    if len(archive.filelist) != 1:
        raise UnusableZipFile(file, n_files=len(archive.filelist))
    n_bytes = archive.filelist[0].file_size
    return n_bytes, archive.open(archive.filelist[0], 'r')

def human_readable_bytes(n_bytes: int) -> str:
    if n_bytes < 1:
        return '0 bytes'
    k = math.floor(math.log10(n_bytes) / 3)
    suffix = ['bytes', 'kB', 'MB', 'GB', 'TB'][k]
    if suffix == 'bytes':
        return f'{n_bytes} bytes'
    return f'{n_bytes * 10**(-k*3):.2f} {suffix}'


class CSVFormatProblem(Exception):
    pass


class HeaderLacksField(CSVFormatProblem):

    def __init__(self, fields: typing.Sequence[str]):
        self.fields = list(fields)
        msg = ', '.join(fields)
        super().__init__(f'input file header lacks the following field(s): {msg}')


class UnkownCSVDialect(CSVFormatProblem):
    
    def __init__(self):
        super().__init__('file is not in CSV format, or uses an unkown dialect. It might either use a separator other than "," and ";", or it quotes and/or whitespace rules wrong.')
    

class UnsupportedDateFormat(CSVFormatProblem):

    def __init__(self, date: str):
        self.date = date
        super().__init__(f'Unable to infer date format from "{date}". See documentation of `odmkraken.extract.infer_format`.')


def infer_format(handle: typing.IO):
    # TODO: this is not very robust. But instead of developing it further
    # we should take this to the next level and invest in a pandas-based
    # workflow leveraging panderas or great expectations.
    seps = '|'.join(CSV_KNOWN_SEPARATORS)
    lines = [handle.readline().decode('utf-8').strip() for i in range(3)]
    
    match = re.match(f'[a-zA-Z0-9_]+["\']? *({seps})', lines[0])
    if not match:
        raise UnkownCSVDialect()
    format = {'sep': match.group(1)}
    
    header = re.split(f'\W+', lines[0])
    header = [f.strip('"\',;') for f in header]
    missing = CSV_REQUIRED_FIELDS.difference(header)
    if missing:
        raise HeaderLacksField(iter(missing))

    i_datum = header.index('DATUM')
    datum = lines[1].split(format['sep'])[i_datum]
    
    if re.match(r'\d{2}\.\d{2}\.\d{4}$', datum):
        format['date'] = 'DD.MM.YYYY'
    elif re.match(r'\d{2}-[A-Z]{3}-\d{2}$', datum):
        format['date'] = 'DD-MON-YY'
    elif re.match(r'\d{2}-[A-Z][a-z]{2}-\d{2}$', datum):
        format['date'] = 'DD-Mon-YY'
    elif re.match(r'\d{2}-[A-Z]{3}-\d{4}$', datum):
        format['date'] = 'DD-MON-YYYY'
    elif re.match(r'\d{2}-[A-Z][a-z]{2}-\d{4}$', datum):
        format['date'] = 'DD-Mon-YYYY'
    elif re.match(r'\d{4}-\d{1,2}-\d{1,2}$', datum):
        format['date'] = 'YYYY-MM-DD'
    else:
        raise UnsupportedDateFormat(datum)

    # TODO: check dates also apply on other date columns

    return format


def compute_checksum(handle, file_hash=hashlib.sha256(), chunk_size=8192) -> bytes:
    handle.seek(0)
    chunk = handle.read(chunk_size)
    while chunk:
        file_hash.update(chunk)
        chunk = handle.read(chunk_size)
    handle.seek(0)
    return file_hash.digest()
