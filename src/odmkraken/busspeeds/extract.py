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

TBL = typing.List[typing.Tuple[int, datetime, datetime]]


class FileAlreadyImportedError(Exception):

    def __init__(self, file: pathlib.Path, checksum: typing.Optional[bytes]=None):
        msg = f'File with identical checksum as `{file}` was already successfully imported'
        super().__init__(msg)


@dagster.op(required_resource_keys={'edmo_bus_data'}, config_schema={'file': str})
def raw_icts_data(context: dagster.OpExecutionContext):
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
    context.log.debug(f'inferred format spec: {format}')
    checksum = compute_checksum(handle)
    context.log.debug(f'checksum: {checksum}')
    
    # ensure we are importing a thusfar unknown file
    if context.resources.edmo_bus_data.check_file_already_imported(checksum):
        raise FileAlreadyImportedError(file, checksum)

    # dump contents of file into a newly created table
    context.resources.edmo_bus_data.import_csv_file(handle, file, checksum, **format)


@dagster.graph()
def icts_data():
    raw_icts_data()


def open_file(file: pathlib.Path) -> typing.Tuple[int, typing.IO]:
    if not zipfile.is_zipfile(file):
        return 0, file.open('rb')

    archive = zipfile.ZipFile(file)
    n_bytes = archive.filelist[0].file_size
    if len(archive.filelist) != 1:
        raise ValueError('zip archive must contain exactly one file')
    return n_bytes, archive.open(archive.filelist[0], 'r')

def human_readable_bytes(n_bytes: int) -> str:
    if n_bytes < 1:
        return '0 bytes'
    k = math.floor(math.log10(n_bytes) / 3)
    suffix = ['bytes', 'kB', 'MB', 'GB', 'TB'][k]
    return f'{n_bytes * 10**(-k*3):.2f} {suffix}'


class WrongFileFormat(Exception):
    pass


def infer_format(handle: typing.IO):
    lines = [handle.readline().decode('utf-8') for i in range(3)]
    for field in ("TYP", "DATUM", "SOLLZEIT", "ZEIT", "FAHRZEUG",
                  "LINIE", "UMLAUF", "FAHRT", "HALT", "LATITUDE",
                  "LONGITUDE", "EINSTEIGER", "AUSSTEIGER"):
        if field not in lines[0]:
            raise WrongFileFormat(f'input file header lacks `{field}` field')
    
    match = re.match(f'TYP["\']?(;|,)', lines[0])
    if not match:
        raise WrongFileFormat('does not seem to be CSV format')
    format = {'sep': match.group(1)}
    
    header = [s.strip('"\' ') for s in lines[0].split(format['sep'])]
    i_datum = header.index('DATUM')
    datum = lines[1].split(format['sep'])[i_datum]
    
    if re.match(r'\d{2}\.\d{2}\.\d{4}', datum):
        format['date'] = 'DD.MM.YYYY'
    elif re.match(r'\d{2}-[A-Z]{3}-\d{2}', datum):
        format['date'] = 'DD-MON-YY'
    elif re.match(r'\d{2}-[A-Z][a-z]{2}-\d{2}', datum):
        format['date'] = 'DD-Mon-YY'
    elif re.match(r'\d{2}-[A-Z]{3}-\d{4}', datum):
        format['date'] = 'DD-MON-YYYY'
    elif re.match(r'\d{2}-[A-Z][a-z]{2}-\d{4}', datum):
        format['date'] = 'DD-Mon-YYYY'
    elif re.match(r'\d{4}-\d{1,2}-\d{1,2}', datum):
        format['date'] = 'YYYY-MM-DD'
    else:
        raise WrongFileFormat('unknown date format')

    return format


def compute_checksum(handle, file_hash=hashlib.sha256(), chunk_size=8192) -> bytes:
    handle.seek(0)
    chunk = handle.read(chunk_size)
    while chunk:
        file_hash.update(chunk)
        chunk = handle.read(chunk_size)
    handle.seek(0)
    return file_hash.digest()
