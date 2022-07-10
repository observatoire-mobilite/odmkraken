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


@dagster.asset(required_resource_keys={'edmo_bus_data'}, config_schema={'file': str})
def raw_icts_data(context: dagster.OpExecutionContext):
    """Import vehicle data from a zipped CSV data-file.

    The task first copies over all of the raw data onto a temporary
    table on the database server. It then checks for any new vehicle,
    line or stop codes, before normalizing and adding the tracking
    information to the `init.pings` table.

    Note that if any exception occurs at any stage, the entire process
    is rolled back, ensuring the integrity of the existing data.
    Also note that one of the actions triggering such a rollback is
    re-importing already imported data.

    Args:
        db: database service handle
        archive: path to the CSV (or zipped CSV-file) hodling the data.
    """
    file = context.op_config['file']
    n_bytes, handle = open_file(pathlib.Path(file))
    if n_bytes == 0:
        context.log.warn('file is either empty or not a zip-file')
    else:
        context.log.info(f'file size: {human_readable_bytes(n_bytes)}')
    format = infer_format(handle)
    checksum = compute_checksum(handle)
    
    # ensure we are importing a thusfar unknown file
    with context.resources.db.cursor() as cur:
        cur.execute(f'select * from bus_data.data_files where checksum=%s', (checksum, ))
        if cur.rowcount > 0:
            raise ValueError('file was already imported')
    context.log.info('file has not been imported before')

    # create a place to hold the data while we work on it
    with context.resources.db.cursor() as cur:
        cur.callproc('bus_data.create_staging_table')
        temp_tbl = cur.fetchone()[0]
    context.log.info(f'staging table is `{temp_tbl}`')

    # dump contents of file into newly created table
    sql = f'COPY {temp_tbl} FROM STDIN WITH (FORMAT csv, DELIMITER \';\', HEADER 1)'
    with context.resources.db.cursor() as cur:

        cur.copy_expert(sql, handle)
        cur.execute(f'select count(*) from {temp_tbl}')
        context.log.info(f'ingested {cur.fetchone()[0]} lines')
    
        context.log.info('adjusting date format (this might take a while)')
        cur.callproc('bus_data.adjust_format', (format['date'], ))
    
    # run normalization sequence; this should be atomic (i.e. "all or nothing"):
    # custom cursor context manager ensures automatic roll-back if 
    # any of the 4 calls raises an exception or auto-commit otherwise
    with context.resources.db.cursor() as cur:
        
        cur.callproc('bus_data.extract_vehicles')
        context.log.info(f'detected new vehicles: {", ".join(str(r[1]) for r in cur.fetchall())}')

        cur.callproc('bus_data.extract_lines')
        context.log.info(f'detected new lines: {", ".join(str(r[1]) for r in cur.fetchall())}')

        cur.callproc('bus_data.extract_stops')
        context.log.info(f'detected new stops: {", ".join(str(r[1]) for r in cur.fetchall())}')
        
        cur.callproc('bus_data.extract_runs_with_timeframes')
        timeframes = cur.fetchall()

        cur.callproc('bus_data.extract_pings')

        sql = 'insert into "bus_data"."data_files" (id, filename, imported_on, checksum) values (gen_random_uuid(), %s, now(), %s) returning id'
        cur.execute(sql, (str(file), checksum))
        file_id = cur.fetchone()[0]
        context.log.info(f'file registered as {file_id}')

        sql = 'insert into "bus_data"."data_file_timeframes"(id, file_id, vehicle_id, time_start, time_end) values (gen_random_uuid(), %s, %s, %s, %s);'
        context.resources.db.execute_batch(sql, [(str(file_id), *t) for t in timeframes])
        context.log.info(f'imported {len(timeframes)} vehicle-timeframes')

    with context.resources.db.cursor() as cur:
        cur.execute(f'drop table if exists {temp_tbl};')        


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
