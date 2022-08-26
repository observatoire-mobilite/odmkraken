import typing
import psycopg2
from psycopg2 import sql
import jinja2
from pathlib import Path
import os
import click
from warnings import warn
import secrets
import collections

here = Path(__file__).absolute().parent
PgCred = collections.namedtuple('PgCred', 'hostname:port:database:username:password'.split(':'))

def render_sql_template(template: str, conn, **kwargs) ->  str:
    """Generate a complex SQL instruction `str` from a file.
    
    Args:
        template: name of the file to look for - must be in this module's parent directory.
    """
    kwargs = {k: (v.as_string(conn) if hasattr(v, 'as_string') else v) for k, v in kwargs.items()}
    loader = jinja2.FileSystemLoader(here)
    env = jinja2.Environment(loader=loader)
    env.filters['literal'] = lambda r: sql.Literal(r).as_string(conn)
    env.filters['identifier'] = lambda r: sql.Identifier(r).as_string(conn)
    tpl = env.get_template(template)
    return tpl.render(**kwargs)


@click.group()
def main():
    pass


def exec_autocommit(conn, query: sql.SQL, *args):
    conn.set_session(autocommit=True)
    with conn.cursor() as cur:
        cur.execute(query, args)
    conn.commit()


def create_database(conn, dbname: str, force: bool=False):
    """Create a new database on the server.

    Checks if the user connected through `conn` has the right privileges
    and whether the database exists. Only then creates it.

    Arguments:
        conn: an established `psycopg2` connection to the target database server
        dbname: a name for the database to be created
        force: if True, drop any existing database under the same name

    Raises:
        RuntimeError: the database already exists
        RuntimeError: the current user account lacks create privileges on the server
    """

    # check user permissions
    with conn.cursor() as cur:
        cur.execute('select rolcreatedb from pg_authid where rolname=current_user')
        if not cur.fetchone()[0]:
            raise RuntimeError('The account used to connect to the database is not allowed to create new databases.')

    # check if DB already exists
    with conn.cursor() as cur:
        cur.execute('select 1 from pg_database WHERE datname=%s', (dbname, ))
        if cur.rowcount == 1:
            if not force:
                raise RuntimeError('Database `{dbname}` already exists on this server')
            exec_autocommit(conn, sql.SQL('drop database if exists {}').format(sql.Identifier(dbname)))

    # drop and create
    exec_autocommit(conn, sql.SQL('create database {}').format(sql.Identifier(dbname)))


@main.command()
@click.option('--dsn', default='postgres://postgres@localhost', show_default=True, help='DSN to the newly created database as its owner.')
@click.option('--dbname', default='odmkraken', show_default=True, help='Name of the database to be created')
@click.option('--force', help='Drop any existing database under the same name instead of raising an exception')
@click.option('--pgpass-file', help='Path to local password file where to store the credentials of the accounts generated. If left unspecified, no such file will be created.')
def db(dsn: typing.Optional[str]=None, dbname: str='odmkraken', force: bool=False, pgpass_file: typing.Optional[Path]=None):
    """ Create a new database for `odmkraken`."""
    
    # connect to database
    conn = psycopg2.connect(dsn)

    # create database (if possible)
    try:
        create_database(conn, dbname, force=force)
    except psycopg2.Error:
        conn.rollback()
    finally:
        conn.close()

    # reconnect to db new db, still as superuser
    conn = psycopg2.connect(f'{dsn}/{dbname}')

    # generate user accounts
    users = {f'{dbname}_{user}': secrets.token_urlsafe(16) 
             for user in ('owner', 'aoo', 'ro')}
    for name, pwd in users.items():
        query = sql.SQL('create user {} with password %s').format(sql.Identifier(name))
        exec_autocommit(conn, query, pwd)

    if pgpass is not None:
        


class PgPass:
    
    def __init__(self, file: Path):
        self._file = file
        self._creds = self._read(file)

    def __iter__(self):
        return iter(self._creds)

    def flush(self):
        self._write(self._file)        

    def _read(self, file: Path) -> typing.List[PgCred]:
        if not file.exists():
            return []
        with file.open('r') as h:
            return [PgCred(*l.strip().split(':')) for l in h.readlines()]

    def _write(self, file: Path):
        with file.open('w') as h:
            h.writelines('\n'.join(':'.join(c) for c in self))


@main.command()
@click.option('--dsn', default='postgres://postgres@localhost/odmvp', show_default=True, help='DSN to the newly created database as its owner.')
@click.option('--network-schema', default='network', show_default=True, help='Name of the schema storing network data.')
@click.option('--vehicle-schema', default='bus_data', show_default=True, help='Name of the schema storing network data.')
@click.option('--user-owner', default='odmvp_owner', show_default=True, help='Name of the user account owning the database and all its objects.')
@click.option('--user-aoo', default='odmvp_aoo', show_default=True, help='Name of the application user account, which may modify data but not structure.')
@click.option('--user-ro', default='odmvp_ro', show_default=True, help='Name of the read only user account, which may only read data.')
@click.option('--srid', type=int, default=2169, show_default=True, help='SRID used for road network data and pings.')
def tables(dsn: typing.Optional[str]=None, template: str='create.sql',
           network_schema: str='network',
           vehicle_schema: str='bus_data',
           user_owner: str='odmvp_owner',
           user_aoo: str='odmvp_aoo',
           user_ro: str='odmvp_ro',
           srid: int=2169):
    """Run the creation script against the provided connection."""
    with psycopg2.connect(dsn) as conn:
        param = dict(
                network_schema = sql.Identifier(network_schema),
                vehicle_schema = sql.Identifier(vehicle_schema),
                user_owner = sql.Identifier(user_owner),
                user_aoo = sql.Identifier(user_aoo),
                user_ro = sql.Identifier(user_ro),
                system_srid = sql.Literal(srid)
            )
        ddl = render_sql_template(template, conn, **param)
        with conn.cursor() as cur:
            cur.execute(ddl)


if __name__ == '__main__':
    main()