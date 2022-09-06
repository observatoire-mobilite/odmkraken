"""ODMkraken Database Deployment Utility.

This tool simplifies setting up the `odmkraken` database on postgres.
Simply provide the username and password of a superuser account, and
`deploy setup` will do the rest.
"""
import typing
import click
import secrets
from . import DB
import psycopg2.errors
from psycopg2 import sql as sql
import sys


@click.group()
@click.option('--host', '-h', envvar='PGHOST', default='localhost', show_default=True, help='DNS or IP address where to reach the database server(s).')
@click.option('--port', '-p', envvar='PGPORT', default=5432, show_default=True, help='Port the database servers listens on.')
@click.option('--user', '-U', envvar='PGUSER', default='postgres', show_default=True, help='Database user name to connect with.')
@click.option('--password', envvar='PGPASS', prompt=True, hide_input=True)
@click.option('--passfile', envvar='PGPASSFILE', help='Path to the pgpass file.')
@click.option('--sslmode', type=click.Choice(['verify-full', 'verify-ca']), help='SSL mode.')
@click.option('--sslcert', help='Path to the client certificate file.')
@click.option('--sslkey', help='Path to the private key file belonging to the certificate file.')
@click.option('--sslrootcert', help='Path to root authority certificate used for validation.')
@click.pass_context
def db(ctx: click.Context, **kwargs):
    """Database deployment utility for `odmkraken`."""
    ctx.obj = DB(**kwargs)


@db.command()
@click.pass_obj
def test(db, length: int=16):
    """Generate a random password."""
    print(db._connargs)


@db.command()
@click.option('--length', type=click.IntRange(8, 100), help='Number of characters to generate')
def genpw(length: int=16):
    """Generate a random password."""
    print(secrets.token_urlsafe(length))


@db.command()
@click.pass_obj
@click.argument('role')
@click.option('--length', type=click.IntRange(8, 100), help='Number of characters to generate')
def resetpw(db: DB, role: str, length: int=16):
    """Change user passwords."""
    password = secrets.token_urlsafe(length)
    db.execute(sql.SQL('alter user {user} with password {password}')
               .format(user=sql.Identifier(role), 
                       password=sql.Literal(password)))
    #dsn = '@'.join((':'.join((role, password)), ':'.join(str(s) for s in db.address)))
    #print(dsn)
    print(password)


@db.command
@click.pass_context
@click.option('--dbname', default='odmkraken', show_default=True, help='Name of the database to be created.')
@click.option('--network-schema-name', default='network', show_default=True, help='Name of the schema storing network data.')
@click.option('--vehdata-schema-name', default='vehdata', show_default=True, help='Name of the schema storing vehicle data.')
@click.option('--srid', type=click.INT, default=2169, show_default=True, help='SRID used for all geographical data.')
def setup(ctx: click.Context, dbname: str,
    network_schema_name: str,
    vehdata_schema_name: str,
    srid: int=2169):
    """Setup the `odmkraken` database.

    Arguments:
        dbname: the name of the catalogue to be created.
        network_schema_name: schema name where to store network topology data.
        vehdata_schema_name: name of the schema storing vehicle date.
        srid: identifier of the coordinate system used for all geographical data.
    """
    param = {
        'dbname': sql.Identifier(dbname),
        'network_schema': sql.Identifier(network_schema_name),
        'vehdata_schema': sql.Identifier(vehdata_schema_name),
        'user_owner': sql.Identifier(f'{dbname}_owner'),
        'user_aoo': sql.Identifier(f'{dbname}_aoo'),
        'user_ro': sql.Identifier(f'{dbname}_ro'),
        'system_srid': sql.Literal(srid)
    }
    db = ctx.obj
    
    try:
        db.execute(sql.SQL('create database {}').format(param['dbname']))  # must run outside transaction
    except psycopg2.errors.DuplicateDatabase:
        sys.stderr.writelines([
            'FAILURE: could not create the database !\n',
            f'PROBLEM: A database `{dbname}` already exists.\n'
            'Hint: the `teardown` command deletes databases, irreversibly - use with caution.'
        ])
        ctx.exit(1)
    
    try:
        db.run_script('db.setup.sql', dbname=dbname, param=param)
    except psycopg2.Error:
        print('Something went wrong. Invoking `teardown` command...')
        ctx.invoke(teardown, dbname=dbname)
        raise
    print('All done!')


@db.command
@click.pass_obj
@click.option('--dbname', default='odmkraken', show_default=True, help='Name of the database to be created')
@click.confirmation_option(prompt='ATTENTION! This operation will irreversibly delete the database and all of its contents. Do you want to continue?')
def teardown(db: DB, dbname: str):
    """Delete the specified database.

    This drops the specified database (using `--dbname`) and the
    three user accounts coming with it (`<dbname>_owner`, `<dbname>_aoo` and `<dbname>_ro`).
    There is no checking done to see if the database is a `odmkraken` database,
    so use with caution.
    Also note that no errors are risen if either the  database or one or several
    of the accounts do not exist.

    Arguments:
        dbname: the target database.
    """
    param = {
        'user_owner': sql.Identifier(f'{dbname}_owner'),
        'user_aoo': sql.Identifier(f'{dbname}_aoo'),
        'user_ro': sql.Identifier(f'{dbname}_ro')
    }
    db.execute(sql.SQL('drop database if exists {}').format(sql.Identifier(dbname)), autocommit=True)  # must run outside transaction
    db.run_script('db.teardown.sql', param=param)
    print(f'Script `{dbname}` successfully executed.')


if __name__ == '__main__':
    db()