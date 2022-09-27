import pytest
import psycopg2
from psycopg2.sql import SQL, Identifier, Literal
from odmkraken.resources.edmo.busdata.sql import *

@pytest.fixture()
def testing_database(dsn='postgres://localhost', dbname='testing_odmkraken'):
    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(SQL('create database {}').format(dbname))
        
    conn = psycopg2.connect(f'{dsn}/{dbname}')
    try:
        yield conn
    except psycopg2.Error:
        conn.rollback()
    finally:
        conn.close()

    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(SQL('drop database if exists {}').format(dbname))
    

@pytest.mark.skip(reason='no postgres server')
def test_staging_table(testing_database):
    with testing_database.cursor() as cur:
        cur.execute(sql.create_staging_table(schema='public', staging_table='test'))