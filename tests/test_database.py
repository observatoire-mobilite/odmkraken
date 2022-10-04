import pytest
import psycopg2


def test_db():
  conn = psycopg2.connect('postgres://postgres:postgres@localhost:5432')
  with conn.cursor() as cur:
    cur.execute('select version()')
    ver = cur.fetchone()
  assert 'Postgres' in ver[0]
