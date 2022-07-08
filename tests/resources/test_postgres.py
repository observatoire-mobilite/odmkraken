import pytest
from odmkraken.resources.postgres import *
import dagster


def test_postgresconnector():
    c = PostgresConnector(dsn='moin-1234')
    assert c._dsn == 'moin-1234'
    assert c._conn is None

@pytest.fixture
def fake_psyco(mocker):
    fake_psyco = mocker.patch('odmkraken.resources.postgres.psycopg2')
    fake_psyco.Error = RuntimeError
    fake_cur = mocker.Mock()
    fake_cur.execute = mocker.Mock()
    fake_cur.callproc = mocker.Mock()
    fake_cur.close = mocker.Mock()
    fake_conn = mocker.Mock()
    fake_conn.cursor = mocker.Mock(return_value=fake_cur)
    fake_conn.rollback = mocker.Mock()
    fake_conn.close = mocker.Mock()
    fake_psyco.connect = mocker.Mock(return_value=fake_conn)
    fake_psyco._conn = fake_conn
    fake_psyco._cur = fake_cur
    return fake_psyco


def test_postgresconnector_connect(fake_psyco):
    """Must connect using DSN on first call of `connection`."""
    c = PostgresConnector(dsn='test')
    fake_psyco.connect.assert_not_called()
    assert c.connection == fake_psyco._conn
    fake_psyco.connect.assert_called_once_with('test')
    assert c.connection == fake_psyco._conn
    fake_psyco.connect.assert_called_once_with('test')


def test_postgresconnector_cursor(fake_psyco):
    """Cursor context manager must roll back connection."""
    c = PostgresConnector(dsn='test')
    
    # normal operations
    with c.cursor(name='test') as cur:
        fake_psyco._conn.cursor.assert_called_once_with(name='test')
        assert cur == fake_psyco._cur
    fake_psyco._conn.rollback.assert_not_called()
    cur.close.assert_called_once()

    # simulate problem
    with c.cursor(name='test') as cur:
        raise RuntimeError()
    fake_psyco._conn.rollback.assert_called_once()
    cur.close.assert_called()


def test_postgresconnector_query(fake_psyco):
    """Query must work properly on cursor."""
    c = PostgresConnector(dsn='test')
    with c.query('select 1', 'moin', 22, name='test') as cur:
        fake_psyco._conn.cursor.assert_called_once_with(name='test')
        fake_psyco._cur.execute.assert_called_once_with('select 1', ('moin', 22))
    cur.close.assert_called_once()
        
    with c.query('select 1', 'moin', 22, name='test') as cur:
        raise RuntimeError()
    fake_psyco._conn.rollback.assert_called_once()
    cur.close.assert_called()    


def test_postgresconnector_callproc(fake_psyco):
    """Query must work properly on cursor."""
    c = PostgresConnector(dsn='test')
    with c.callproc('proc', 'moin', 22, name='test') as cur:
        fake_psyco._conn.cursor.assert_called_once_with(name='test')
        fake_psyco._cur.callproc.assert_called_once_with('proc', ('moin', 22))
    cur.close.assert_called_once()
        
    with c.callproc('proc', 'moin', 22, name='test') as cur:
        raise RuntimeError()
    fake_psyco._conn.rollback.assert_called_once()
    cur.close.assert_called()    

        
def test_resource_handler(fake_psyco):
    ctx = dagster.build_init_resource_context(config={'dsn': 'test'})
    with postgres_connection(ctx) as pgc:
        with pgc.query('select 1', 'moin', 22, name='test') as cur:
            fake_psyco.connect.assert_called_once_with('test')
            assert cur == fake_psyco._cur
        fake_psyco._conn.close.assert_not_called()
    fake_psyco._conn.close.assert_called_once()

