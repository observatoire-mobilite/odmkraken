import typing
import pytest
from odmkraken.resources.postgres import *
import dagster
from contextlib import suppress


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
    fake_cur.fetchone = mocker.Mock(return_value=('hallo', 2))
    fake_cur.fetchall = mocker.Mock(return_value=[('hallo', 2), ('moin', 3)])
    fake_cur.mogrify = mocker.Mock(side_effect=lambda r: f"'{r}'")
    fake_cur.close = mocker.Mock()
    fake_cur.copy_expert = mocker.Mock()
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
    with suppress(RuntimeError), c.cursor(name='test') as cur:
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
        
    with suppress(RuntimeError), c.query('select 1', 'moin', 22, name='test') as cur:
        raise RuntimeError()
    fake_psyco._conn.rollback.assert_called_once()
    cur.close.assert_called()    


def test_postgresconnector_callproc(fake_psyco) -> typing.NoReturn:
    """Query must work properly on cursor."""
    c = PostgresConnector(dsn='test')
    with c.callproc('proc', 'moin', 22, name='test') as cur:
        fake_psyco._conn.cursor.assert_called_once_with(name='test')
        fake_psyco._cur.callproc.assert_called_once_with('proc', ('moin', 22))
    cur.close.assert_called_once()
        
    with suppress(RuntimeError), c.callproc('proc', 'moin', 22, name='test') as cur:
        raise RuntimeError()
    fake_psyco._conn.rollback.assert_called_once()
    cur.close.assert_called()    


def test_postgresconnector_run(fake_psyco) -> typing.NoReturn:
    c: PostgresConnector = PostgresConnector(dsn='test')
    c.run('some sql', 'arg1', 22, name='test')
    fake_psyco._conn.cursor.assert_called_once_with(name='test')
    fake_psyco._cur.execute.assert_called_once_with('some sql', ('arg1', 22))


def test_postgresconnector_fetchone(fake_psyco) -> typing.NoReturn:
    c = PostgresConnector(dsn='test')
    res = c.fetchone('some sql', 'arg1', 23)
    fake_psyco._conn.cursor.assert_called_once()
    fake_psyco._cur.execute.assert_called_once_with('some sql', ('arg1', 23))
    assert res == ('hallo', 2)


def test_postgresconnector_fetchall(fake_psyco) -> typing.NoReturn:
    c = PostgresConnector(dsn='test')
    res = c.fetchall('some sql', 'arg1', 23)
    fake_psyco._conn.cursor.assert_called_once()
    fake_psyco._cur.execute.assert_called_once_with('some sql', ('arg1', 23))
    assert res == [('hallo', 2), ('moin', 3)]


def test_postgresconnector_close(fake_psyco):
    c = PostgresConnector(dsn='test')
    
    # we aren't connected yet, so nothing must happen
    c.close()
    fake_psyco._conn.close.assert_not_called()

    # force connection and try again
    c.connection
    c.close()
    fake_psyco._conn.close.assert_called_once()
    
    # since we're closed, call-count must remain at 1
    c.close()
    fake_psyco._conn.close.assert_called_once()


def test_postgresconnector_copy_from(fake_psyco, mocker):
    c = PostgresConnector(dsn='test')
    fake_handle = mocker.Mock()
    tpl = sql.SQL('COPY {} FROM STDIN WITH (FORMAT csv, DELIMITER {}, HEADER 1)')
    
    # call with simple table name
    variants = (
        ('test', sql.Identifier('test'), '?'),
        (('schema', 'test'), sql.Identifier('schema', 'test'), '!'),
        (sql.Identifier('testing'), sql.Identifier('testing'), '+')
    )
    for target, q, sep in variants:
        c.copy_from(fake_handle, target, separator=sep)
        q = tpl.format(q, sql.Literal(sep))
        fake_psyco._cur.copy_expert.assert_called_with(q, fake_handle)
        

def test_postgresconnector_execute_batch(fake_psyco, mocker):
    c = PostgresConnector(dsn='test')
    fake_eb = mocker.patch('odmkraken.resources.postgres.execute_batch', autospec=True)
    fake_data = [('a', 2, 3), ('b', 5, 6)]
    c.execute_batch('some query', fake_data)
    fake_eb.assert_called_once_with(fake_psyco._cur, 'some query', fake_data)

    other_cur = mocker.Mock()
    c.execute_batch('some query', fake_data, cursor=other_cur)
    fake_eb.assert_called_with(other_cur, 'some query', fake_data)


def test_resource_handler(fake_psyco):
    ctx = dagster.build_init_resource_context(config={'dsn': 'test'})
    with postgres_connection(ctx) as pgc:
        with pgc.query('select 1', 'moin', 22, name='test') as cur:
            fake_psyco.connect.assert_called_once_with('test')
            assert cur == fake_psyco._cur
        fake_psyco._conn.close.assert_not_called()
    fake_psyco._conn.close.assert_called_once()

