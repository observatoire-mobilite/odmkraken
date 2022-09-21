import typing
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch
from contextlib import contextmanager
import dagster
import warnings
from io import TextIOBase

class PostgresConnector:
    """Convenience wrapper around a `psycopg2 connection."""

    def __init__(self, dsn: str):
        """Create a new `PostgresConnector` instance.

        Arguments:
            dsn: a postgres connection string.
        """
        self._conn = None
        self._dsn = str(dsn)

    @property
    def connection(self):
        """Return active connection."""
        if self._conn is None:
            self._conn = psycopg2.connect(self._dsn)
        return self._conn
    
    @contextmanager
    def cursor(self, name: typing.Optional[str]=None):
        """Get a cursor with error handling."""
        cur = self.connection.cursor(name=name)
        try:
            yield cur
        except psycopg2.Error:
            self.connection.rollback()
            raise
        else:
            self.connection.commit()
        finally:
            cur.close()

    @contextmanager
    def query(self, sql: str, *args, **kwargs):
        """Get a cursor and execute a query on it."""
        with self.cursor(**kwargs) as cur:
            cur.execute(sql, args)
            yield cur

    @contextmanager
    def callproc(self, proc: str, *args, **kwargs):
        """Get a cursor and execute a stored procedure on it."""
        with self.cursor(**kwargs) as cur:
            cur.callproc(proc, args)
            yield cur

    def run(self, sql: str, *args, **kwargs) -> None:
        with self.cursor(**kwargs) as cur:
            cur.execute(sql, args)

    def fetchone(self, *args, **kwargs) -> typing.Tuple[typing.Any, ...]:
        with self.query(*args, **kwargs) as cur:
            return cur.fetchone()

    def fetchall(self, *args, **kwargs) -> typing.List[typing.Tuple[typing.Any, ...]]:
        with self.query(*args, **kwargs) as cur:
            return cur.fetchall()

    def close(self) -> None:
        if self._conn is None:
            warnings.warn('trying to close database connection, but was never connected')
            return
        self._conn.close()
        self._conn = None
       

    def copy_from(self, handle: TextIOBase, table: typing.Union[str, sql.Identifier, typing.Tuple[str, str]], separator: str=','):
        if isinstance(table, tuple):
            tbl_id = sql.Identifier(*table)
        elif isinstance(table, str):
            tbl_id = sql.Identifier(table)
        else:
            tbl_id = table
        
        with self.cursor() as cur:
            sep_lit = sql.Literal(separator)
            query = sql.SQL('COPY {} FROM STDIN WITH (FORMAT csv, DELIMITER {}, HEADER 1)').format(tbl_id, sep_lit)
            print(query.as_string(cur))
            cur.copy_expert(query, handle)

    def execute_batch(self, query: str, data: typing.List[typing.Tuple[typing.Any, ...]], cursor=None):
        cursor = self.connection.cursor() if cursor is None else cursor
        execute_batch(cursor, query, data)


@dagster.resource
@contextmanager
def postgres_connection(init_context: dagster.InitResourceContext):
    dsn = init_context.resource_config['dsn']
    pgc = PostgresConnector(dsn)
    try:
        yield pgc
    finally:
        pgc.close()
