import typing
import psycopg2
from contextlib import contextmanager
import dagster
import warnings


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
        except psycopg2.Error as e:
            self.connection.rollback()
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

    def close(self):
        if self._conn is None:
            warnings.warn('trying to close database connection, but was never connected')
            return
        self._conn.close() 


@dagster.resource
@contextmanager
def postgres_connection(init_context: dagster.InitResourceContext):
    dsn = init_context.resource_config['dsn']
    pgc = PostgresConnector(dsn)
    try:
        yield pgc
    finally:
        pgc.close()
