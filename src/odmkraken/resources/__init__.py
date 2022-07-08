import typing
from abc import ABC, abstractmethod
from contextlib import AbstractContextManager


class DBconnector(ABC):

    @abstractmethod    
    def cursor(self, name: typing.Optional[str]=None) -> AbstractContextManager[...]:
        """Get a cursor with error handling."""
        ...

    @abstractmethod
    def query(self, sql: str, *args, **kwargs) -> AbstractContextManager[...]:
        """Get a cursor and execute a query on it."""
        ...

    @abstractmethod
    def callproc(self, proc: str, *args, **kwargs) -> AbstractContextManager[...]:
        """Get a cursor and execute a stored procedure on it."""
        ...

    @abstractmethod
    def close(self):
        ...