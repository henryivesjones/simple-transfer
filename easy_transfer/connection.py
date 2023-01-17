import abc
from typing import Dict, Generator, Iterable, Sequence, TextIO, Tuple

from easy_transfer.column import Column


class NotConnectedException(Exception):
    pass


class Connection(abc.ABC):
    @abc.abstractmethod
    def connect(self):
        pass

    @abc.abstractmethod
    def close(self):
        pass

    @abc.abstractmethod
    def select(self, query: str, args: Sequence = []) -> Generator[Tuple, None, None]:
        pass

    @abc.abstractmethod
    def execute(self, query: str, args: Sequence = []):
        pass

    @abc.abstractmethod
    def execute_many(self, queries: Iterable[str], args: Iterable[Sequence] = []):
        pass

    @abc.abstractmethod
    def extract_table_ddl(self, schema: str, table: str) -> Iterable[Column]:
        pass

    @abc.abstractmethod
    def extract_table(self, schema: str, table: str) -> Generator[Tuple, None, None]:
        pass

    @abc.abstractmethod
    def import_csv(self, schema: str, table: str, f: TextIO):
        pass

    @staticmethod
    @abc.abstractmethod
    def generate_create_table_statement(
        schema: str, table: str, columns: Iterable[Column]
    ) -> str:
        pass

    @staticmethod
    @abc.abstractmethod
    def generate_drop_table_statement(schema: str, table: str) -> str:
        pass

    @staticmethod
    @abc.abstractmethod
    def generate_rename_table_statement(schema: str, table: str, new_table: str) -> str:
        pass
