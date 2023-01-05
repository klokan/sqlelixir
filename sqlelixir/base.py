import sys

from io import TextIOBase
from typing import Any

from sqlalchemy.schema import MetaData, Table
from sqlalchemy.types import TypeEngine, NullType

from sqlelixir.importer import Importer
from sqlelixir.parser import Parser
from sqlelixir.types import TypeRegistry


postgres_naming_convention = {
    "pk": "%(table_name)s_pkey",
    "uq": "%(table_name)s_%(column_0_name)s_key",
    "fk": "%(table_name)s_%(column_0_name)s_fkey",
    "ix": "%(table_name)s_%(column_0_name)s_idx",
}


class SQLElixir:
    def __init__(self, metadata: MetaData | None = None):
        if metadata is None:
            metadata = MetaData(naming_convention=postgres_naming_convention)

        self.types = TypeRegistry()
        self.metadata = metadata
        self.parser = Parser(self.types, self.metadata)

    def register_type(self, schema: str | None, name: str, type_: TypeEngine):
        self.types.add(schema, name, type_)

    def register_importer(self, patterns: str | list[str]):
        if isinstance(patterns, str):
            patterns = [patterns]

        importer = Importer(self.parser, patterns)
        sys.meta_path.append(importer)

    def parse(self, sql: str | TextIOBase, module: Any):
        self.parser.parse(sql, module)


def real_tables(metadata: MetaData) -> list[Table]:
    """Find all real tables in metadata, ignoring views.

    This is useful when creating a schema that contains views.
    SQAlchemy can't really represent them, so this function can
    be used to filter them out.

    >> metadata.create_all(bind, tables=sqlelixir.real_tables(metadata))
    """

    tables = []
    for table in metadata.tables.values():
        # Views without columns are represented as bare tables.
        if not table.columns:
            continue

        # Views with columns don't specify their types,
        # and SQLAlchemy fills in the NULL type.
        if any(isinstance(column.type, NullType) for column in table.columns):
            continue

        tables.append(table)

    return tables
