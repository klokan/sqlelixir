import enum
import sys

from io import TextIOBase
from typing import Any, Type

from sqlalchemy.schema import MetaData
from sqlalchemy.types import Enum, TypeEngine

from sqlelixir.importer import Importer
from sqlelixir.parser import Parser
from sqlelixir.types import TypeRegistry, python_enum_values


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

    def register_enum(
        self, schema: str | None, name: str, enum: Type[enum.Enum], **kwargs
    ):
        kwargs.setdefault("values_callable", python_enum_values)
        type_ = Enum(enum, schema=schema, name=name, **kwargs)
        self.types.add(schema, name, type_)

    def register_type(self, schema: str | None, name: str, type_: TypeEngine):
        self.types.add(schema, name, type_)

    def register_importer(self, patterns: str | list[str]):
        if isinstance(patterns, str):
            patterns = [patterns]

        importer = Importer(self.parser, patterns)
        sys.meta_path.append(importer)

    def parse(self, sql: str | TextIOBase, module: Any):
        self.parser.parse(sql, module)