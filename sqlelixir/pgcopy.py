__all__ = [
    "NULL",
    "copy_from",
    "copy_to",
    "dump",
    "load",
    "global_converter",
    "configure_converter",
    "make_converter",
]

from collections.abc import Iterable, Iterator
from datetime import date, datetime, timedelta
from operator import methodcaller
from typing import BinaryIO, TextIO, Type, TypeVar, cast
from uuid import UUID

import psycopg2.extensions
import cattrs
from sqlalchemy import Table
from sqlalchemy.engine import Connection
from sqlalchemy.sql.expression import Select
from sqlalchemy.sql.schema import Column
from sqlalchemy.dialects import postgresql

NULL = r"\N"


def ifnull(value: str | None, default: str = NULL) -> str:
    if value is None:
        return default
    else:
        return value


def nullif(value: str, default: str = NULL) -> str | None:
    if value == default:
        return None
    else:
        return value


def structure_iso8601(value, as_type):
    return as_type.fromisoformat(value)


unstructure_iso8601 = methodcaller("isoformat")


def configure_converter(converter: cattrs.Converter):
    """Configure cattrs converter for use with load() and dump()."""

    if converter.unstruct_strat is not cattrs.UnstructureStrategy.AS_TUPLE:
        raise RuntimeError("Converter must use tuple strategy")

    converter.register_structure_hook(bool, lambda v, _: v == "t")
    converter.register_structure_hook(int, lambda v, _: int(v))
    converter.register_structure_hook(float, lambda v, _: float(v))
    converter.register_structure_hook(date, structure_iso8601)
    converter.register_structure_hook(datetime, structure_iso8601)
    converter.register_structure_hook(UUID, lambda v, _: UUID(v))

    converter.register_unstructure_hook(bool, lambda v: "t" if v else "f")
    converter.register_unstructure_hook(int, str)
    converter.register_unstructure_hook(float, str)
    converter.register_unstructure_hook(date, unstructure_iso8601)
    converter.register_unstructure_hook(datetime, unstructure_iso8601)
    converter.register_unstructure_hook(timedelta, str)
    converter.register_unstructure_hook(UUID, str)


def make_converter(**kwargs) -> cattrs.Converter:
    """Create cattrs converter and configure it for use with load() and dump()."""
    kwargs.setdefault("unstruct_strat", cattrs.UnstructureStrategy.AS_TUPLE)
    converter = cattrs.Converter(**kwargs)
    configure_converter(converter)
    return converter


global_converter = make_converter()


T = TypeVar("T")


def dump(
    file: TextIO, objs: Iterable[object], converter: cattrs.Converter = global_converter
):
    """Encode objects into COPY input file."""

    unstructure = converter.unstructure

    for obj in objs:
        file.write("\t".join(map(ifnull, unstructure(obj))))
        file.write("\n")


def load(
    file: TextIO, as_type: Type[T], converter: cattrs.Converter = global_converter
) -> Iterator[T]:
    """Decode objects of a given type from COPY output file."""

    structure = converter.structure

    for line in file:
        yield structure(map(nullif, line.rstrip("\n").split("\t")), as_type)


def copy_from(connection: Connection, file: BinaryIO | TextIO, columns: list[Column]):
    """Copy data in text format from file to table."""
    if not columns:
        raise RuntimeError("Missing columns")

    table: Table = columns[0].table

    for column in columns:
        if column.table is not table:
            raise RuntimeError("Multiple tables")

    if table.schema is not None:
        table_name = f"{table.schema}.{table.name}"
    else:
        table_name = table.name

    column_names = ", ".join(column.name for column in columns)
    target = f"{table_name} ({column_names})"

    with make_cursor(connection) as cursor:
        cursor.copy_expert(f"COPY {target} FROM STDIN", file)


def copy_to(connection: Connection, file: BinaryIO | TextIO, query: Select):
    """Copy data in text format from table to file."""
    with make_cursor(connection) as cursor:
        compiled = query.compile(dialect=postgresql.dialect())
        source = cursor.mogrify(compiled.string, compiled.params).decode()
        cursor.copy_expert(f"COPY ({source}) TO STDOUT", file)


def make_cursor(connection: Connection) -> psycopg2.extensions.cursor:
    return cast(psycopg2.extensions.cursor, connection.connection.cursor())
