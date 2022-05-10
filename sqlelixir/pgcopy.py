__all__ = ["copy_from", "copy_to", "NULL"]

from typing import BinaryIO, TextIO, cast

import psycopg2.extensions
from sqlalchemy import Table
from sqlalchemy.engine import Connection
from sqlalchemy.sql.expression import Select
from sqlalchemy.sql.schema import Column
from sqlalchemy.dialects import postgresql

NULL = r"\N"


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
