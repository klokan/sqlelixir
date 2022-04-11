__all__ = ["copy_from", "copy_to", "NULL"]

from typing import BinaryIO, TextIO, cast

import psycopg2.extensions
from sqlalchemy import Table
from sqlalchemy.engine import Connection
from sqlalchemy.sql.schema import Column

NULL = r"\N"


def copy_from(connection: Connection, file: BinaryIO | TextIO, columns: list[Column]):
    """Copy data in text format from file to table."""
    target = format_reference(columns)
    with make_cursor(connection) as cursor:
        cursor.copy_expert(f"COPY {target} FROM STDIN", file)


def copy_to(connection: Connection, file: BinaryIO | TextIO, columns: list[Column]):
    """Copy data in text format from table to file."""
    source = format_reference(columns)
    with make_cursor(connection) as cursor:
        cursor.copy_expert(f"COPY {source} TO STDOUT", file)


def format_reference(columns: list[Column]) -> str:
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
    return f"{table_name} ({column_names})"


def make_cursor(connection: Connection) -> psycopg2.extensions.cursor:
    return cast(psycopg2.extensions.cursor, connection.connection.cursor())
