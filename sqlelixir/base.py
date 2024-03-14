import sys

from io import TextIOBase
from pathlib import Path
from typing import Any

from sqlalchemy.engine import Connection, Engine
from sqlalchemy.schema import MetaData, Table, CreateSchema
from sqlalchemy.sql.expression import TextClause
from sqlalchemy.sql.functions import _FunctionGenerator
from sqlalchemy.types import TypeEngine, Enum

from sqlelixir.importer import Importer
from sqlelixir.parser import Parser, Procedure
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


def create_all(bind: Engine | Connection, metadata: MetaData, checkfirst: bool = True):
    """Create schema from parsed SQLAlchemy metadata.

    Equivalent to `MetaData.create_all()`, but handles other constructs
    supported by SQLElixir (schemas, views, temporary tables) in addition
    to just tables.
    """
    tables = []
    views = {}

    # Sort metadata items into tables and views.
    # Views are represented as table objects, but need to be created via SQL.
    for table in metadata.tables.values():
        if table.info.get("sqlelixir.type") == "VIEW":
            # XXX
            # Skip materialized views for now. TimescaleDB complains when
            # creating a continuous aggregate view on a regular table.
            if not table.info.get("sqlelixir.materialized", False):
                views[table.schema, table.name] = table.info["sqlelixir.DDL"]
        else:
            # Temporary tables are created by the application as needed.
            if not table.info.get("sqlelixir.temporary", False):
                tables.append(table)

    # Determine which objects already exist in the database.
    if checkfirst:
        existing_schemas = set(
            bind.execute("SELECT nspname FROM pg_catalog.pg_namespace").scalars()
        )
        existing_views = {
            tuple(row)
            for row in bind.execute(
                "SELECT schemaname, viewname FROM pg_catalog.pg_views"
            )
        }
    else:
        existing_schemas = {}
        existing_views = {}

    # Create necessary schemas first, since `MetaData.create_all()` does not.
    for schema in metadata._schemas - existing_schemas:
        bind.execute(CreateSchema(schema))

    # Create physical tables.
    metadata.create_all(bind, tables=tables, checkfirst=checkfirst)

    # Create views next, since they depend on physical tables.
    for name, sql in views.items():
        if name not in existing_views:
            bind.execute(sql)


def generate_type_stubs():
    """Generate type stubs for all modules imported by SQLElixir."""
    object_types = (Enum, Table, _FunctionGenerator, Procedure, TextClause)

    for module in list(sys.modules.values()):
        spec = getattr(module, "__spec__", None)
        if spec is None:
            continue
        if not isinstance(spec.loader, Importer):
            continue

        assert spec.origin is not None
        path = Path(spec.origin).with_suffix(".pyi")

        objects = {
            name: type(obj)
            for name, obj in module.__dict__.items()
            if isinstance(obj, object_types)
        }

        if not objects:
            path.unlink(missing_ok=True)
            continue

        lines = []
        lines.extend(
            sorted(
                f"from {object_type.__module__} import {object_type.__name__}\n"
                for object_type in set(objects.values())
            )
        )
        lines.append("\n")
        lines.extend(
            sorted(
                f"{name}: {object_type.__name__}\n"
                for name, object_type in objects.items()
            )
        )
        content = "".join(lines)

        if not path.exists() or path.read_text() != content:
            path.write_text(content)
