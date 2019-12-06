import os.path
import re
import sys

import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as pg
import sqlparse

from collections import defaultdict
from datetime import datetime
from importlib.machinery import ModuleSpec
from io import BytesIO
from sqlalchemy.sql.elements import ClauseElement, Executable
from sqlparse import tokens as tk
from struct import Struct
from types import SimpleNamespace
from uuid import UUID
from xml.etree import ElementTree


try:
    from geoalchemy2 import Geography, Geometry
except ImportError:
    Geography = Geometry = None

try:
    from sqlalchemy_utils import LtreeType
except ImportError:
    LtreeType = None



# https://docs.sqlalchemy.org/en/13/dialects/postgresql.html#using-enum-with-array
class ArrayOfEnum(sa.TypeDecorator):
    impl = pg.ARRAY

    def bind_expression(self, bindvalue):
        return sa.cast(bindvalue, self)

    def result_processor(self, dialect, coltype):
        super_rp = super(ArrayOfEnum, self).result_processor(dialect, coltype)

        def handle_raw_string(value):
            inner = re.match(r"^{(.*)}$", value).group(1)
            return inner.split(",") if inner else []

        def process(value):
            if value is not None:
                return super_rp(handle_raw_string(value))
            else:
                return None

        return process


class XML(sa.types.UserDefinedType):
    def get_col_spec(self):
        return "XML"

    def bind_processor(self, dialect):
        def process(value):
            if value is not None:
                return ElementTree.tostring(value, encoding="unicode")
            else:
                return None

        return process

    def result_processor(self, dialect, coltype):
        def process(value):
            if value is not None:
                return ElementTree.fromstring(value)
            else:
                return None

        return process


class SQLElixir:

    builtin_types = {
        "bigint": sa.BigInteger,
        "bigserial": sa.BigInteger,
        "boolean": sa.Boolean,
        "bytea": pg.BYTEA,
        "citext": sa.Text,
        "date": sa.Date,
        "daterange": pg.DATERANGE,
        "double precision": sa.Float,
        "float": sa.Float,
        "geography": Geography,
        "geometry": Geometry,
        "int": sa.Integer,
        "integer": sa.Integer,
        "int4range": pg.INT4RANGE,
        "int8range": pg.INT8RANGE,
        "interval": sa.Interval,
        "json": pg.JSON,
        "jsonb": pg.JSONB,
        "ltree": LtreeType,
        "numeric": pg.NUMERIC,
        "serial": sa.Integer,
        "text": sa.Text,
        "time": sa.Time,
        "timestamp": sa.DateTime,
        "timestamptz": sa.DateTime(timezone=True),
        "tsrange": pg.TSRANGE,
        "tstzrange": pg.TSTZRANGE,
        "tsvector": pg.TSVECTOR,
        "uuid": pg.UUID(as_uuid=True),
        "xml": XML,
    }

    def __init__(self, metadata=None):
        self.types = defaultdict(dict)
        self.types[None] = self.builtin_types.copy()
        self.metadata = metadata

    def register_package(self, qualname):
        sys.meta_path.append(Package(self, qualname))

    def import_module(self, path):
        module = SimpleNamespace()
        with self.open_module(path) as fp:
            self.parse_module(module, fp)
        return module

    def open_module(self, path):
        return open(path, "r", encoding="utf-8")

    def parse_module(self, module, fp):
        module.__text__ = fp.read()
        parser = Parser(self.types, self.metadata, module)
        parser.parse()


class Package:
    def __init__(self, elixir, qualname):
        self.elixir = elixir
        self.names = qualname.split(".")

    def find_spec(self, qualname, path, target=None):
        if path is None:
            return None
        names = qualname.split(".")
        if len(names) <= len(self.names):
            return None
        if any(i != j for i, j in zip(names, self.names)):
            return None
        for entry in path:
            full_path = os.path.join(entry, names[-1] + ".sql")
            try:
                fp = self.elixir.open_module(full_path)
            except FileNotFoundError:
                continue
            return ModuleSpec(
                name=qualname, loader=self, loader_state=fp, origin=full_path
            )
        return None

    def create_module(self, spec):
        return None

    def exec_module(self, module):
        with module.__spec__.loader_state as fp:
            self.elixir.parse_module(module, fp)


class Parser:
    def __init__(self, types, metadata, module):
        self.types = types
        self.metadata = metadata
        self.module = module
        self.schema = None
        self.stream = None
        self.next_type = None
        self.next_value = None
        self.value = None

    def parse(self):
        for statement in sqlparse.parse(self.module.__text__):
            self.begin(statement)
            self.parse_statement()

    def parse_statement(self):
        if self.accept("CREATE"):
            if self.accept("SCHEMA"):
                self.expect(tk.Name)
                self.schema = self.value
                return
            if self.accept("FUNCTION") and self.accept(tk.Name):
                return self.parse_function()
            if self.accept("TYPE") and self.accept(tk.Name):
                return self.parse_type()
            if self.accept("TABLE") and self.accept(tk.Name):
                return self.parse_table()

    def parse_function(self):
        schema, name = self.parse_qualname()
        if schema is not None:
            func = getattr(getattr(sa.func, schema), name)
        else:
            func = getattr(sa.func, name)
        self.export(schema, name, func)

    def parse_type(self):
        schema, name = self.parse_qualname()
        if self.accept("AS") and self.accept("ENUM"):
            variants = []
            self.expect("(")
            while not self.accept(")"):
                self.expect(tk.String)
                variants.append(self.value)
                self.accept(",")
            enum = sa.Enum(*variants, schema=schema, name=name)
            self.export(schema, name, enum)
            self.types[schema][name] = enum

    def parse_table(self):
        schema, name = self.parse_qualname()
        constraints = []
        args = []
        kwargs = {}
        args.append(name)
        args.append(self.metadata)
        kwargs["schema"] = schema
        self.expect("(")
        while not self.accept(")"):
            if self.accept("PRIMARY"):
                constraints.append(self.parse_primary_key())
            elif self.accept("CONSTRAINT"):
                self.expect(tk.Name)
                if self.accept("FOREIGN"):
                    constraints.append(self.parse_foreign_key())
            elif self.accept(tk.Name):
                args.append(self.parse_column())
            self.parse_until(",)")
            self.accept(",")
        args.extend(constraints)
        table = sa.Table(*args, **kwargs)
        self.export(schema, name, table)

    def parse_primary_key(self):
        columns = []
        self.expect("KEY")
        self.expect("(")
        while not self.accept(")"):
            self.expect(tk.Name)
            columns.append(self.value)
            self.accept(",")
        return sa.PrimaryKeyConstraint(*columns)

    def parse_foreign_key(self):
        columns = []
        self.expect("KEY")
        self.expect("(")
        while not self.accept(")"):
            self.expect(tk.Name)
            columns.append(self.value)
            self.accept(",")
        self.expect("REFERENCES")
        refcolumns = self.parse_reference()
        return sa.ForeignKeyConstraint(columns, refcolumns)

    def parse_reference(self):
        self.expect(tk.Name)
        schema, name = self.parse_qualname()
        if schema is not None:
            qualname = "{}.{}".format(schema, name)
        else:
            qualname = name
        if self.accept("("):
            refcolumns = []
            while not self.accept(")"):
                self.expect(tk.Name)
                refcolumn = "{}.{}".format(qualname, self.value)
                refcolumns.append(refcolumn)
                self.accept(",")
            return refcolumns
        else:
            reftable = self.metadata.tables[qualname]
            return reftable.primary_key.columns.values()

    def parse_column(self):
        args = []
        kwargs = {}
        args.append(self.value)
        args.append(self.parse_column_type())
        while True:
            if self.accept("NOT NULL"):
                kwargs["nullable"] = False
                continue
            if self.accept("UNIQUE"):
                kwargs["unique"] = True
                continue
            if self.accept("PRIMARY"):
                self.expect("KEY")
                kwargs["primary_key"] = True
                continue
            break
        if self.accept("REFERENCES"):
            (refcolumn,) = self.parse_reference()
            args.append(sa.ForeignKey(refcolumn))
        elif self.accept("DEFAULT"):
            default = self.parse_default()
            if default is not None:
                kwargs["default"] = default
            else:
                args.append(sa.FetchedValue())
        return sa.Column(*args, **kwargs)

    def parse_column_type(self):
        self.expect(tk.Name)
        schema, name = self.parse_qualname()
        type_ = self.types[schema][name]
        if self.accept("["):
            dimensions = 1
            self.accept(tk.Number)
            self.expect("]")
            while self.accept("["):
                dimensions += 1
                self.accept(tk.Number)
                self.expect("]")
            if isinstance(type_, sa.Enum):
                if dimensions != 1:
                    raise RuntimeError("Mulitple dimensions for enum array", schema, name)
                type_ = ArrayOfEnum(type_)
            else:
                type_ = pg.ARRAY(type_, dimensions=dimensions)
        return type_

    def parse_default(self):
        if self.accept("TRUE"):
            return True
        if self.accept("FALSE"):
            return False
        if self.accept(tk.Number) or self.accept(tk.String):
            return self.value
        return None

    def parse_qualname(self):
        name = self.value
        if self.accept("."):
            self.expect(tk.Name)
            return name, self.value
        else:
            return None, name

    def parse_until(self, terminators):
        depth = 0
        while True:
            if depth == 0:
                for terminator in terminators:
                    if self.match(terminator):
                        return
            if self.accept("("):
                depth += 1
                continue
            if self.accept(")"):
                assert depth > 0
                depth -= 1
                continue
            self.advance()

    def begin(self, statement):
        stream = []
        for token in statement.flatten():
            ttype = token.ttype
            if ttype in tk.Whitespace:
                continue
            if ttype in tk.Comment:
                continue
            if ttype in tk.Number:
                value = float(token.value)
                if value.is_integer:
                    value = int(value)
                stream.append((tk.Number, value))
            elif ttype in tk.String:
                if token.value.startswith("'"):
                    stream.append((tk.String, token.value.strip("'")))
                elif token.value.startswith('"'):
                    stream.append((tk.Name, token.value.strip('"')))
                else:
                    raise Exception
            elif ttype in tk.Name or ttype in tk.Keyword:
                stream.append((tk.Name, token.value))
            else:
                stream.append((tk.Generic, token.value))
        self.stream = iter(stream)
        self.next_value = None
        self.advance()

    def match(self, pattern):
        if self.next_type is None:
            return False
        if isinstance(pattern, str):
            return (
                self.next_type is tk.Name or self.next_type is tk.Generic
            ) and self.next_value.upper() == pattern.upper()
        else:
            return self.next_type is pattern

    def accept(self, pattern):
        if self.match(pattern):
            self.advance()
            return True
        return False

    def expect(self, pattern):
        if not self.accept(pattern):
            raise Exception

    def advance(self):
        self.value = self.next_value
        try:
            self.next_type, self.next_value = next(self.stream)
        except StopIteration:
            self.next_type = self.next_value = None

    def export(self, schema, name, obj):
        assert schema == self.schema
        setattr(self.module, name, obj)


class Copy(Executable, ClauseElement):
    """
    PGCOPY executable clause.

    Expects rows as an iterable multi-parameter.
    """

    def __init__(self, text):
        self.text = text

    def _execute_on_connection(self, connection, multiparams, params):
        (rows,) = multiparams
        if connection._echo:
            connection.engine.logger.info("%s", self.text)
        with BytesIO() as temp:
            pg_write(rows, temp)
            temp.seek(0)
            with connection.connection.cursor() as cursor:
                cursor.copy_expert(self.text, temp)

    def __str__(self):
        return self.text


def pg_write(rows, output):
    """Convert rows into PGCOPY binary format and write them to output."""

    int8 = Struct("!B").pack
    int16 = Struct("!h").pack
    int32 = Struct("!i").pack
    int64 = Struct("!q").pack
    double = Struct("!d").pack

    null = int32(-1)
    bool_len = int32(1)
    int32_len = int32(4)
    int64_len = int32(8)
    double_len = int32(8)
    uuid_len = int32(16)

    write = output.write

    write(b"PGCOPY\n\377\r\n\0")
    write(int32(0))
    write(int32(0))

    for row in rows:
        write(int16(len(row)))
        for value in row:
            if value is None:
                write(null)
            elif value is False:
                write(bool_len)
                write(int8(0))
            elif value is True:
                write(bool_len)
                write(int8(1))
            elif isinstance(value, int):
                write(int32_len)
                write(int32(value))
            elif isinstance(value, float):
                write(double_len)
                write(double(value))
            elif isinstance(value, str):
                encoded = value.encode("utf-8")
                write(int32(len(encoded)))
                write(encoded)
            elif isinstance(value, bytes):
                write(int32(len(value)))
                write(value)
            elif isinstance(value, datetime):
                write(int64_len)
                write(int64(pg_timestamp(value)))
            elif isinstance(value, UUID):
                write(uuid_len)
                write(value.bytes)
            else:
                raise ValueError(value)

    write(int16(-1))


def pg_timestamp(t):
    """Convert datetime to Postgres internal representation."""
    date = date2j(t.year, t.month, t.day) - 2451545
    time = ((t.hour * 60 + t.minute) * 60 + t.second) * 1000000 + t.microsecond
    return date * 86400000000 + time


def date2j(y, m, d):
    """Calculate Julian day number."""
    if m > 2:
        m += 1
        y += 4800
    else:
        m += 13
        y += 4799
    century = y // 100
    julian = y * 365 - 32167
    julian += y // 4 - century + century // 4
    julian += 7834 * m // 256 + d
    return julian
