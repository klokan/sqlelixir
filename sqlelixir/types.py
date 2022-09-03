from collections import defaultdict
from typing import Type
from xml.etree import ElementTree

import enum

import sqlalchemy.dialects.postgresql as postgres
import sqlalchemy.sql.sqltypes as standard

from sqlalchemy.sql.type_api import TypeEngine
from sqlalchemy.types import UserDefinedType, TypeDecorator


def custom_enum_type(enum_type, data_type):
    class CustomEnumType(TypeDecorator):
        cache_ok = True
        impl = data_type

        def process_bind_param(self, value, dialect):
            return value.value

        def process_result_value(self, value, dialect):
            return enum_type(value)

    return CustomEnumType


class XMLType(UserDefinedType):
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


builtin_types = {
    "bigint": standard.BigInteger,
    "bigserial": standard.BigInteger,
    "boolean": standard.Boolean,
    "citext": standard.Text,
    "date": standard.Date,
    "double precision": standard.Float,
    "float": standard.Float,
    "int": standard.Integer,
    "integer": standard.Integer,
    "interval": standard.Interval,
    "serial": standard.Integer,
    "text": standard.Text,
    "time": standard.Time,
    "timestamp": standard.DateTime,
    "timestamptz": standard.DateTime(timezone=True),
    "bytea": postgres.BYTEA,
    "daterange": postgres.DATERANGE,
    "inet": postgres.INET,
    "int4range": postgres.INT4RANGE,
    "int8range": postgres.INT8RANGE,
    "json": postgres.JSON(none_as_null=True),
    "jsonb": postgres.JSONB(none_as_null=True),
    "numeric": postgres.NUMERIC,
    "tsrange": postgres.TSRANGE,
    "tstzrange": postgres.TSTZRANGE,
    "tsvector": postgres.TSVECTOR,
    "uuid": postgres.UUID(as_uuid=True),
    "xml": XMLType,
}


class TypeRegistry:
    types: defaultdict[str | None, dict[str, TypeEngine]]

    def __init__(self):
        self.types = defaultdict(dict)
        self.types[None] = builtin_types.copy()

    def add(self, schema: str | None, name: str, type_: TypeEngine):
        normalized_name = name.lower()
        if normalized_name in self.types[schema]:
            raise RuntimeError("Type already registered", schema, name)

        self.types[schema][normalized_name] = type_

    def get(self, schema: str | None, name: str) -> TypeEngine | None:
        return self.types[schema].get(name.lower())


def python_enum_values(enum: Type[enum.Enum]) -> list[str]:
    return [member.value for member in enum]
