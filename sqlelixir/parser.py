from __future__ import annotations
from collections.abc import Iterator
from importlib import import_module
from io import TextIOBase
from typing import Any

import enum

from sqlalchemy.schema import (
    CheckConstraint,
    Column,
    ColumnDefault,
    Computed,
    Constraint,
    DefaultClause,
    ForeignKeyConstraint,
    Identity,
    Index,
    PrimaryKeyConstraint,
    Table,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import ARRAY, ExcludeConstraint
from sqlalchemy.schema import MetaData
from sqlalchemy.sql import text as text_expression
from sqlalchemy.sql import func as func_generator
from sqlalchemy.sql.expression import TextClause
from sqlalchemy.sql.type_api import TypeEngine
from sqlalchemy.types import Enum, Text

from sqlparse.engine import StatementSplitter
from sqlparse.lexer import tokenize
from sqlparse.sql import Token
from sqlparse.tokens import (
    Comment,
    Name,
    Number,
    Operator,
    Punctuation,
    String,
    Literal,
)

from sqlelixir.types import TypeRegistry, custom_enum_type, python_enum_values


class Procedure:
    name: str

    def __init__(self, schema: str | None, name: str):
        if schema is not None:
            self.name = f"{schema}.{name}"
        else:
            self.name = name

    def __call__(self, *args):
        if not args:
            return text_expression(f"CALL {self.name}()")

        params = ", ".join(f":arg{i}" for i in range(len(args)))
        clause = text_expression(f"CALL {self.name}({params})").bindparams()
        kwargs = {f"arg{i}": arg for i, arg in enumerate(args)}
        return clause.bindparams(**kwargs)


class Parser:
    types: TypeRegistry
    metadata: MetaData

    schema: str | None
    module: Any

    stream: Iterator[tuple[int, Token]]
    stream_sentinel = tuple[int, Token]
    tokens: list[Token]
    token: Token
    index: int

    def __init__(self, types: TypeRegistry, metadata: MetaData):
        self.types = types
        self.metadata = metadata

    def declare_schema(self, schema: str):
        if self.schema is not None:
            raise RuntimeError("Schema already declared")

        self.schema = schema

    def export(self, schema: str | None, name: str, obj: Any):
        if schema != self.schema:
            raise RuntimeError("Invalid schema")

        setattr(self.module, name, obj)

    def parse(self, sql: str | TextIOBase, module: Any):
        self.schema = None
        self.module = module

        for statement in StatementSplitter().process(tokenize(sql)):
            self.stream = enumerate(statement.tokens)
            self.stream_sentinel = (len(statement.tokens), Token(Punctuation, ";"))
            self.tokens = statement.tokens
            self.advance()

            if self.accept_keyword("CREATE"):
                if self.accept_keyword("SCHEMA"):
                    schema = self.expect_name()
                    self.declare_schema(schema)
                elif self.accept_keyword("TYPE"):
                    self.parse_create_type()
                elif self.accept_keyword("TABLE"):
                    self.parse_create_table()
                elif self.accept_keyword("TEMPORARY"):
                    self.expect_keyword("TABLE")
                    self.parse_create_table(temporary=True)
                elif self.accept_keyword("INDEX"):
                    self.parse_create_index(unique=False)
                elif self.accept_keyword("UNIQUE"):
                    self.expect_keyword("INDEX")
                    self.parse_create_index(unique=True)
                elif self.accept_keyword("VIEW"):
                    self.parse_create_view()
                elif self.accept_keyword("MATERIALIZED") or self.accept_keyword(
                    "RECURSIVE"
                ):
                    self.expect_keyword("VIEW")
                    self.parse_create_view()
                elif self.accept_keyword("FUNCTION"):
                    self.parse_function()
                elif self.accept_keyword("PROCEDURE"):
                    self.parse_procedure()

            elif self.keyword_is_next("PREPARE"):
                self.parse_prepare()

    def parse_create_type(self) -> None:
        schema, name = self.parse_identifier()
        self.expect_keyword("AS")

        if self.accept_keyword("ENUM"):
            values = []
            enum_type = None
            data_type = None
            attribute = None

            if self.accept_punctuation("("):
                while True:
                    value = self.expect_string()
                    values.append(value)

                    if self.accept_punctuation(","):
                        continue
                    else:
                        break

                self.expect_punctuation(")")

            if self.accept_keyword("PRAGMA"):
                self.expect_punctuation("(")

                while True:
                    if self.accept_keyword("CLASS"):
                        full_name = self.expect_string()
                        module_name, separator, class_name = full_name.rpartition(".")
                        assert module_name
                        assert separator == "."
                        assert class_name
                        module = import_module(module_name)
                        enum_type = getattr(module, class_name)
                    elif self.accept_keyword("DATA"):
                        self.expect_keyword("TYPE")
                        type_name = self.expect_name()
                        data_type = self.types.get(None, type_name)
                        if data_type is None:
                            raise RuntimeError("Unknown type", type_name)
                    elif self.accept_keyword("ATTRIBUTE"):
                        attribute = self.expect_string()
                    else:
                        raise RuntimeError("Invalid enum pragma")

                    if self.accept_punctuation(","):
                        continue
                    else:
                        break

                self.expect_punctuation(")")

            # Clients can register types.
            type_ = self.types.get(schema, name)
            if type_ is None:
                if enum_type is not None:
                    if data_type is None and attribute is None:
                        type_ = Enum(
                            enum_type,
                            metadata=self.metadata,
                            schema=schema,
                            name=name,
                            native_enum=True,
                            values_callable=python_enum_values,
                        )
                    elif data_type is Text and attribute is None:
                        type_ = Enum(
                            enum_type,
                            metadata=self.metadata,
                            schema=schema,
                            name=name,
                            native_enum=False,
                            values_callable=python_enum_values,
                        )
                    else:
                        if values:
                            raise RuntimeError("Enum values not allowed", schema, name)
                        if data_type is None:
                            raise RuntimeError("Enum data type required")

                        type_ = custom_enum_type(enum_type, data_type, attribute)
                else:
                    if not values:
                        raise RuntimeError("Enum values not specified", schema, name)
                    if attribute is not None:
                        raise RuntimeError("Enum attribute not allowed")

                    if data_type is None:
                        type_ = Enum(
                            *values,
                            metadata=self.metadata,
                            schema=schema,
                            name=name,
                            native_enum=True,
                        )
                    elif data_type is Text:
                        type_ = Enum(
                            *values,
                            metadata=self.metadata,
                            schema=schema,
                            name=name,
                            native_enum=False,
                        )
                    else:
                        raise RuntimeError("Invalid enum data type", data_type)

                self.types.add(schema, name, type_)

            self.export(schema, name, type_)

    def parse_create_table(self, temporary: bool = False) -> None:
        schema, name = self.parse_identifier()

        table = Table(
            name,
            self.metadata,
            schema=schema,
            prefixes=["TEMPORARY"] if temporary else None,
        )
        constraints = []

        self.expect_punctuation("(")

        while True:
            if (table_constraint := self.parse_table_constraint()) is not None:
                constraints.append(table_constraint)
            else:
                column, column_constraints = self.parse_column()
                table.append_column(column)
                constraints.extend(column_constraints)

            self.parse_until_end_of_expression()

            if self.accept_punctuation(","):
                continue
            else:
                break

        self.expect_punctuation(")")

        for constraint in constraints:
            table.append_constraint(constraint)

        self.export(schema, name, table)

    def parse_table_constraint(self) -> Constraint | None:
        if self.accept_keyword("CONSTRAINT"):
            name = self.expect_name()
        else:
            name = None

        if self.keyword_is_next("PRIMARY"):
            return self.parse_primary_key_table_constraint(name)

        if self.keyword_is_next("UNIQUE"):
            return self.parse_unique_table_constraint(name)

        if self.keyword_is_next("EXCLUDE"):
            return self.parse_exclude_table_constraint(name)

        if self.keyword_is_next("FOREIGN"):
            return self.parse_foreign_key_table_constraint(name)

        if self.keyword_is_next("CHECK"):
            return self.parse_check_constraint(name)

        return None

    def parse_column(self) -> tuple[Column, list[Constraint]]:
        name = self.expect_name()
        type_ = self.parse_column_type()

        if self.accept_keyword("AS"):
            key = self.expect_name()
        else:
            key = name

        nullable = True
        items = []
        constraints = []

        while True:
            if self.accept_keyword("NOT NULL"):
                nullable = False

            elif self.keyword_is_next("DEFAULT"):
                server_default, column_default = self.parse_column_default(type_)
                items.append(server_default)
                if column_default is not None:
                    items.append(column_default)

            elif self.keyword_is_next("GENERATED"):
                generated = self.parse_column_generated()
                items.append(generated)

            elif (constraint := self.parse_column_constraint(key)) is not None:
                constraints.append(constraint)

            else:
                break

        column = Column(name, type_, *items, key=key, nullable=nullable)
        return column, constraints

    def parse_column_constraint(self, column: str) -> Constraint | None:
        if self.accept_keyword("CONSTRAINT"):
            name = self.expect_name()
        else:
            name = None

        if self.keyword_is_next("PRIMARY"):
            return self.parse_primary_key_column_constraint(name, column)

        if self.keyword_is_next("UNIQUE"):
            return self.parse_unique_column_constraint(name, column)

        if self.keyword_is_next("REFERENCES"):
            return self.parse_foreign_key_column_constraint(name, column)

        if self.keyword_is_next("CHECK"):
            return self.parse_check_constraint(name)

        return None

    def parse_column_type(self) -> TypeEngine:
        schema, name = self.parse_identifier()

        type_ = self.types.get(schema, name)
        if type_ is None:
            raise RuntimeError("Unknown type", schema, name)

        if self.accept_punctuation("["):
            dimensions = 1
            self.accept_number()
            self.expect_punctuation("]")

            while self.accept_punctuation("["):
                dimensions += 1
                self.accept_number()
                self.expect_punctuation("]")

            return ARRAY(type_, dimensions=dimensions)
        else:
            return type_

    def parse_column_default(
        self, type_: TypeEngine
    ) -> tuple[DefaultClause, ColumnDefault | None]:
        self.expect_keyword("DEFAULT")

        start = self.index

        if self.accept_keyword("TRUE"):
            value = True
            end = start + 1
        elif self.accept_keyword("FALSE"):
            value = False
            end = start + 1
        elif self.number_is_next():
            value = self.expect_number()
            end = start + 1
        elif self.string_is_next():
            value = self.expect_string()
            if isinstance(type_, Enum) and issubclass(type_.python_type, enum.Enum):
                value = type_.python_type(value)
            end = start + 1
        else:
            value = None
            end = self.parse_until_end_of_expression()

        expression = self.format(start, end)
        server_default = DefaultClause(text_expression(expression))

        if value is not None:
            column_default = ColumnDefault(value)
        else:
            column_default = None

        return server_default, column_default

    def parse_column_generated(
        self,
    ) -> Computed | Identity:
        self.expect_keyword("GENERATED")

        if self.accept_keyword("ALWAYS"):
            always = True
        else:
            always = False
            if self.accept_keyword("BY"):
                self.expect_keyword("DEFAULT")

        self.expect_keyword("AS")

        if self.accept_keyword("IDENTITY"):
            return Identity(always=always)
        else:
            assert always
            expression = self.parse_enclosed_expression()
            self.expect_keyword("STORED")
            return Computed(expression)

    def parse_primary_key_table_constraint(
        self, name: str | None
    ) -> PrimaryKeyConstraint:
        self.expect_keyword("PRIMARY")
        self.expect_keyword("KEY")
        columns = self.parse_column_list()
        return PrimaryKeyConstraint(*columns, name=name)

    def parse_primary_key_column_constraint(
        self, name: str | None, column: str
    ) -> PrimaryKeyConstraint:
        self.expect_keyword("PRIMARY")
        self.expect_keyword("KEY")
        return PrimaryKeyConstraint(column, name=name)

    def parse_unique_table_constraint(self, name: str | None) -> UniqueConstraint:
        self.expect_keyword("UNIQUE")
        columns = self.parse_column_list()
        return UniqueConstraint(*columns, name=name)

    def parse_unique_column_constraint(
        self, name: str | None, column: str
    ) -> UniqueConstraint:
        self.expect_keyword("UNIQUE")
        return UniqueConstraint(column, name=name)

    def parse_exclude_table_constraint(self, name: str | None) -> ExcludeConstraint:
        self.expect_keyword("EXCLUDE")

        if self.accept_keyword("USING"):
            using = self.expect_name()
        else:
            using = "GIST"

        self.expect_punctuation("(")

        elements = []
        while True:
            column = self.expect_name()
            self.expect_keyword("WITH")
            operator = self.expect_operator()
            elements.append((column, operator))

            if self.accept_punctuation(","):
                continue
            else:
                break

        self.expect_punctuation(")")

        if self.accept_keyword("WHERE"):
            where = self.parse_enclosed_expression()
        else:
            where = None

        return ExcludeConstraint(
            *elements,
            name=name,
            using=using,
            where=where,
        )

    def parse_foreign_key_table_constraint(
        self, name: str | None
    ) -> ForeignKeyConstraint:
        self.expect_keyword("FOREIGN")
        self.expect_keyword("KEY")
        columns = self.parse_column_list()
        return self.parse_foreign_key_constraint(name, columns)

    def parse_foreign_key_column_constraint(
        self, name: str | None, column: str
    ) -> ForeignKeyConstraint:
        return self.parse_foreign_key_constraint(name, [column])

    def parse_foreign_key_constraint(
        self, name: str | None, columns: list[str]
    ) -> ForeignKeyConstraint:
        self.expect_keyword("REFERENCES")

        foreign_schema, foreign_table = self.parse_identifier()
        foreign_columns = self.parse_column_list()

        if foreign_schema is not None:
            refbase = f"{foreign_schema}.{foreign_table}"
        else:
            refbase = foreign_table

        refcolumns = [f"{refbase}.{column}" for column in foreign_columns]

        onupdate = None
        ondelete = None
        while self.accept_keyword("ON"):
            if self.accept_keyword("UPDATE"):
                onupdate = self.parse_foreign_key_action()
            elif self.accept_keyword("DELETE"):
                ondelete = self.parse_foreign_key_action()
            else:
                raise RuntimeError("Invalid foreign key action")

        use_alter = False
        if self.accept_keyword("PRAGMA"):
            self.expect_punctuation("(")

            while True:
                pragma = self.expect_name()
                if pragma == "use_alter":
                    use_alter = True

                if self.accept_punctuation(","):
                    continue
                else:
                    break

            self.expect_punctuation(")")

        return ForeignKeyConstraint(
            columns,
            refcolumns,
            name=name,
            onupdate=onupdate,
            ondelete=ondelete,
            use_alter=use_alter,
        )

    def parse_foreign_key_action(self) -> str:
        if self.accept_keyword("CASCADE"):
            return "CASCADE"
        if self.accept_keyword("RESTRICT"):
            return "RESTRICT"

        if self.accept_keyword("SET"):
            if self.accept_keyword("NULL"):
                return "SET NULL"
            if self.accept_keyword("DEFAULT"):
                return "SET DEFAULT"

        raise RuntimeError("Invalid foreign key action")

    def parse_check_constraint(self, name: str | None) -> CheckConstraint:
        self.expect_keyword("CHECK")
        expression = self.parse_enclosed_expression()
        return CheckConstraint(expression, name=name)

    def parse_create_index(self, unique: bool):
        name = self.expect_name()

        self.expect_keyword("ON")
        schema, table_name = self.parse_identifier()

        if schema is not None:
            table = self.metadata.tables[f"{schema}.{table_name}"]
        else:
            table = self.metadata.tables[table_name]

        if self.accept_keyword("USING"):
            using = self.expect_name()
        else:
            using = None

        expressions = self.parse_index_expression_list()

        if self.accept_keyword("INCLUDE"):
            include = self.parse_column_list()
        else:
            include = None

        if self.accept_keyword("WHERE"):
            where = self.parse_enclosed_expression()
        else:
            where = None

        index = Index(
            name,
            *expressions,
            unique=unique,
            postgresql_using=using,
            postgresql_include=include,
            postgresql_where=where,
        )
        table.append_constraint(index)

    def parse_create_view(self) -> None:
        schema, name = self.parse_identifier()
        table = Table(name, self.metadata, schema=schema)

        if self.accept_punctuation("("):
            while True:
                column_name = self.expect_name()
                table.append_column(Column(column_name))

                if self.accept_punctuation(","):
                    continue
                else:
                    break

            self.expect_punctuation(")")

        self.export(schema, name, table)

    def parse_identifier(self) -> tuple[str | None, str]:
        name1 = self.expect_name()
        if self.accept_punctuation("."):
            name2 = self.expect_name()
            return name1, name2
        else:
            return None, name1

    def parse_column_list(self) -> list[str]:
        columns = []
        self.expect_punctuation("(")

        while True:
            column = self.expect_name()
            columns.append(column)

            if self.accept_punctuation(","):
                continue
            else:
                break

        self.expect_punctuation(")")
        return columns

    def parse_enclosed_expression(self) -> str:
        depth = 0

        self.expect_punctuation("(")
        start = end = self.index

        while True:
            if self.accept_punctuation("("):
                depth += 1
                continue

            if self.punctuation_is_next(")"):
                if depth > 0:
                    depth -= 1
                else:
                    break

            end = self.index + 1
            self.advance()

        self.expect_punctuation(")")

        return self.format(start, end)

    def parse_index_expression_list(self) -> list[str | TextClause]:
        expressions = []
        self.expect_punctuation("(")

        while True:
            expression = self.parse_index_expression()
            expressions.append(expression)

            if self.accept_punctuation(","):
                continue
            else:
                break

        self.expect_punctuation(")")
        return expressions

    def parse_index_expression(self) -> str | TextClause:
        start = self.index

        if (column := self.accept_name()) is not None:
            if self.punctuation_is_next(",") or self.punctuation_is_next(")"):
                return column

        end = self.parse_until_end_of_expression()
        if end == start:
            raise RuntimeError("Empty index expression")

        text = self.format(start, end)
        return text_expression(text)

    def parse_until_end_of_expression(self) -> int:
        end = self.index
        depth = 0

        while True:
            if self.accept_punctuation("("):
                depth += 1
                continue

            if self.punctuation_is_next(","):
                if depth == 0:
                    break

            if self.punctuation_is_next(")"):
                if depth == 0:
                    break
                else:
                    depth -= 1

            end = self.index + 1
            self.advance()

        return end

    def parse_function(self) -> None:
        schema, name = self.parse_identifier()

        if schema is not None:
            clause = getattr(getattr(func_generator, schema), name)
        else:
            clause = getattr(func_generator, name)

        self.export(schema, name, clause)

    def parse_procedure(self) -> None:
        schema, name = self.parse_identifier()
        self.export(schema, name, Procedure(schema, name))

    def parse_prepare(self) -> None:
        self.expect_keyword("PREPARE")
        schema, name = self.parse_identifier()
        self.expect_keyword("AS")

        start = end = self.index
        while not self.accept_punctuation(";"):
            end = self.index + 1
            self.advance()

        if start == end:
            raise RuntimeError("Empty prepared statement")

        expression = self.format(start, end)
        clause = text_expression(expression)

        self.export(schema, name, clause)

    def accept_name(self) -> str | None:
        if self.token.is_keyword:
            name = self.token.value
        elif self.token.ttype in Name:
            name = self.token.value
        elif self.token.ttype is String.Symbol:
            name = self.token.value.strip('"')
        else:
            return None

        self.advance()
        return name

    def expect_name(self) -> str:
        name = self.accept_name()
        if name is None:
            raise RuntimeError("Expected name")
        return name

    def accept_operator(self) -> str | None:
        if self.token.ttype in Operator:
            value = self.token.value
            self.advance()
            return value
        else:
            return None

    def expect_operator(self) -> str:
        operator = self.accept_operator()
        if operator is None:
            raise RuntimeError("Expected operator")
        return operator

    def string_is_next(self) -> bool:
        return self.token.ttype in String

    def accept_string(self) -> str | None:
        if self.token.ttype is String.Single:
            value = self.token.value.strip("'")
            self.advance()
            return value
        else:
            return None

    def expect_string(self) -> str:
        value = self.accept_string()
        if value is None:
            raise RuntimeError("Expected string")
        return value

    def accept_literal(self) -> str | None:
        if self.token.ttype is Literal:
            value = self.token.value.removeprefix("$$").removesuffix("$$").strip()
            self.advance()
            return value
        else:
            return None

    def expect_literal(self) -> str:
        value = self.accept_literal()
        if value is None:
            raise RuntimeError("Expected literal")
        return value

    def number_is_next(self) -> bool:
        return self.token.ttype in Number

    def accept_number(self) -> int | float | None:
        if self.token.ttype is Number.Integer:
            value = int(self.token.value)
        elif self.token.ttype is Number.Float:
            value = float(self.token.value)
        else:
            return None

        self.advance()
        return value

    def expect_number(self) -> int | float:
        value = self.accept_number()
        if value is None:
            raise RuntimeError("Expected number")
        return value

    def keyword_is_next(self, value: str) -> bool:
        if self.token.is_keyword and self.token.normalized == value:
            return True
        # ENUM is not a keyword
        elif self.token.ttype in Name and self.token.value.upper() == value:
            return True
        else:
            return False

    def accept_keyword(self, value: str) -> bool:
        if self.keyword_is_next(value):
            self.advance()
            return True
        else:
            return False

    def expect_keyword(self, value: str):
        if not self.accept_keyword(value):
            raise RuntimeError("Expected keyword", value)

    def punctuation_is_next(self, value: str) -> bool:
        return self.token.ttype is Punctuation and self.token.value == value

    def accept_punctuation(self, value: str) -> bool:
        if self.punctuation_is_next(value):
            self.advance()
            return True
        else:
            return False

    def expect_punctuation(self, value: str):
        if not self.accept_punctuation(value):
            raise RuntimeError("Expected punctuation", value)

    def advance(self):
        index, token = next(self.stream, self.stream_sentinel)
        while token.is_whitespace or token.ttype in Comment:
            index, token = next(self.stream, self.stream_sentinel)

        self.index = index
        self.token = token

    def format(self, start: int, end: int) -> str:
        return "".join(token.value for token in self.tokens[start:end])
