import enum

from types import SimpleNamespace

import pytest

from sqlalchemy.dialects.postgresql import ARRAY, ExcludeConstraint, UUID
from sqlalchemy.engine import create_mock_engine
from sqlalchemy.sql.expression import TextClause
from sqlalchemy.schema import (
    Constraint,
    Table,
    CheckConstraint,
    ForeignKeyConstraint,
    PrimaryKeyConstraint,
    UniqueConstraint,
)
from sqlalchemy.types import (
    Boolean,
    Enum,
    Float,
    Integer,
    Text,
    TypeDecorator,
    NullType,
)

from sqlelixir import SQLElixir


class WidgetType(enum.Enum):
    BASIC = "basic"
    CUSTOM = "custom"


class NumberType(enum.Enum):
    ONE = 1
    TWO = 2


@pytest.fixture
def elixir() -> SQLElixir:
    return SQLElixir()


@pytest.fixture
def module() -> SimpleNamespace:
    return SimpleNamespace()


def find_constraint(table: Table, name: str) -> Constraint:
    for constraint in table.constraints:
        if constraint.name == name:
            return constraint

    raise AssertionError("Constraint not found", name)


def test_create_schema(elixir: SQLElixir, module: SimpleNamespace):
    elixir.parse("CREATE SCHEMA test;", module)


def test_create_schema_twice(elixir: SQLElixir, module: SimpleNamespace):
    sql = """
    CREATE SCHEMA test;
    CREATE SCHEMA test2;
    """

    with pytest.raises(RuntimeError):
        elixir.parse(sql, module)


def test_create_enum_python(elixir: SQLElixir, module: SimpleNamespace):
    sql = """
    CREATE TYPE widget_types AS ENUM
    PRAGMA (CLASS 'sqlelixir.base_test.WidgetType');
    """

    elixir.parse(sql, module)

    assert isinstance(module.widget_types, Enum)
    assert module.widget_types.name == "widget_types"
    assert module.widget_types.enums == ["basic", "custom"]
    assert module.widget_types.python_type is WidgetType
    assert module.widget_types.native is True


def test_create_enum_python_non_native(elixir: SQLElixir, module: SimpleNamespace):
    sql = """
    CREATE TYPE widget_types AS ENUM
    PRAGMA (CLASS 'sqlelixir.base_test.WidgetType', DATA TYPE text);
    """

    elixir.parse(sql, module)

    assert isinstance(module.widget_types, Enum)
    assert module.widget_types.name == "widget_types"
    assert module.widget_types.enums == ["basic", "custom"]
    assert module.widget_types.python_type is WidgetType
    assert module.widget_types.native is False


def test_create_enum_string(elixir: SQLElixir, module: SimpleNamespace):
    sql = """
    CREATE TYPE widget_types AS ENUM ('basic', 'custom');
    """

    elixir.parse(sql, module)

    assert isinstance(module.widget_types, Enum)
    assert module.widget_types.name == "widget_types"
    assert module.widget_types.enums == ["basic", "custom"]
    assert module.widget_types.python_type is str
    assert module.widget_types.native is True


def test_create_enum_int(elixir: SQLElixir, module: SimpleNamespace):
    sql = """
    CREATE TYPE numbers AS ENUM
    PRAGMA (CLASS 'sqlelixir.base_test.NumberType', DATA TYPE int);
    """

    elixir.parse(sql, module)

    assert issubclass(module.numbers, TypeDecorator)
    assert module.numbers.impl is Integer


def test_create_table_column_names(elixir: SQLElixir, module: SimpleNamespace):
    sql = """
    CREATE TABLE test (
        normal_column int,
        "quoted_column" int,
        original_column int AS aliased_column
    )
    """

    elixir.parse(sql, module)

    table = module.test
    assert isinstance(table, Table)

    assert table.c.normal_column.name == "normal_column"
    assert table.c.normal_column.key == "normal_column"

    assert table.c.quoted_column.name == "quoted_column"
    assert table.c.quoted_column.key == "quoted_column"

    assert table.c.aliased_column.name == "original_column"
    assert table.c.aliased_column.key == "aliased_column"


def test_create_table_column_types(elixir: SQLElixir, module: SimpleNamespace):
    sql = """
    CREATE TYPE widget_types AS ENUM ('basic', 'custom');
    CREATE TABLE test (
        boolean_column boolean,
        integer_column int,
        double_column double precision,
        text_column text,
        enum_column widget_types,
        uuid_column uuid,
        array_column int[][3]
    );
    """

    elixir.parse(sql, module)

    table = module.test
    assert isinstance(table, Table)
    assert isinstance(table.c.boolean_column.type, Boolean)
    assert isinstance(table.c.integer_column.type, Integer)
    assert isinstance(table.c.double_column.type, Float)
    assert isinstance(table.c.text_column.type, Text)
    assert isinstance(table.c.enum_column.type, Enum)
    assert isinstance(table.c.uuid_column.type, UUID)
    assert isinstance(table.c.array_column.type, ARRAY)
    assert table.c.array_column.type.dimensions == 2


def test_create_table_column_types_case(elixir: SQLElixir, module: SimpleNamespace):
    sql = """
    CREATE TABLE test (
        boolean_column Boolean,
        integer_column INTEGER
    );
    """

    elixir.parse(sql, module)

    table = module.test
    assert isinstance(table.c.boolean_column.type, Boolean)
    assert isinstance(table.c.integer_column.type, Integer)


def test_create_table_defaults(elixir: SQLElixir, module: SimpleNamespace):
    sql = """
    CREATE TYPE widget_types AS ENUM ('basic', 'custom')
         PRAGMA (CLASS 'sqlelixir.base_test.WidgetType');
    CREATE TYPE widget_sizes AS ENUM ('small', 'large');
    CREATE TABLE test (
        default_true boolean DEFAULT TRUE,
        default_false boolean DEFAULT FALSE,
        default_number double precision DEFAULT 1.25,
        default_text text DEFAULT 'draft',
        default_enum_registered widget_types DEFAULT 'basic',
        default_enum_unregistered widget_sizes DEFAULT 'small',
        default_expression uuid DEFAULT gen_random_uuid()
    );
    """

    elixir.parse(sql, module)

    table = module.test
    assert isinstance(table, Table)

    default_true = table.c.default_true
    assert default_true.default.arg is True
    assert default_true.server_default.arg.text == "TRUE"

    default_false = table.c.default_false
    assert default_false.default.arg is False
    assert default_false.server_default.arg.text == "FALSE"

    default_number = table.c.default_number
    assert default_number.default.arg == 1.25
    assert default_number.server_default.arg.text == "1.25"

    default_text = table.c.default_text
    assert default_text.default.arg == "draft"
    assert default_text.server_default.arg.text == "'draft'"

    default_enum_registered = table.c.default_enum_registered
    assert default_enum_registered.default.arg is WidgetType.BASIC
    assert default_enum_registered.server_default.arg.text == "'basic'"

    default_enum_unregistered = table.c.default_enum_unregistered
    assert default_enum_unregistered.default.arg == "small"
    assert default_enum_unregistered.server_default.arg.text == "'small'"

    default_expression = table.c.default_expression
    assert default_expression.server_default.arg.text == "gen_random_uuid()"


def test_create_table_generated(elixir: SQLElixir, module: SimpleNamespace):
    sql = """
    CREATE TABLE test (
        identity_column int GENERATED BY DEFAULT AS IDENTITY,
        computed_column int GENERATED ALWAYS AS (length(source_column)) STORED,
        source_column text
    );
    """

    elixir.parse(sql, module)

    table = module.test
    assert isinstance(table, Table)
    assert table.c.identity_column.identity.always is False
    assert table.c.computed_column.computed.sqltext.text == "length(source_column)"


@pytest.mark.parametrize(
    "sql",
    [
        # Implicit table constraint
        """
        CREATE TABLE widgets (
            PRIMARY KEY (widget_id),
            widget_id uuid NOT NULL
        );
        """,
        # Named table constraint
        """
        CREATE TABLE widgets (
            widget_id uuid NOT NULL,
            CONSTRAINT widgets_pkey PRIMARY KEY (widget_id)
        );
        """,
        # Implicit column constraint
        """
        CREATE TABLE widgets (
            widget_id uuid NOT NULL PRIMARY KEY
        );
        """,
        # Named column constraint
        """
        CREATE TABLE widgets (
            widget_id uuid NOT NULL CONSTRAINT widgets_pkey PRIMARY KEY
        );
        """,
    ],
)
def test_create_table_primary_key_constraint_simple(
    elixir: SQLElixir, module: SimpleNamespace, sql: str
):
    elixir.parse(sql, module)

    table = module.widgets
    assert isinstance(table, Table)
    assert table.c.widget_id.primary_key

    constraint = find_constraint(table, "widgets_pkey")
    assert isinstance(constraint, PrimaryKeyConstraint)
    assert constraint.columns.widget_id is table.c.widget_id


@pytest.mark.parametrize(
    "sql",
    [
        # Implicit table constraint
        """
        CREATE TABLE widget_items (
            PRIMARY KEY (widget_id, item_id),
            widget_id uuid NOT NULL,
            item_id int NOT NULL
        );
        """,
        # Named table constraint
        """
        CREATE TABLE widget_items (
            widget_id uuid NOT NULL,
            item_id int NOT NULL,
            CONSTRAINT widget_items_pkey PRIMARY KEY (widget_id, item_id)
        );
        """,
    ],
)
def test_create_table_primary_key_constraint_compound(
    elixir: SQLElixir, module: SimpleNamespace, sql: str
):
    elixir.parse(sql, module)

    table = module.widget_items
    assert isinstance(table, Table)
    assert table.c.widget_id.primary_key
    assert table.c.item_id.primary_key

    constraint = find_constraint(table, "widget_items_pkey")
    assert isinstance(constraint, PrimaryKeyConstraint)
    assert constraint.columns.widget_id is table.c.widget_id
    assert constraint.columns.item_id is table.c.item_id


@pytest.mark.parametrize(
    "sql",
    [
        # Implicit table constraint
        """
        CREATE TABLE widgets (
            UNIQUE (widget_id),
            widget_id uuid NOT NULL
        );
        """,
        # Named table constraint
        """
        CREATE TABLE widgets (
            widget_id uuid NOT NULL,
            CONSTRAINT widgets_widget_id_key UNIQUE (widget_id)
        );
        """,
        # Implicit column constraint
        """
        CREATE TABLE widgets (
            widget_id uuid NOT NULL UNIQUE
        );
        """,
        # Named column constraint
        """
        CREATE TABLE widgets (
            widget_id uuid NOT NULL CONSTRAINT widgets_widget_id_key UNIQUE
        );
        """,
    ],
)
def test_create_table_unique_constraint_simple(
    elixir: SQLElixir, module: SimpleNamespace, sql: str
):
    elixir.parse(sql, module)

    table = module.widgets
    assert isinstance(table, Table)

    constraint = find_constraint(table, "widgets_widget_id_key")
    assert isinstance(constraint, UniqueConstraint)
    assert constraint.columns.widget_id is table.c.widget_id


def test_create_table_unique_constraint_compound(
    elixir: SQLElixir, module: SimpleNamespace
):
    sql = """
    CREATE TABLE widget_items (
        widget_id uuid NOT NULL,
        item_id int NOT NULL,
        CONSTRAINT widget_items_widget_id_item_id_key
            UNIQUE (widget_id, item_id)
    );
    """

    elixir.parse(sql, module)

    table = module.widget_items
    assert isinstance(table, Table)

    constraint = find_constraint(table, "widget_items_widget_id_item_id_key")
    assert isinstance(constraint, UniqueConstraint)
    assert constraint.columns.widget_id is table.c.widget_id
    assert constraint.columns.item_id is table.c.item_id


def test_create_table_exclude_constraint(elixir: SQLElixir, module: SimpleNamespace):
    sql = """
    CREATE TABLE widgets (
        account_id uuid NOT NULL,
        active tstzrange NOT NULL,
        deleted timestamptz,
        CONSTRAINT widgets_account_id_active_excl
           EXCLUDE USING GIST (account_id WITH =, active WITH &&)
             WHERE (deleted IS NULL)
    );
    """

    elixir.parse(sql, module)

    table = module.widgets
    assert isinstance(table, Table)

    constraint = find_constraint(table, "widgets_account_id_active_excl")
    assert isinstance(constraint, ExcludeConstraint)
    assert constraint.columns.account_id is table.c.account_id
    assert constraint.columns.active is table.c.active
    assert constraint.using == "GIST"
    assert constraint.where.text == "deleted IS NULL"


@pytest.mark.parametrize(
    "sql",
    [
        # Implicit table constraint
        """
        CREATE TABLE parents (
            parent_id uuid
        );
        CREATE TABLE test (
            parent_id uuid,
            FOREIGN KEY (parent_id) REFERENCES parents (parent_id)
        );
        # """,
        # Named table constraint
        """
        CREATE TABLE parents (
            parent_id uuid
        );
        CREATE TABLE test (
            parent_id uuid,
            CONSTRAINT test_parent_id_fkey
               FOREIGN KEY (parent_id)
            REFERENCES parents (parent_id)
        );
        """,
        # Implicit column constraint
        """
        CREATE TABLE parents (
            parent_id uuid
        );
        CREATE TABLE test (
            parent_id uuid REFERENCES parents (parent_id)
        );
        """,
        # Named column constraint
        """
        CREATE TABLE parents (
            parent_id uuid
        );
        CREATE TABLE test (
            parent_id uuid CONSTRAINT test_parent_id_fkey REFERENCES parents (parent_id)
        );
        """,
    ],
)
def test_create_table_foreign_key_constraint_simple(
    elixir: SQLElixir, module: SimpleNamespace, sql: str
):
    elixir.parse(sql, module)

    table = module.test
    assert isinstance(table, Table)

    constraint = find_constraint(table, "test_parent_id_fkey")
    assert isinstance(constraint, ForeignKeyConstraint)
    assert constraint.referred_table is module.parents
    assert constraint.columns.parent_id is table.c.parent_id


def test_create_table_foreign_key_constraint_compound(
    elixir: SQLElixir, module: SimpleNamespace
):
    sql = """
    CREATE TABLE parents (
        parent_id uuid
        item_id int
    );
    CREATE TABLE test (
        parent_id uuid,
        item_id int,
        CONSTRAINT test_parent_id_item_id_fkey
            FOREIGN KEY (parent_id, item_id)
        REFERENCES parents (parent_id, item_id)
    );
    """

    elixir.parse(sql, module)

    table = module.test
    assert isinstance(table, Table)

    constraint = find_constraint(table, "test_parent_id_item_id_fkey")
    assert isinstance(constraint, ForeignKeyConstraint)
    assert constraint.referred_table is module.parents
    assert constraint.columns.parent_id is table.c.parent_id
    assert constraint.columns.item_id is table.c.item_id


def test_create_table_foreign_key_constraint_cascades(
    elixir: SQLElixir, module: SimpleNamespace
):
    sql = """
    CREATE TABLE parents (
        parent_id uuid
    );
    CREATE TABLE test (
        parent_id uuid REFERENCES parents (parent_id)
            ON UPDATE CASCADE
            ON DELETE SET NULL
    );
    """

    elixir.parse(sql, module)

    table = module.test
    assert isinstance(table, Table)

    constraint = find_constraint(table, "test_parent_id_fkey")
    assert isinstance(constraint, ForeignKeyConstraint)
    assert constraint.onupdate == "CASCADE"
    assert constraint.ondelete == "SET NULL"


def test_create_table_foreign_key_constraint_pragma(
    elixir: SQLElixir, module: SimpleNamespace
):
    sql = """
    CREATE TABLE parents (
        parent_id uuid
    );
    CREATE TABLE test (
        parent_id uuid REFERENCES parents (parent_id) PRAGMA (use_alter)
    );
    """

    elixir.parse(sql, module)

    table = module.test
    assert isinstance(table, Table)

    constraint = find_constraint(table, "test_parent_id_fkey")
    assert isinstance(constraint, ForeignKeyConstraint)
    assert constraint.use_alter


@pytest.mark.parametrize(
    "sql",
    [
        # Implicit table constraint
        """
        CREATE TABLE test (
            active tstzrange,
            CHECK (NOT lower_inf(active))
        );
        """,
        # Named table constraint
        """
        CREATE TABLE test (
            active tstzrange,
            CONSTRAINT test_active_check CHECK (NOT lower_inf(active))
        );
        """,
        # Implicit column constraint
        """
        CREATE TABLE test (
            active tstzrange CHECK (NOT lower_inf(active))
        );
        """,
        # Named column constraint
        """
        CREATE TABLE test (
            active tstzrange CONSTRAINT test_active_check CHECK (NOT lower_inf(active))
        );
        """,
    ],
)
def test_create_table_check_constraint(
    elixir: SQLElixir, module: SimpleNamespace, sql: str
):
    elixir.parse(sql, module)

    table = module.test
    assert isinstance(table, Table)

    for constraint in table.constraints:
        if isinstance(constraint, CheckConstraint):
            assert constraint.sqltext.text == "NOT lower_inf(active)"
            break
    else:
        raise AssertionError("Constraint not found")


def test_create_index(elixir: SQLElixir, module: SimpleNamespace):
    sql = """
    CREATE TABLE test (
        account_id uuid,
        widget_id uuid,
        deleted timestamptz,
        description text
    );
    CREATE UNIQUE INDEX test_account_id_description_fulltext
        ON test USING GIN (account_id, to_tsvector('english', description))
           INCLUDE (widget_id)
     WHERE (deleted IS NULL)
    """

    elixir.parse(sql, module)

    table = module.test
    assert isinstance(table, Table)

    for index in table.indexes:
        if index.name == "test_account_id_description_fulltext":
            assert index.table is table
            assert index.unique
            assert index.dialect_options["postgresql"]["using"] == "GIN"
            assert index.dialect_options["postgresql"]["include"] == ["widget_id"]
            assert index.dialect_options["postgresql"]["where"] == "deleted IS NULL"
            break
    else:
        raise AssertionError("Index not found")


def test_create_view_without_columns(elixir: SQLElixir, module: SimpleNamespace):
    sql = """
    CREATE VIEW test AS
    SELECT * FROM widgets;
    """

    elixir.parse(sql, module)

    table = module.test
    assert isinstance(table, Table)
    assert not table.columns


def test_create_view_with_columns(elixir: SQLElixir, module: SimpleNamespace):
    sql = """
    CREATE VIEW test (widget_id, created) AS
    SELECT * FROM widgets;
    """

    elixir.parse(sql, module)

    table = module.test
    assert isinstance(table, Table)
    assert isinstance(table.c.widget_id.type, NullType)
    assert isinstance(table.c.created.type, NullType)


def test_create_materialized_view(elixir: SQLElixir, module: SimpleNamespace):
    sql = """
    CREATE MATERIALIZED VIEW test AS
    SELECT * FROM widgets;
    """

    elixir.parse(sql, module)

    table = module.test
    assert isinstance(table, Table)
    assert not table.columns


def test_prepare(elixir: SQLElixir, module: SimpleNamespace):
    sql = """
    PREPARE widget_count AS SELECT count(*) FROM widgets WHERE deleted IS NULL;
    """

    elixir.parse(sql, module)

    widget_count = module.widget_count
    assert isinstance(widget_count, TextClause)
    assert widget_count.text == "SELECT count(*) FROM widgets WHERE deleted IS NULL"


# Run with pytest -s to see the output.
def test_importer(elixir: SQLElixir):
    elixir.register_importer("sqlelixir.schema*")
    __import__("sqlelixir.schema_test")

    def dump(sql, *multiparams, **params):
        print(sql.compile(dialect=engine.dialect))

    engine = create_mock_engine("postgresql://", dump)

    print("\n=== CREATE ALL ===\n")
    elixir.metadata.create_all(engine)

    print("\n=== DROP ALL ===\n")
    elixir.metadata.drop_all(engine)
