from enum import Enum
from sqlalchemy import create_mock_engine, MetaData, Text
from sqlelixir import SQLElixir


class TestState(Enum):
    PENDING = "pending"
    COMPLETED = "completed"


def dump(sql, *multiparams, **params):
    print(sql.compile(dialect=engine.dialect))


if __name__ == "__main__":
    metadata = MetaData()

    elixir = SQLElixir(metadata)
    elixir.register_type(Text, "box")
    elixir.register_enum(
        TestState, "test_states", schema="test", values_callable=elixir.enum_values
    )
    elixir.import_module("test.sql")

    engine = create_mock_engine('postgresql://', dump)
    metadata.create_all(engine, checkfirst=False)
