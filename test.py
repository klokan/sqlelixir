from sqlalchemy.ext.declarative import declarative_base
from sqlelixir import SQLElixir
Base = declarative_base()
elixir = SQLElixir(Base.metadata)
test = elixir.import_module("test.sql")
print(repr(test.test_states))
print(repr(test.test_parents))
print(repr(test.test_children))
print(repr(test.test_function))
