## Version 2

### v2.19 (2024-03-20)
- Add support for session to pgcopy.

### v2.18 (2024-03-19)
- Add custom class for representing functions.
- Add execution options for executable statements.

### v2.17 (2024-03-15)
- Add support for ON COMMIT clause on temporary tables.

### v2.16 (2024-03-14)
- Add `create_all()`.

### v2.15 (2024-03-14)
- Add support for temporary tables.

### v2.14 (2023-12-06)
- Add support for functions and procedures.
- Add `generate_type_stubs()`.

### v2.12 (2023-04-06)
- Add optional mapping attribute to custom enum.

### v2.11 (2023-02-23)
- Expand type annotation for `pgcopy.copy_to()` to allow DML statements.

### v2.10 (2023-02-06)
- Add `pgcopy.dump()` and `pgcopy.load()`.

### v2.9 (2023-01-05)
- Parse view columns.

### v2.8 (2022-12-19)
- Add `smallint` type.

### v2.7 (2022-09-05)
- Fix custom enum NULL handling.

### v2.6 (2022-09-03)
- Add custom enums.

### v2.5 (2022-08-11)
- Load enum values from Python.
- Add non-native enums.

### v2.4 (2022-05-10)
- Add query capability to `pgcopy.copy_to()`.

### v2.3 (2022-04-19)
- Add `inet` Postgres type.
- Import enum classes directly from schema.

### v2.2 (2022-04-13)
- Remove pragma `table_info()`.
- Use pattern matching for importer.

### v2.1 (2022-04-12)
- Upgrade to Python 3.10.
- Add `pgcopy` utility.
- Add views.
- Add pragma `table_info()`.

### v2.0 (2021-08-10)
- Separate into modules.
- Refactor parsing completely.
- Change `register_enum()` to specify `values_callable` by default,
  making it simpler to call in the usual case.
- Rename `register_package()` to `register_importer()`.
- Create SQLAlchemy metadata by default with Postgres naming convention.
- Remove SQLAlchemy-Utils and GeoAlchemy2 types.
  They can be registered by clients easily.
- Remove `PROCEDURE` call statements and `FUNCTION` call clauses as exports.
  They were almost never used.
- Remove special type adapter for arrays of enums,
  newer versions of SQLAlchemy handle this case without any modifications needed.
- Parse `DEFAULT` value of enum type represented with Python enum correctly.
- Parse `DEFAULT` value expression into a server side default.
- Parse both named and implicit constraints.
- Add `PRAGMA (use_alter)` to foreign key constraints,
  corresponding to the `use_alter` parameter of SQLAlchemy `ForeignKeyConstraint`.
- Add type annotations.
- Add tests for individual features.
- Add pre-commit with Black and Flake8 checks.
- Add Visual Studio Code development container specification.

## Version 1

### v1.22 (2021-06-15)
- Parse all constraints.
- Require SQLAlchemy 1.4.

### v1.21 (2021-01-19)
- Add custom enum types.

### v1.20 (2020-11-27)
- Configure JSON columns with `None` as `NULL`.

### v1.19 (2020-11-13)
- Add procedures.
- Use tokenizer for prepared statements.

### v1.18 (2020-01-21)
- Remove Flask integration.
- Move column alias right after type.

### v1.17 (2019-12-07)
- Add prepared texts.

### v1.16 (2019-12-07)
- Use column alias as key.

### v1.15 (2019-12-06)
- Add custom type registration.
- Add column aliases.

### v1.14 (2019-12-06)
- Add array dimensions, enum arrays.

### v1.13 (2019-12-06)
- Add functions.
- Add import_module.
- Add tests.

### v1.12 (2018-11-05)
- Add `ltree` type.

### v1.11 (2018-09-30)
- Add integer range and fulltext search types.

### v1.10 (2018-09-28)
- Add one-dimensional array types.

### v1.9 (2018-09-28)
- Add PostGIS and XML types.

### v1.8 (2018-06-30)
- Add `numeric` type.

### v1.7 (2018-06-30)
- Add `daterange` type.

### v1.6 (2018-04-23)
- Do not process foreign key references unless necessary.

### v1.5 (2018-04-20)
- Add integer built-in type.

### v1.4 (2018-04-20)
- Allow inline primary key declarations.

### v1.3 (2018-04-10)
- Parse double quoted strings as names.

### v1.2 (2018-04-10)
- Store original SQL text.

### v1.1 (2017-11-08)
- Add `bytea` type.
- Add composite foreign key constraints.

### v1.0 (2017-10-23)
Initial release.
