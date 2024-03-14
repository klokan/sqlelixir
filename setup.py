from setuptools import setup

setup(
    name="SQLElixir",
    version="2.16",
    description="SQL files as Python modules",
    packages=["sqlelixir"],
    install_requires=[
        "sqlalchemy>=1.4.0",
        "sqlparse>=0.4.1",
    ],
)
