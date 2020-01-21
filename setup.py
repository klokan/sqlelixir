from setuptools import setup

setup(
    name='SQLElixir',
    version='1.18',
    description='SQL files as Python modules',
    packages=['sqlelixir'],
    install_requires=[
        'sqlalchemy>=1.1.0',
        'sqlparse>=0.2',
    ])
