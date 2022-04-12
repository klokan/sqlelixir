import fnmatch
import os.path
import re

from collections.abc import Iterable
from importlib.abc import MetaPathFinder, Loader
from importlib.machinery import ModuleSpec
from types import ModuleType

from sqlelixir.parser import Parser


class Importer(MetaPathFinder, Loader):
    def __init__(self, parser: Parser, patterns: list[str]):
        assert patterns
        self.parser = parser
        self.patterns = [re.compile(fnmatch.translate(pattern)) for pattern in patterns]

    def find_spec(
        self, fullname: str, path: Iterable[str] | None, target=None
    ) -> ModuleSpec | None:
        if path is None:
            return None

        for pattern in self.patterns:
            if pattern.fullmatch(fullname) is not None:
                break
        else:
            return None

        __, __, name = fullname.rpartition(".")
        filename = f"{name}.sql"

        for entry in path:
            full_path = os.path.join(entry, filename)
            if os.path.exists(full_path):
                return ModuleSpec(name=fullname, loader=self, origin=full_path)

        return None

    def create_module(self, spec: ModuleSpec) -> None:
        return None

    def exec_module(self, module: ModuleType):
        assert module.__spec__ is not None
        assert module.__spec__.origin is not None

        with open(module.__spec__.origin, "r", encoding="utf-8") as fp:
            self.parser.parse(fp, module)
