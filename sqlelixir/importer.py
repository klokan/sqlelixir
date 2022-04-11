import os.path

from collections.abc import Iterable
from importlib.abc import MetaPathFinder, Loader
from importlib.machinery import ModuleSpec
from types import ModuleType

from sqlelixir.parser import Parser


class Importer(MetaPathFinder, Loader):
    def __init__(self, parser: Parser, package: str):
        self.parser = parser
        self.package_parts = package.split(".")

    def find_spec(
        self, fullname: str, path: Iterable[str] | None, target=None
    ) -> ModuleSpec | None:
        if path is None:
            return None

        name_parts = fullname.split(".")
        name = name_parts.pop()

        if len(name_parts) < len(self.package_parts):
            return None

        for name_part, package_part in zip(name_parts, self.package_parts):
            if name_part != package_part:
                return None

        filename = name + ".sql"
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
