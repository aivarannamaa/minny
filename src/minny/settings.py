import re
from collections.abc import Callable
from dataclasses import dataclass
from logging import getLogger
from typing import Any, Literal

from minny.common import UserError

logger = getLogger(__name__)

INSTALLER_NAMES = ("pip", "mip", "circup")
DEFAULT_NO_DELETE_PATTERNS = [
    "/sd",
    "/rom",
    "/ram",
    "/boot.py",
    "/boot.txt",
    "/flash/boot.py",
    "/safemode.py",
    "/safemode.txt",
    "/repl.py",
    "/flash/SKIPSD",
    "/settings.toml",
    "/webrepl_cfg.py",
    "/flash/webrepl_cfg.py",
    "/boot_out.txt",
    "/.*",
    "/flash/.*",
]
CompileSetting = Literal["auto"] | list[str]


@dataclass
class DependenciesTable:
    pip: list[str]
    mip: list[str]
    circup: list[str]


@dataclass
class DeployFilesItem:
    source_dir: str
    target_dir: str
    include: list[str]
    exclude: list[str]
    compile: CompileSetting
    no_compile: list[str]


@dataclass
class DeployPackagesItem:
    target_dir: str
    include: list[str]
    exclude: list[str]
    compile: list[str]
    no_compile: list[str]


@dataclass
class DeployTable:
    files: list[DeployFilesItem]
    packages: list[DeployPackagesItem]
    no_delete: list[str]


@dataclass
class MinnySettings:
    dependencies: DependenciesTable
    deploy: DeployTable


class SettingsReader:
    def read_minny_settings(self, context: Any, path: str, context_path: str) -> MinnySettings:
        table = self.read_table(
            context,
            path,
            {},
            ["dependencies", "deploy"],
            context_path=context_path,
        )
        table_abs_path = self._join_paths(context_path, path)
        return MinnySettings(
            dependencies=self.read_minny_dependencies_table(
                table, "dependencies", context_path=table_abs_path
            ),
            deploy=self.read_minny_deploy_table(table, "deploy", context_path=table_abs_path),
        )

    def read_minny_dependencies_table(
        self, context: Any, path: str, context_path: str
    ) -> DependenciesTable:
        table = self.read_table(context, path, {}, list(INSTALLER_NAMES), context_path=context_path)
        table_abs_path = self._join_paths(context_path, path)

        return DependenciesTable(
            pip=self.read_string_array(table, "pip", [], context_path=table_abs_path),
            mip=self.read_string_array(table, "mip", [], context_path=table_abs_path),
            circup=self.read_string_array(table, "circup", [], context_path=table_abs_path),
        )

    def read_minny_deploy_table(self, context: Any, path: str, context_path: str) -> DeployTable:
        table = self.read_table(
            context,
            path,
            {},
            ["files", "packages", "no-delete"],
            context_path=context_path,
        )
        table_abs_path = self._join_paths(context_path, path)
        files = self.read_mapped_array(
            table, "files", [], self.read_minny_deploy_files_item, context_path=table_abs_path
        )
        packages = self.read_mapped_array(
            table,
            "packages",
            [{}],
            self.read_minny_deploy_packages_item,
            context_path=table_abs_path,
        )

        return DeployTable(
            files=files,
            packages=packages,
            no_delete=self.read_string_array(
                table,
                "no-delete",
                DEFAULT_NO_DELETE_PATTERNS.copy(),
                context_path=table_abs_path,
            ),
        )

    def read_minny_deploy_files_item(
        self, context: Any, path: str, context_path: str
    ) -> DeployFilesItem:
        table = self.read_table(
            context,
            path,
            {},
            ["source-dir", "target-dir", "include", "exclude", "compile", "no-compile"],
            context_path=context_path,
        )
        table_abs_path = self._join_paths(context_path, path)

        return DeployFilesItem(
            source_dir=self.read_string(table, "source-dir", ".", context_path=table_abs_path),
            target_dir=self.read_string(table, "target-dir", "auto", context_path=table_abs_path),
            include=self.read_string_array(table, "include", [], context_path=table_abs_path),
            exclude=self.read_string_array(table, "exclude", [], context_path=table_abs_path),
            compile=self.read_compile_setting(
                table, "compile", "auto", context_path=table_abs_path
            ),
            no_compile=self.read_string_array(table, "no-compile", [], context_path=table_abs_path),
        )

    def read_minny_deploy_packages_item(
        self, context: Any, path: str, context_path: str
    ) -> DeployPackagesItem:
        table = self.read_table(
            context,
            path,
            {},
            ["target-dir", "include", "exclude", "compile", "no-compile"],
            context_path=context_path,
        )
        table_abs_path = self._join_paths(context_path, path)

        return DeployPackagesItem(
            target_dir=self.read_string(table, "target-dir", "auto", context_path=table_abs_path),
            include=self.read_mapped_array(
                table,
                "include",
                ["auto"],
                self.read_string_no_default,
                context_path=table_abs_path,
            ),
            exclude=self.read_mapped_array(
                table,
                "exclude",
                [],
                self.read_string_no_default,
                context_path=table_abs_path,
            ),
            compile=self.read_mapped_array(
                table,
                "compile",
                ["*"],
                self.read_string_no_default,
                context_path=table_abs_path,
            ),
            no_compile=self.read_mapped_array(
                table,
                "no-compile",
                [],
                self.read_string_no_default,
                context_path=table_abs_path,
            ),
        )

    def read_table(
        self, context: Any, path: str, default: Any, allowed_keys: list[str], context_path: str
    ) -> dict[str, Any]:
        obj = self.read_setting(context, path, default, context_path)
        obj_abs_path = self._join_paths(context_path, path)
        if obj == default:
            return obj

        if not isinstance(obj, dict):
            raise UserError(f"{obj_abs_path} must be a table")

        unknown_keys = []
        for key in obj:
            if key not in allowed_keys:
                unknown_keys.append(key)
        if unknown_keys:
            raise UserError(
                f"{obj_abs_path} contains unknown keys: {unknown_keys}. Allowed keys are: {allowed_keys}"
            )

        return obj

    def read_mapped_array(
        self,
        context: Any,
        path: str,
        default: Any,
        item_mapper: Callable[[Any, str, str], Any],
        context_path: str,
    ) -> list[Any]:
        arr = self.read_array(context, path, default, context_path=context_path)
        arr_abs_path = self._join_paths(context_path, path)
        result = [item_mapper(arr, f"[{i}]", arr_abs_path) for i in range(len(arr))]
        return result

    def read_string_array(
        self, context: Any, path: str, default: list, context_path: str
    ) -> list[str]:
        return self.read_mapped_array(
            context, path, default, self.read_string_no_default, context_path=context_path
        )

    def read_compile_setting(
        self,
        context: Any,
        path: str,
        default: CompileSetting,
        context_path: str,
    ) -> CompileSetting:
        obj = self.read_setting(context, path, default, context_path)
        obj_abs_path = self._join_paths(context_path, path)
        if obj == "auto":
            return "auto"
        if not isinstance(obj, list):
            raise UserError(f"{obj_abs_path} must be 'auto' or an array of strings")
        patterns = [
            self.read_string_no_default(obj, f"[{i}]", obj_abs_path) for i in range(len(obj))
        ]
        if "auto" in patterns:
            raise UserError(f"{obj_abs_path} must use scalar 'auto', not include it as a pattern")
        return patterns

    def read_array(self, context: Any, path: str, default: list, context_path: str) -> list[Any]:
        obj = self.read_setting(context, path, default, context_path)
        obj_abs_path = self._join_paths(context_path, path)
        if obj == default:
            return obj

        if not isinstance(obj, list):
            raise UserError(f"{obj_abs_path} must be an array")

        return obj

    def read_string_no_default(self, context: Any, path: str, context_path: str) -> str:
        return self.read_string(context, path, None, context_path)

    def read_string(self, context: Any, path: str, default: str | None, context_path: str) -> str:
        obj = self.read_setting(context, path, default, context_path)
        obj_abs_path = self._join_paths(context_path, path)
        if obj is None:
            raise ValueError(f"No string at {obj_abs_path} and no default")

        if obj == default:
            return obj

        if not isinstance(obj, str):
            raise UserError(f"{obj_abs_path} must be a string")

        return obj

    def read_bool(self, context: Any, path: str, default: bool, context_path: str) -> bool:
        obj = self.read_setting(context, path, default, context_path)
        obj_abs_path = self._join_paths(context_path, path)
        if not isinstance(obj, bool):
            raise UserError(f"{obj_abs_path} must be a boolean")

        return obj

    def read_setting(
        self, context: dict[str, Any] | list[Any], path: str, default: Any, context_path: str
    ) -> Any:
        sections = path.split(".")
        section_pattern = re.compile(r"^([A-Za-z-]+)?(?:\[(\d+)])?$")

        full_path = context_path
        while sections:
            head = sections.pop(0)
            if full_path:
                full_path += "."
            full_path += head

            m = section_pattern.match(head)
            if not m:
                raise ValueError(
                    f"Unsupported setting {head!r} ({full_path}); Context: {context}; Default: {default} "
                )

            name, index = m.groups()

            if name is not None:
                assert isinstance(context, dict)
                if name in context:
                    context = context[name]
                else:
                    return default

            if index is not None:
                assert isinstance(context, list)
                context = context[int(index)]

        return context

    def _join_paths(self, context_path: str, path: str) -> str:
        if path.startswith("["):
            return context_path + path
        elif not context_path:
            return path
        elif not path:
            return context_path
        else:
            return context_path + "." + path


def load_minny_settings_from_pyproject_toml(pyproject_toml: dict[str, Any]) -> MinnySettings:
    return load_minny_settings(pyproject_toml, "tool.minny", context_path="")


def load_minny_settings(context: dict[str, Any], path: str, context_path: str) -> MinnySettings:
    reader = SettingsReader()
    return reader.read_minny_settings(context, path, context_path=context_path)


def read_setting(context: dict[str, Any], path: str, default: Any, context_path: str) -> Any:
    reader = SettingsReader()
    return reader.read_setting(context, path, default, context_path)
