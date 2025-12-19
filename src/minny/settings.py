import re
from dataclasses import dataclass
from logging import getLogger
from typing import Any, Callable, Dict, List, Optional

from minny import UserError

logger = getLogger(__name__)


@dataclass
class DependenciesTable:
    pip: List[str]
    mip: List[str]
    circup: List[str]


@dataclass
class DeployFilesItem:
    source: str
    destination: str
    include: List[str]
    exclude: List[str]
    compile: List[str]
    no_compile: List[str]


@dataclass
class DeployPackagesItem:
    destination: str
    include: List[str]
    exclude: List[str]
    compile: List[str]
    no_compile: List[str]


@dataclass
class DeployTable:
    current_package_installer: str
    files: List[DeployFilesItem]
    packages: List[DeployPackagesItem]


@dataclass
class MinnySettings:
    dependencies: DependenciesTable
    deploy: DeployTable


class SettingsReader:
    def read_minny_settings(self, context: Any, path: str, context_path: str) -> MinnySettings:
        table = self.read_table(
            context, path, {}, ["dependencies", "deploy"], context_path=context_path
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
        table = self.read_table(
            context, path, {}, ["pip", "mip", "circup"], context_path=context_path
        )
        table_abs_path = self._join_paths(context_path, path)

        return DependenciesTable(
            pip=self.read_string_array(table, "pip", [], context_path=table_abs_path),
            mip=self.read_string_array(table, "mip", [], context_path=table_abs_path),
            circup=self.read_string_array(table, "circup", [], context_path=table_abs_path),
        )

    def read_minny_deploy_table(self, context: Any, path: str, context_path: str) -> DeployTable:
        table = self.read_table(context, path, {}, ["files", "packages"], context_path=context_path)
        table_abs_path = self._join_paths(context_path, path)
        files = self.read_mapped_array(
            table, "files", [{}], self.read_minny_deploy_files_item, context_path=table_abs_path
        )
        packages = self.read_mapped_array(
            table,
            "packages",
            [{}],
            self.read_minny_deploy_packages_item,
            context_path=table_abs_path,
        )

        return DeployTable(
            current_package_installer=self.read_current_package_installer(
                table, "current-package-installer", context_path=table_abs_path
            ),
            files=files,
            packages=packages,
        )

    def read_minny_deploy_files_item(
        self, context: Any, path: str, context_path: str
    ) -> DeployFilesItem:
        table = self.read_table(
            context,
            path,
            {},
            ["source", "destination", "include", "exclude", "compile"],
            context_path=context_path,
        )
        table_abs_path = self._join_paths(context_path, path)

        return DeployFilesItem(
            source=self.read_string(table, "source", "auto", context_path=table_abs_path),
            destination=self.read_string(table, "destination", "auto", context_path=table_abs_path),
            include=self.read_string_array(table, "include", [], context_path=table_abs_path),
            exclude=self.read_string_array(table, "exclude", [], context_path=table_abs_path),
            compile=self.read_string_array(table, "compile", [], context_path=table_abs_path),
            no_compile=self.read_string_array(table, "no-compile", [], context_path=table_abs_path),
        )

    def read_minny_deploy_packages_item(
        self, context: Any, path: str, context_path: str
    ) -> DeployPackagesItem:
        table = self.read_table(
            context,
            path,
            {},
            ["destination", "include", "exclude", "compile"],
            context_path=context_path,
        )
        table_abs_path = self._join_paths(context_path, path)

        return DeployPackagesItem(
            destination=self.read_string(table, "destination", "auto", context_path=table_abs_path),
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
        self, context: Any, path: str, default: Any, allowed_keys: List[str], context_path: str
    ) -> Dict[str, Any]:
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
    ) -> List[Any]:
        arr = self.read_array(context, path, default, context_path=context_path)
        arr_abs_path = self._join_paths(context_path, path)
        result = [item_mapper(arr, f"[{i}]", arr_abs_path) for i in range(len(arr))]
        return result

    def read_string_array(
        self, context: Any, path: str, default: List, context_path: str
    ) -> List[str]:
        return self.read_mapped_array(
            context, path, default, self.read_string_no_default, context_path=context_path
        )

    def read_array(self, context: Any, path: str, default: List, context_path: str) -> List[Any]:
        obj = self.read_setting(context, path, default, context_path)
        obj_abs_path = self._join_paths(context_path, path)
        if obj == default:
            return obj

        if not isinstance(obj, list):
            raise UserError(f"{obj_abs_path} must be an array")

        return obj

    def read_string_no_default(self, context: Any, path: str, context_path: str) -> str:
        return self.read_string(context, path, None, context_path)

    def read_string(
        self, context: Any, path: str, default: Optional[str], context_path: str
    ) -> str:
        obj = self.read_setting(context, path, default, context_path)
        obj_abs_path = self._join_paths(context_path, path)
        if obj is None:
            raise ValueError(f"No string at {obj_abs_path} and no default")

        if obj == default:
            return obj

        if not isinstance(obj, str):
            raise UserError(f"{obj_abs_path} must be a string")

        return obj

    def read_setting(
        self, context: Dict[str, Any] | List[Any], path: str, default: Any, context_path: str
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

    def read_current_package_installer(
        self, context: Dict[str, Any], path: str, context_path: str
    ) -> str:
        result = self.read_string(context, path, "auto", context_path)
        allowed_values = ["pip", "circup", "mip", "none", "auto"]
        if result not in allowed_values:
            raise UserError(
                f"{self._join_paths(context_path, path)} must be one of {', '.join(allowed_values)}"
            )
        return result


def load_minny_settings_from_pyproject_toml(pyproject_toml: Dict[str, Any]) -> MinnySettings:
    return load_minny_settings(pyproject_toml, "tool.minny", context_path="")


def load_minny_settings(context: Dict[str, Any], path: str, context_path: str) -> MinnySettings:
    reader = SettingsReader()
    return reader.read_minny_settings(context, path, context_path=context_path)


def read_setting(context: Dict[str, Any], path: str, default: Any, context_path: str) -> Any:
    reader = SettingsReader()
    return reader.read_setting(context, path, default, context_path)
