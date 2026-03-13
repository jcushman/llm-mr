from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import (
    Any,
    ContextManager,
    Dict,
    Iterable,
    List,
    Optional,
    Protocol,
    Sequence,
)

import click

Row = Dict[str, Any]


@dataclass
class TableStream:
    rows: Iterable[Row]
    fieldnames: Optional[Sequence[str]] = None


class InputPlugin(Protocol):
    name: str
    extensions: Sequence[str]

    def open(self, path: Path) -> ContextManager[TableStream]: ...


class OutputPlugin(Protocol):
    name: str
    extensions: Sequence[str]

    def write(
        self, path: Path, rows: Iterable[Row], fieldnames: Sequence[str]
    ) -> None: ...


class InputRegistry:
    def __init__(self) -> None:
        self._plugins: Dict[str, InputPlugin] = {}
        self._by_name: Dict[str, InputPlugin] = {}

    def register(self, plugin: InputPlugin) -> None:
        for ext in plugin.extensions:
            key = normalize_extension(ext)
            if key in self._plugins:
                raise ValueError(
                    f"Input plugin already registered for extension '.{key}'"
                )
            self._plugins[key] = plugin
        self._by_name[plugin.name] = plugin

    def for_path(self, path: Path) -> InputPlugin:
        key = normalize_extension(path.suffix)
        plugin = self._plugins.get(key)
        if plugin is None:
            suffix = path.suffix or "<none>"
            raise click.ClickException(
                f"No input plugin registered for file extension '{suffix}'"
            )
        return plugin

    def for_name(self, name: str) -> InputPlugin:
        plugin = self._by_name.get(name)
        if plugin is None:
            raise click.ClickException(
                f"No input plugin registered with name '{name}'"
            )
        return plugin

    def values(self) -> List[InputPlugin]:
        return list(self._plugins.values())


class OutputRegistry:
    def __init__(self) -> None:
        self._plugins: Dict[str, OutputPlugin] = {}
        self._by_name: Dict[str, OutputPlugin] = {}

    def register(self, plugin: OutputPlugin) -> None:
        for ext in plugin.extensions:
            key = normalize_extension(ext)
            if key in self._plugins:
                raise ValueError(
                    f"Output plugin already registered for extension '.{key}'"
                )
            self._plugins[key] = plugin
        self._by_name[plugin.name] = plugin

    def for_path(self, path: Path) -> OutputPlugin:
        key = normalize_extension(path.suffix)
        plugin = self._plugins.get(key)
        if plugin is None:
            suffix = path.suffix or "<none>"
            raise click.ClickException(
                f"No output plugin registered for file extension '{suffix}'"
            )
        return plugin

    def for_name(self, name: str) -> OutputPlugin:
        plugin = self._by_name.get(name)
        if plugin is None:
            raise click.ClickException(
                f"No output plugin registered with name '{name}'"
            )
        return plugin

    def values(self) -> List[OutputPlugin]:
        return list(self._plugins.values())


@dataclass
class PluginContext:
    inputs: InputRegistry
    outputs: OutputRegistry


def normalize_extension(ext: str) -> str:
    ext = ext.lower().strip()
    if ext.startswith("."):
        ext = ext[1:]
    return ext
