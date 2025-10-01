from __future__ import annotations

import click

from llm.plugins import hookspecs, pm

from .io_plugins import register_builtin_io
from .processors import register_builtin_processors
from .registries import InputRegistry, OutputRegistry, PluginContext, ProcessorRegistry


class SheetMapHookSpecs:
    @hookspecs.hookspec
    def register_mr_inputs(self, register):
        """Register additional SheetMap input plugins."""

    @hookspecs.hookspec
    def register_mr_outputs(self, register):
        """Register additional SheetMap output plugins."""

    @hookspecs.hookspec
    def register_mr_processors(self, register):
        """Register additional SheetMap processors."""


_hooks_registered = False


def _ensure_hooks_registered() -> None:
    global _hooks_registered
    if _hooks_registered:
        return
    pm.add_hookspecs(SheetMapHookSpecs)
    _hooks_registered = True


def register(cli):  # pragma: no cover - exercised via llm runtime
    _ensure_hooks_registered()

    inputs = InputRegistry()
    outputs = OutputRegistry()
    processors = ProcessorRegistry()
    context = PluginContext(inputs=inputs, outputs=outputs, processors=processors)

    register_builtin_io(inputs, outputs)
    register_builtin_processors(processors)

    pm.hook.register_mr_inputs(register=inputs.register)
    pm.hook.register_mr_outputs(register=outputs.register)
    pm.hook.register_mr_processors(register=processors.register)

    @cli.group()
    def mr():
        """Map-reduce helpers for spreadsheets."""

    for processor in processors.values():
        processor.register_cli(mr, context)
