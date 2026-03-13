from __future__ import annotations

import llm

from .hookspecs import load_mr_plugins, mr_pm
from .io_plugins import register_builtin_io
from .processors import MapProcessor, ReduceProcessor, FilterProcessor
from .registries import InputRegistry, OutputRegistry, PluginContext


@llm.hookimpl
def register_commands(cli):  # pragma: no cover - exercised via llm runtime
    load_mr_plugins()

    inputs = InputRegistry()
    outputs = OutputRegistry()
    context = PluginContext(inputs=inputs, outputs=outputs)

    register_builtin_io(inputs, outputs)

    mr_pm.hook.register_mr_inputs(register=inputs.register)
    mr_pm.hook.register_mr_outputs(register=outputs.register)

    @cli.group()
    def mr():
        """Map-reduce helpers for spreadsheets."""

    for processor in [MapProcessor(), ReduceProcessor(), FilterProcessor()]:
        processor.register_cli(mr, context)
