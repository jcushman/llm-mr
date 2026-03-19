"""Hook specifications and plugin manager for llm-mr extensions.

Third-party packages (e.g. llm-mr-parquet) register under the ``llm_mr``
entry-point group and use :data:`mr_hookimpl` to mark their implementations::

    # pyproject.toml
    [project.entry-points.llm_mr]
    parquet = "llm_mr_parquet"

    # llm_mr_parquet/__init__.py
    from llm_mr.hookspecs import mr_hookimpl

    @mr_hookimpl
    def register_mr_inputs(register):
        register(ParquetInputPlugin())

Plugin classes must satisfy the ``InputPlugin`` / ``OutputPlugin`` protocols
(``open(path)`` and ``write(path, ...)``).  For stdin/stdout streaming support,
also implement ``StreamableInput`` (``open_stream(stream)``) and/or
``StreamableOutput`` (``write_stream(stream, ...)``).  The harness falls back
to a temp file for plugins that don't support streaming.
"""

from __future__ import annotations

import pluggy

mr_hookspec = pluggy.HookspecMarker("llm_mr")
mr_hookimpl = pluggy.HookimplMarker("llm_mr")


class MrHookSpecs:
    @mr_hookspec
    def register_mr_inputs(self, register):
        """Register additional input plugins (readers)."""

    @mr_hookspec
    def register_mr_outputs(self, register):
        """Register additional output plugins (writers)."""


mr_pm = pluggy.PluginManager("llm_mr")
mr_pm.add_hookspecs(MrHookSpecs)

_loaded = False


def load_mr_plugins() -> None:
    """Discover and load third-party llm-mr plugins via entry points."""
    global _loaded
    if _loaded:
        return
    _loaded = True
    mr_pm.load_setuptools_entrypoints("llm_mr")
