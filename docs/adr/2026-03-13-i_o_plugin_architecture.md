# I/O Plugin Architecture

**Date:** 2026-03-13
**Status:** Accepted

## Context

`llm-mr` needs to read and write multiple tabular formats (CSV, JSONL, XLSX)
and allow third parties to add more (e.g. Parquet). The `llm` tool already has
a global pluggy `PluginManager` for its own hooks. We need extension points
for I/O without interfering with llm's plugin namespace, and need to decide
whether processors (map/reduce/filter) should also be extensible.

## Decision

**Separate PluginManager.** I/O plugins register via a dedicated
`PluginManager("llm_mr")` under the `llm_mr` entry-point group, not llm's
global `pm`. This avoids namespace collisions — hooks like
`register_mr_inputs` won't conflict with anything in the llm ecosystem.

**I/O is extensible; processors are not.** CSV, JSONL, and XLSX are built-in.
Third-party packages (e.g. `llm-mr-parquet`) register via entry points and
implement `register_mr_inputs` / `register_mr_outputs` hooks. The three
processors (map, reduce, filter) are *not* exposed as extension points — the
three-mode (`-e`/`-p`/interactive) execution pattern is still evolving and not
stable enough to be a plugin contract, and it's not clear what new commands
others would want to add.

**Protocol uses `Path`; streaming is opt-in.** The base `InputPlugin` and
`OutputPlugin` protocols take a `Path` — the narrowest common interface and
the only contract third-party plugins must satisfy. Optional `StreamableInput`
and `StreamableOutput` runtime-checkable protocols add `open_stream(IO[str])`
and `write_stream(IO[str], ...)` for plugins that can read/write text streams
directly. The CSV and JSONL built-ins implement both layers; XLSX is file-only
(openpyxl requires a path). When piping stdin/stdout to a plugin that doesn't
support streaming, the harness transparently spools through a temp file.

## Alternatives Considered

**Reuse llm's global PluginManager.** This would mean llm-mr hooks share a
namespace with llm hooks, risking collisions as both projects evolve. A
separate PM is a small cost for clean isolation.

**Make processors extensible too.** A `register_mr_processors` hook was
prototyped and removed. The three-mode execution pattern is internal and
changing — exposing it as a contract would freeze an unstable API or break
downstream plugins on every change.

**`Union[Path, TextIO]` in the base protocol.** An earlier design had the base
`open()` accept `Union[Path, IO[str]]`, with builtins widening their signatures
and XLSX only supporting `Path`. This made the protocol dishonest — plugins
implemented `open(Path)` but the harness could pass a stream, causing runtime
crashes. The separate streamable protocols make the capability explicit and
type-safe.
