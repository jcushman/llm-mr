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
stable enough to be a plugin contract.

**Protocol uses `Path`; builtins extend to `TextIO`.** The I/O protocol's
`open()` method takes a `Path`, which is the narrowest common interface. The
CSV and JSONL built-ins additionally accept `TextIO` to support stdin/stdout
piping. XLSX is file-only (openpyxl requires a path). Format resolution never
routes piped data to XLSX.

## Alternatives Considered

**Reuse llm's global PluginManager.** This would mean llm-mr hooks share a
namespace with llm hooks, risking collisions as both projects evolve. A
separate PM is a small cost for clean isolation.

**Make processors extensible too.** A `register_mr_processors` hook was
prototyped and removed. The three-mode execution pattern is internal and
changing — exposing it as a contract would freeze an unstable API or break
downstream plugins on every change.
