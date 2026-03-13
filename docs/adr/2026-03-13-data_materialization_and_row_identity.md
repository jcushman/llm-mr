# Data Materialization and Row Identity

**Date:** 2026-03-13
**Status:** Accepted (amended)

## Context

Processors need to read input, apply transformations (possibly to a filtered
subset), and write output — including expanding rows when `--multiple` is used.
The design must track which rows were actually processed, especially when
`--where` filters to a subset or `--multiple` expands results.

## Decision

**Hybrid materialization.** Two execution paths coexist depending on mode:

- **Expression mode (`-e`) for map and filter:** Rows are streamed one at a
  time via `_stream_map_expression` / `_stream_filter_expression`. No full
  materialization occurs — each row is read, processed, and written
  immediately. This allows processing files larger than memory. Exception:
  `--in-place` map still materializes (cannot read and write the same file
  concurrently).

- **LLM / interactive modes and reduce:** All rows are loaded via
  `_materialize()` before processing begins. This is necessary for batching,
  parallel LLM calls, error tracking via `.err` sidecars, interactive
  planning (which needs sample rows), and reduce grouping.

**Early limit.** `_materialize()` accepts an optional `limit` parameter and
stops reading after N rows. This means `--limit 10` on a 5GB file reads only
10 rows regardless of mode.

**In-place mutation of row dicts** (materialized path). Processors write
results directly into row dicts (`row[target_column] = value`). The output
writer then serializes the same dicts.

**Row identity via `id(row)`** (materialized path). When `--where` filters
rows, `target_rows` is a subset of `rows`. The code uses `id(row)` to map
between the filtered view and the original list. The streaming path avoids
this by processing rows sequentially with inline `--where` checks.

**`processed_indices` for `--multiple` expansion** (materialized path).
`_expand_multiple_rows` needs to know which rows were processed to avoid
expanding pre-existing list values. The streaming path handles `--multiple`
expansion inline as each row is processed.

## Alternatives Considered

**Full streaming for all modes.** Would reduce memory in LLM mode too, but
complicates batching, parallel execution, error recovery (`.err` files store
row indices), and interactive planning. Since the LLM is the bottleneck in
prompt mode (not memory), the benefit is marginal.

**Copy-on-write rows.** Safer than in-place mutation, but adds overhead and
complexity for no current benefit — rows are processed once and written once.
