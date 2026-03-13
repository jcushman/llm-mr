# llm-mr Design Notes

## Vision

`llm-mr` extends the [`llm`](https://llm.datasette.io/) CLI with tools for applying
map/reduce/filter workflows to spreadsheets using language models. It targets analysts
and operations teams who rely on lightweight spreadsheet automation but need the
flexibility of LLM-powered transformations.

## Requirements

- Provide an `llm mr` command group with `map`, `reduce`, and `filter` subcommands.
- Operate on CSV, JSONL, and XLSX files via a pluggable I/O system.
- Use [`uv`](https://docs.astral.sh/uv/latest/) for dependency resolution and
  packaging metadata.
- Produce deterministic output compatible with existing spreadsheet tooling.
- Offer hooks for specifying the LLM model and API key using the conventions of the
  upstream `llm` tool.
- Track design rationale to aid future contributors and agent runs.

## Key Decisions

### Three Execution Modes

Every command (map, reduce, filter) accepts a second positional argument — the
**instruction** — and interprets it according to one of three modes:

| Flag | Mode | Meaning |
|------|------|---------|
| *(default)* | Interactive | Tool asks the LLM to synthesize a deterministic expression, shows it for confirmation, falls back to per-item LLM if declined or not possible. |
| `-p` | Prompt | Treat the instruction as a literal LLM prompt. Run it per-item (map/filter) or per-group (reduce). Predictable, no magic. |
| `-e` | Expression | Treat the instruction as a Python expression. Run deterministically with no LLM calls. |

This replaces the old `--smart-model` flag. The interactive mode is the default because
it matches how users discover tools — start exploratory, then graduate to `-p` or `-e`
once you know what works.

### Two Model Roles

- **`-m` / `--model`** — the primary model, used for planning (interactive mode) and
  as the default for per-item work.
- **`--worker-model`** — optional cheaper/faster model for per-item LLM work. When
  provided, `-m` handles the one-shot planning step and `--worker-model` handles the
  N per-item calls.

This keeps the common case simple (`-m` for everything) while letting power users
optimize cost.

### Packaging

- Adopt a modern PEP 621 `pyproject.toml` with `hatchling` as the build backend.
- Opt into `uv` packaging by enabling `[tool.uv] package = true` and defining dev
  dependency groups for linting (`ruff`) and testing (`pytest`).

### Command Registration

- Follow the approach used by [`llm-cmd`](https://github.com/simonw/llm-cmd) by
  exposing a `register(cli)` function decorated with `llm.hookimpl`. This keeps the
  plugin compatible with the discovery mechanisms used by `llm`.

### I/O Plugin Architecture

- CSV, JSONL, and XLSX are supported via pluggable input/output registries.
- Files are loaded fully into memory for now; large file streaming can be revisited
  later.
- Third-party I/O plugins (e.g. `llm-mr-parquet`) register via a **separate pluggy
  PluginManager** (`llm_mr.hookspecs.mr_pm`) under the `llm_mr` entry-point group.
  This avoids namespace collisions with llm's global plugin manager.
- The three built-in processors (map, reduce, filter) are registered directly —
  there is no processor extension hook. The three-mode (`-p`/`-e`/interactive)
  pattern is still evolving and is not yet stable enough to be a plugin contract.

### Prompt Construction

- Map operations format each row (or batch of rows) as JSON to reduce ambiguity
  in LLM prompts. Few-shot examples reuse existing, non-empty values in the target
  column to ground the responses.
- Reduce operations reuse the same JSON formatting and add recursive summarization
  when the prompt size would exceed a configurable threshold.
- Filter operations ask the LLM to classify each row as "keep" or "discard" using
  structured output.

### Deterministic Expressions

- In expression mode (`-e`), Python expressions run in a restricted evaluation
  context with safe builtins (len, int, str, etc.) but no arbitrary module access.
- Map/filter expressions use the variable `row` (a dict).
- Reduce expressions use the variable `rows` (a list of dicts) with additional
  builtins like `sum`, `any`, `all`.
- Security relies on the user reviewing the expression (in interactive mode, a
  confirmation prompt is shown). The restricted `eval` context limits accidental
  damage but is not a true sandbox.

### `--where` Pre-filters

The `--where` flag remains as a simple pre-filter on all three commands. It uses
fixed comparison expressions (`status=active`, `score>=10`) and runs before the
instruction is applied. This is useful for narrowing data before an expensive
LLM operation.

## Open Questions

- How should we detect prompt size relative to actual model token limits? We
  currently rely on a simple character threshold, which may need refinement.
- Future work includes caching of LLM responses and richer prompt templating.
- Should `llm` itself grow a concept of "default fast model" for plan/execute
  patterns? This would benefit multiple plugins.
