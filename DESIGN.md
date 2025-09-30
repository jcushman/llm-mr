# llm-mr Design Notes

## Vision

`llm-mr` extends the [`llm`](https://llm.datasette.io/) CLI with tools for applying
map/reduce workflows to spreadsheets using language models. It targets analysts and
operations teams who rely on lightweight spreadsheet automation but need the
flexibility of LLM-powered transformations.

## Requirements

- Provide an `llm mr` command group with at least `map` and `reduce` subcommands.
- Operate on CSV files initially, with an upgrade path to other formats (e.g. Excel).
- Use [`uv`](https://docs.astral.sh/uv/latest/) for dependency resolution and
  packaging metadata.
- Produce deterministic CSV output compatible with existing spreadsheet tooling.
- Offer hooks for specifying the LLM model and API key using the conventions of the
  upstream `llm` tool.
- Track design rationale to aid future contributors and agent runs.

## Key Decisions

### Packaging

- Adopt a modern PEP 621 `pyproject.toml` with `hatchling` as the build backend.
- Opt into `uv` packaging by enabling `[tool.uv] package = true` and defining dev
  dependency groups for linting (`ruff`) and testing (`pytest`).

### Command Registration

- Follow the approach used by [`llm-cmd`](https://github.com/simonw/llm-cmd) by
  exposing a `register(cli)` function decorated with `llm.hookimpl`. This keeps the
  plugin compatible with the discovery mechanisms used by `llm`.

### CSV Handling

- Use Python's built-in `csv` module to avoid external dependencies and ease future
  support for different spreadsheet formats. CSV files are loaded fully into memory
  for now; large file streaming can be revisited later.

### Command Surface

- `llm mr map` accepts positional input/output paths plus options for the LLM prompt,
  target column, optional `--where` filters (simple comparison expressions),
  batching configuration, and the smart deterministic helper.
- `llm mr reduce` accepts the CSV input, one or more `--group-by` columns, optional
  `--where` filters, and the LLM prompt that should be applied to each group. Output
  is a two-column CSV containing the group key and the reduced value.

### Prompt Construction

- Map operations will format each row (or batch of rows) as JSON to reduce ambiguity
  in LLM prompts. Few-shot examples reuse existing, non-empty values in the target
  column to ground the responses.
- Reduce operations reuse the same JSON formatting and add recursive summarization
  when the prompt size would exceed a configurable threshold.

### Deterministic Short-Circuiting (Stretch Goal)

- Provide an optional "smart" model hook that attempts to synthesize a Python
  expression implementing the requested transformation. When available, the user is
  prompted to confirm executing the deterministic function instead of issuing LLM
  requests for every row.
- Expressions run in a restricted evaluation context to limit security risks.

## Open Questions

- How should we detect prompt size relative to actual model token limits? We
  currently rely on a simple character threshold, which may need refinement.
- The grouping mini-language for `reduce` currently supports comma-separated
  equality/inequality expressions and column names. We may expand this to a more
  capable parser or leverage an existing library.
- Future work includes Excel I/O, caching of LLM responses, and richer prompt
  templating (e.g., Jinja2-style templates).

## Next Steps

- Implement the CLI commands with robust error handling and logging.
- Add automated tests covering map batching, reduce recursion, and deterministic
  fallbacks.
- Document usage examples in the README once basic functionality is stable.
