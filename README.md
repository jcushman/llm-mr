# llm-mr

`llm-mr` is a plugin for the [`llm`](https://llm.datasette.io/) command line tool
that adds map/reduce style helpers for spreadsheet files. The plugin currently
supports CSV input/output and relies on the upstream LLM registry for model
selection.

## Installation

```bash
uv tool install llm-mr
```

Ensure that the [`llm` CLI](https://llm.datasette.io/) is installed and
configured with your preferred models and API keys.

## Usage

### Map

```
llm mr map data.csv -p "Return a short summary of the notes" --column summary -o output.csv
```

Options include:

- `--in-place` to update the original file.
- `--where` for simple filters like `status=active` or `score>=10`.
- `--few-shot` to provide the first N populated rows as context.
- `--batch-size` and `--max-chars` to control batching when sending rows to the
  model.
- `--smart-model` to attempt compiling the prompt into a deterministic Python
  expression before falling back to LLM calls.

### Reduce

```
llm mr reduce data.csv --group-by department --group-by quarter -p "Summarize performance" -o summary.csv
```

The reduce command groups rows by the provided columns, optionally filters rows
with `--where`, and uses the supplied prompt to reduce each group into a single
value. Large groups are recursively reduced to stay within the prompt size
limit.

## Development

This project is managed with [`uv`](https://docs.astral.sh/uv/latest/). Common
commands:

```bash
uv sync               # install dependencies
uv run pytest         # run tests (once added)
uv run ruff format    # format the code
```

See `DESIGN.md` for architectural notes and planned enhancements.
