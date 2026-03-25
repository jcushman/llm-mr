# llm-mr

`llm-mr` is a plugin for the [`llm`](https://llm.datasette.io/) command line tool
that adds map/reduce/filter helpers for spreadsheet files. Use it when you have a large
file you want to process with an LLM, and need to break it down into smaller chunks
rather than processing the entire file at once.

The plugin supports CSV, JSONL, and XLSX input/output, and can be extended with third-party plugins
for other formats.

## Examples

Classify every row in a spreadsheet by sentiment:

```bash
llm mr map "Classify sentiment as positive/negative/neutral" -p -i feedback.csv -c sentiment -o out.csv
```

Summarize notes per department:

```bash
llm mr reduce "Summarize key themes" -p -i employees.csv --group-by department -o summary.csv
```

Filter a JSONL corpus to just the articles about a topic:

```bash
llm mr filter "about climate policy" -p -i articles.jsonl -o climate.jsonl
```

Pipe data through stdin/stdout — JSONL is the default streaming format:

```bash
cat data.jsonl | llm mr filter "about climate" -p | llm mr map "summarize" -c summary -p > out.jsonl
```

Expand each row into multiple output rows:

```bash
llm mr map "List the five largest cities" -p -i countries.csv --multiple -c city -o cities.csv
```

Bulk-rename columns with a Python expression — no LLM needed:

```bash
llm mr map 'row["name"].upper()' -e -i data.csv -c name_upper -o clean.csv
```

Or just describe what you want — interactive mode synthesizes the expression for you:

```bash
$ llm mr map "uppercase the names" -i data.csv -c name_upper -o clean.csv
Use deterministic expression?
  row["name"].upper() [Y/n]: Y
Using deterministic expression: row["name"].upper()
```

## Installation

If you already have [`llm`](https://llm.datasette.io/) installed:

```bash
llm install llm-mr
```

New to `llm`? It's a command-line tool for interacting with language models.
Install both together:

```bash
pip install llm llm-mr
```

Then [configure a model and API key](https://llm.datasette.io/en/stable/setup.html)
before continuing.

## Writing I/O format plugins

Extra tabular formats (beyond CSV, JSONL, and XLSX) ship as normal Python packages that
depend on `llm-mr` and register **input** and/or **output** plugins with [pluggy](https://pluggy.readthedocs.io/)
via the `llm_mr` entry-point group.

Declare the entry point in `pyproject.toml` (the value is an importable module that is
loaded for side effects; a package `__init__.py` works well):

```toml
[project.entry-points.llm_mr]
myformat = "llm_mr_myformat"
```

In that module, use `mr_hookimpl` from `llm_mr.hookspecs` and implement
`register_mr_inputs` and/or `register_mr_outputs`. Each receives a `register` callback;
pass an instance of your plugin class.

Plugins must satisfy the `InputPlugin` and/or `OutputPlugin` protocols in
`llm_mr.registries`:

- **Input:** `name` (string id, e.g. `"parquet"`), `extensions` (e.g. `[".parquet"]`),
  and `open(self, path: Path)` as a context manager yielding a `TableStream` (`rows`
  iterable and optional `fieldnames`).
- **Output:** same `name` / `extensions`, plus `write(self, path: Path, rows, fieldnames)`
  that writes the file.

This is all that's required — stdin/stdout piping works automatically via a
temp-file intermediary. For streaming without a temp file, also implement
`StreamableInput` (`open_stream(self, stream)`) and/or `StreamableOutput`
(`write_stream(self, stream, rows, fieldnames)`).

```python
# llm_mr_myformat/__init__.py
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from llm_mr.hookspecs import mr_hookimpl
from llm_mr.registries import TableStream


class ParquetInputPlugin:
    name = "parquet"
    extensions = [".parquet"]

    @contextmanager
    def open(self, path: Path) -> Iterator[TableStream]:
        rows = ...  # load rows as Iterable[Row]
        yield TableStream(rows=rows, fieldnames=[...])


@mr_hookimpl
def register_mr_inputs(register):
    register(ParquetInputPlugin())
```

After installation, `llm mr` discovers plugins through the same entry-point loading as
the main `llm` tool; users install your package with `pip` / `llm install` like any other
dependency.

## Tutorial: Getting Started

### Step 1: Install

Follow the [Installation](#installation) instructions above, then make sure you
have a model configured. If you already use `llm` with an OpenAI or Anthropic
key, you're all set — skip to Step 2.

If you'd rather use a free local model, [Ollama](https://ollama.com/) is the
quickest path:

```bash
llm install llm-ollama
ollama pull llama3.2:1b
llm -m llama3.2:1b "Hello, world!"
```

### Step 2: Create Sample Data

Create `foods.csv`:

```csv
food,description
pizza,"Cheesy flatbread with tomato sauce and various toppings"
broccoli,"Green cruciferous vegetable, often steamed or roasted"
chocolate,"Sweet confection made from cocoa beans"
kale,"Dark leafy green vegetable, often used in salads"
ice_cream,"Frozen dairy dessert, sweet and creamy"
```

### Step 3: Map

The `map` command adds a new column to the input file.
Here we make a separate LLM request for each row to classify the food as a 'treat' or 'not treat'.
The `-p` flag means "use my instruction as a literal prompt" — the text you provide is sent
directly to the model for each row.

```bash
llm mr map \
  "Based on the food description, classify this as 'treat' or 'not treat'" \
  -p -i foods.csv -c tastiness -o foods_classified.csv
```

```csv
food,description,tastiness
pizza,"Cheesy flatbread with tomato sauce and various toppings",treat
broccoli,"Green cruciferous vegetable, often steamed or roasted",not treat
chocolate,"Sweet confection made from cocoa beans",treat
kale,"Dark leafy green vegetable, often used in salads",not treat
ice_cream,"Frozen dairy dessert, sweet and creamy",treat
```

Because we used the `-p` flag, the LLM is asked to classify each row with the exact prompt we supplied.
Under the hood, the full prompt sent for the first row looks like this:

```
You are assisting with spreadsheet transformations.
<spreadsheet_rows>
<row_0>
{"food": "pizza", "description": "Cheesy flatbread with tomato sauce and various toppings"}
</row_0>
</spreadsheet_rows>
<user_instruction>
Based on the food description, classify this as 'treat' or 'not treat'
</user_instruction>
For each row, provide a single value for column 'tastiness' that answers the user_instruction.
```

Without the `-p` flag, you get **interactive mode**: the tool asks a planning model to
figure out how to handle your instruction — either as a Python expression or as an
LLM prompt. It shows you what it came up with and waits for you to press `Y` (accept)
or `n` (reject) before anything runs.

In the example below, we reject the suggested Python expression and accept the
generated LLM prompt instead:

```
$ llm mr map "label which foods are treats" -i foods.csv -c tastiness -o foods_classified.csv
Use deterministic expression?
  "treat" if any(w in row["description"].lower() for w in ("sweet", "dessert", "confection")) else "not treat" [Y/n]: n
Run as prompt per row?
  Based on the food name and description, classify this food as 'treat' or 'not treat'. Return only one of those two labels. [Y/n]: Y
Processing 5 batches (parallel=1)
  Completed batch 1/5
  ...
Wrote 5 rows to foods_classified.csv
```

Finally we can provide a Python expression directly with `-e` — no LLM call at all:

```bash
llm mr map '"sweet" in row["description"].lower()' -e -i foods.csv -c is_sweet -o sweets.csv
```

```csv
food,description,is_sweet
pizza,"Cheesy flatbread with tomato sauce and various toppings",False
broccoli,"Green cruciferous vegetable, often steamed or roasted",False
chocolate,"Sweet confection made from cocoa beans",True
kale,"Dark leafy green vegetable, often used in salads",False
ice_cream,"Frozen dairy dessert, sweet and creamy",True
```

### Step 4: Filter (Expression)

You can use the `filter` command to keep only rows matching a criterion.
Here we keep only the rows where the food is a 'treat'.

```bash
llm mr filter 'row["tastiness"] == "treat"' \
  -e -i foods_classified.csv -o treats_only.csv
```

```csv
food,description,tastiness
pizza,"Cheesy flatbread with tomato sauce and various toppings",treat
chocolate,"Sweet confection made from cocoa beans",treat
ice_cream,"Frozen dairy dessert, sweet and creamy",treat
```

Like the map step, we could have used the `-p` flag, or no flag for interactive mode.

### Step 5: Reduce

The `reduce` command groups rows by a given column and summarizes each group.
Output is a small table with two columns: the **group key** and the **reduced
value**. By default those columns are named `group` and `mr_result`; here we
rename them to match the grouping column and a clearer summary name using
`--group-key-column` and `-c` / `--column`.

```bash
llm mr reduce "What characteristics do these foods share?" \
  -p -i foods_classified.csv --group-by tastiness \
  --group-key-column tastiness -c summary -o food_analysis.csv
```

```csv
tastiness,summary
treat,"These foods are typically sweet and have a high sugar content"
not treat,"These foods are typically green and have a bitter taste"
```

Again, we could have used `-e` to provide a Python expression directly (no LLM needed), or no flag for interactive mode.

### Step 6: Expand with --multiple

Sometimes you want the model to produce several values per input row — for
example, brainstorming related items or splitting a field into parts. The
`--multiple` flag tells `map` to expect a list from each LLM call and expand
each item into its own output row.

```bash
llm mr map "Come up with more foods matching this description" \
  -p -i food_analysis.csv --column food --multiple -o more_foods.csv
```

(Use the `food_analysis.csv` from step 5 so the input columns are `tastiness`
and `summary`.)

```csv
tastiness,summary,food
treat,"These foods are typically sweet and have a high sugar content","cake"
treat,"These foods are typically sweet and have a high sugar content","candy_bar"
not treat,"These foods are typically green and have a bitter taste","spinach"
not treat,"These foods are typically green and have a bitter taste","swiss_chard"
```

## Instruction Modes

`llm mr` has three commands: `map` to process each row, `reduce` to group rows, and `filter` to keep only rows matching a criterion.

Every command takes one positional argument — the **instruction** — plus a mode
flag. Input is read from `-i` (file) or stdin; output goes to `-o` (file) or
stdout.

| Flag | Mode | What happens |
|------|------|-------------|
| *(default)* | Interactive | LLM tries to synthesize a Python expression; falls back to writing a prompt to run per-row if it can't. Asks you to confirm before running. Requires `-i` (cannot read from stdin). |
| `-p` | Prompt | Treat the instruction as a literal LLM prompt used to process each row. |
| `-e` | Expression | Treat the instruction as a Python expression evaluated locally. No LLM calls at all. |

```bash
# Interactive — tool figures out the best execution strategy
llm mr map "uppercase the names" -i data.csv -c name_upper -o out.csv

# Prompt — send this exact prompt to the LLM for each row
llm mr map "Classify sentiment as positive/negative/neutral" -p -i data.csv -c sentiment -o out.csv

# Expression — Python expression, no LLM needed
llm mr map 'row["name"].upper()' -e -i data.csv -c name_upper -o out.csv
```

## Selecting Models

To choose a different model from the `llm` tool's default for both the planning and per-item work, use the `-m` flag:

```bash
llm mr map "classify sentiment" -p -i data.csv -m gpt-4o -c sentiment -o out.csv
```

In interactive mode the planning step and per-item work can use different models.
Either side can be overridden independently:

```bash
# Cheap worker, default planner
llm mr map "classify sentiment" -i data.csv --worker-model gpt-4o-mini -c sentiment -o out.csv

# Powerful planner, default worker
llm mr map "classify sentiment" -i data.csv --planning-model gpt-4o -c sentiment -o out.csv

# Override both
llm mr map "classify sentiment" -i data.csv --planning-model gpt-4o --worker-model gpt-4o-mini -c sentiment -o out.csv
```

In `-p` mode, only the worker model is used. In `-e` mode, no model is used.

## Recovering from Failures

All three commands (`map`, `reduce`, `filter`) support **auto-resume**: if a
run is interrupted or some batches fail, simply re-run the same command. Already-
processed rows are detected via content matching and skipped automatically.

```bash
# Initial run — some batches may time out or fail
llm mr map "..." -p -i data.jsonl -c result -j 20 -o output.jsonl
# Warning: 3 batches failed; see output.jsonl.err — rerun to retry

# Just re-run — already-done rows are skipped, only failures are retried
llm mr map "..." -p -i data.jsonl -c result -j 4 -o output.jsonl
```

Each LLM batch has a **120-second timeout** by default. If a batch doesn't
return in time, it is recorded as a failure and retried on the next run.
Use `--timeout` to adjust:

```bash
# Generous timeout for large batches
llm mr map "..." -p -i data.jsonl -c result --timeout 300 -o output.jsonl
```

Error details are written to a sidecar file (`<output>.err`) as JSONL. The
`.err` file is for diagnostics — the resume logic does not depend on it
(except for filter, where it is used to distinguish "errored" from "intentionally filtered out").
Use `--force` to discard an existing output and start fresh.

When output goes to stdout (no `-o`), error records are written as JSONL lines
to stderr instead of a sidecar file. You can redirect them:

```bash
cat data.jsonl | llm mr map "..." -p 2>errors.jsonl > out.jsonl
```

Resume is only available when writing to a file (`-o`). Stdout output is not
resumable.

## Python Expressions (`-e`)

Expression mode (`-e`) evaluates a single Python expression in a restricted
sandbox — no imports, no file access, no side effects. **This is a lightweight
convenience restriction, not a security sandbox**. A determined user can escape
it. Do not rely on it to run untrusted expressions.

### Map expressions

The expression receives a single variable `row`, a dict mapping column names to
string values. It should return the value to store in the target column.

```bash
# row["name"] is available as a string
llm mr map 'row["name"].upper()' -e -i data.csv -c name_upper -o out.csv

# arithmetic on coerced values
llm mr map 'int(row["price"]) * 2' -e -i data.csv -c double_price -o out.csv
```

`--multiple` cannot be combined with `-e`.

### Filter expressions

Same as map: the expression receives `row` (a dict). Return a truthy value to
**keep** the row, falsy to discard it.

```bash
llm mr filter 'int(row["score"]) >= 10' -e -i data.csv -o filtered.csv
llm mr filter '"keyword" in row["text"].lower()' -e -i data.csv -o filtered.csv
```

### Reduce expressions

The expression receives `rows`, a **list** of dicts (all rows in the current
group). It should return a single aggregate value. Output columns are still
`group` and `mr_result` by default (or whatever you pass with
`--group-key-column` / `-c`).

```bash
llm mr reduce 'sum(int(r["score"]) for r in rows)' -e -i data.csv --group-by team -o totals.csv
llm mr reduce 'len(rows)' -e -i data.csv --group-by department -o counts.csv
```

### Available builtins

All standard Python builtins are removed. Only the following are available:

`len`, `int`, `float`, `str`, `bool`, `min`, `max`, `abs`, `round`, `sorted`,
`list`, `tuple`, `set`, `dict`, `sum`, `any`, `all`, `enumerate`, `zip`, `map`,
`filter`

String methods (`.upper()`, `.lower()`, `.split()`, `.strip()`, `.startswith()`,
etc.) and dict methods (`.get()`, `.keys()`, `.values()`, `.items()`) work
normally since they are methods on the values, not builtins.

### What is NOT allowed

- **Imports** — `import`, `__import__()`, and the full `__builtins__` dict are
  all removed.
- **Statements** — the expression must be a single expression, not a statement.
  No `=`, `for` (except in comprehensions), `if` (except ternary), `def`,
  `class`, etc.
- **I/O** — `open`, `print`, `input`, and similar are unavailable.
- **Arbitrary functions** — only the builtins listed above are in scope.

## Piping and Formats

All three commands support stdin/stdout piping alongside file-based I/O.

### Input and output

- `-i` / `--input` — read from a file. Omit to read from stdin.
- `-o` / `--output` — write to a file. Omit to write to stdout.
- `--in-place` — (map only) overwrite the input file. Requires `-i`.

```bash
# File to file
llm mr filter "about climate" -p -i data.csv -o out.csv

# Pipe in, pipe out (JSONL default)
cat data.jsonl | llm mr filter "about climate" -p > out.jsonl

# Pipe chain
cat data.jsonl | llm mr filter "about climate" -p | llm mr map "summarize" -c summary -p > out.jsonl

# File in, pipe out (output matches input format)
llm mr map "summarize" -c summary -p -i data.csv > out.csv
```

When reading from stdin, interactive mode is not available — use `-p` or `-e`.
If stdin is a TTY (nothing piped) and no `-i` is provided, the command errors
with a helpful message.

### Format detection

Format is detected automatically from file extensions. When piping (no file
extension), JSONL is the default. Three flags give explicit control:

- `-f` / `--format` — set the default format for both directions
- `--input-format` — override input format only
- `--output-format` — override output format only

Resolution cascade (applied independently for input and output):

1. Specific flag (`--input-format` / `--output-format`)
2. File extension on `-i` / `-o`
3. General `-f` flag
4. Match the other end
5. JSONL fallback

```bash
# Pipe CSV explicitly
cat data.csv | llm mr filter "about climate" -p -f csv > out.csv

# CSV input file, JSONL stdout output
llm mr filter "about climate" -p -i data.csv --output-format jsonl
```

### Status messages

All progress and status messages go to stderr, keeping stdout clean for data.
This is true whether you use `-o` or pipe to stdout.

### Non-interactive use (`llm` stdin)

When stdin is not a TTY (for example in CI or some automation tools), the
underlying `llm` CLI may wait for input. If a command seems to hang, redirect
stdin, e.g. append `</dev/null` to the command.

## Command Reference

### Map

Apply a transformation to each row, producing a new column.

```bash
llm mr map "Return a short summary of the notes" -p -i data.csv -c summary -o output.csv
```

Options:

- `-i` / `--input` — input file (omit to read stdin)
- `-o` / `--output` or `--in-place` — where to write results (omit `-o` for stdout)
- `-c` / `--column` — target column name (default: `mr_result`)
- `-f` / `--format` — default format for both directions
- `--input-format` / `--output-format` — override format per direction
- `--where` — pre-filter rows (e.g. `status=active`, `score>=10`); unmatched rows are passed through with an empty output column
- `--few-shot N` — use N existing values as examples
- `--batch-size` / `--max-chars` — control batching
- `-j` / `--parallel` — concurrent LLM calls (default: 1)
- `--multiple` — model emits a list per row; each item becomes its own output row
- `-m` / `--model` — LLM model to use
- `--worker-model` — model for per-item work (defaults to `-m`)
- `--planning-model` — model for interactive planning (defaults to `-m`)
- `-n` / `--limit` — only process first N rows
- `--timeout` — seconds per LLM batch before failing it (default: 120)
- `--force` — overwrite existing output even if it doesn't match the input (skips resume)
- `--err PATH` — override the error sidecar path (default: `<output>.err`)
- `--dry-run` — show a sample prompt and exit without making LLM calls
- `-v` / `--verbose` — print each prompt as it is sent

### Reduce

Group rows and summarize each group. Each output row has two fields: the group
key (default column name `group`) and the reduced value (default `mr_result`).
Use `--group-key-column` and `-c` to rename them.

```bash
llm mr reduce "Summarize performance" -p -i data.csv --group-by department -o summary.csv
```

With clearer column names:

```bash
llm mr reduce "Summarize performance" -p -i data.csv --group-by department \
  --group-key-column department -c summary -o summary.csv
```

With `-e`, you can aggregate with plain Python — no LLM needed:

```bash
llm mr reduce 'sum(int(r["score"]) for r in rows)' -e -i data.csv --group-by team -o totals.csv
```

Options:

- `-i` / `--input` — input file (omit to read stdin)
- `-o` / `--output` — output path (omit for stdout)
- `--group-by` — column(s) to group by (required, repeatable)
- `--group-key-column` — name of the group-key column in output (default: `group`)
- `-c` / `--column` — result column name (default: `mr_result`; must differ from `--group-key-column`)
- `-f` / `--format` — default format for both directions
- `--input-format` / `--output-format` — override format per direction
- `--where` — pre-filter rows; unmatched rows are excluded from output
- `--max-chars` — max characters per reduction prompt
- `-j` / `--parallel` — concurrent groups
- `-m` / `--model`, `--worker-model`, `--planning-model`
- `-n` / `--limit` — only process first N groups
- `--timeout` — seconds per LLM group before failing it (default: 120)
- `--force` — overwrite existing output even if it doesn't match (skips resume)
- `--err PATH` — override the error sidecar path (default: `<output>.err`)
- `--dry-run` — show a sample prompt and exit without making LLM calls
- `-v` / `--verbose` — print each prompt as it is sent

### Filter

Keep only rows matching a criterion.

```bash
# Expression: Python filter, no LLM
llm mr filter 'int(row["score"]) >= 10' -e -i data.csv -o filtered.csv

# Prompt: LLM classifies each row
llm mr filter "about prediction markets" -p -i data.csv -m gpt-4o -o filtered.csv

# Interactive: tool tries to synthesize a filter expression
llm mr filter "articles from 2024" -i data.csv -o filtered.csv

# Pipe: stdin to stdout
cat data.jsonl | llm mr filter "about climate" -p > out.jsonl
```

Options:

- `-i` / `--input` — input file (omit to read stdin)
- `-o` / `--output` — output path (omit for stdout)
- `-f` / `--format` — default format for both directions
- `--input-format` / `--output-format` — override format per direction
- `--where` — pre-filter before instruction filter; unmatched rows are excluded from output
- `--batch-size` / `--max-chars` — control batching for LLM mode
- `-j` / `--parallel` — concurrent batches
- `-m` / `--model`, `--worker-model`, `--planning-model`
- `-n` / `--limit` — only consider first N rows
- `--timeout` — seconds per LLM batch before failing it (default: 120)
- `--force` — overwrite existing output even if it doesn't match (skips resume)
- `--err PATH` — override the error sidecar path (default: `<output>.err`)
- `--dry-run` — show a sample prompt and exit without making LLM calls
- `-v` / `--verbose` — print each prompt as it is sent

## Debugging and Cost Tracking

### Inspecting prompts

Use `--dry-run` to see the exact prompt and JSON schema that would be sent to
the model, without actually making any API calls:

```bash
llm mr map "Classify sentiment" -p -i data.csv -c sentiment -o out.csv --dry-run
```

This prints the first batch's prompt, the schema, and the total number of
batches that would be processed, then exits.

Use `--verbose` (or `-v`) to print every prompt as it is sent during a real
run:

```bash
llm mr map "Classify sentiment" -p -i data.csv -c sentiment -o out.csv --verbose
```

Both flags work with `map`, `reduce`, and `filter`.

### Cost tracking

The `llm` tool automatically logs every prompt and response to its SQLite
database. After any `llm mr` run, you'll see a line like:

```
Made 47 LLM calls; run 'llm logs -n 47' to review
```

Use that command to inspect the prompts, responses, and token counts from
your run. For more on the logs system, see the
[llm logs documentation](https://llm.datasette.io/en/stable/logging.html).

## Development

This project is managed with [`uv`](https://docs.astral.sh/uv/latest/) and [`just`](https://just.systems/):

```bash
uv sync         # install dependencies
just test       # run tests
just lint       # check linting and formatting
just fix        # auto-fix linting and formatting
just check      # lint + test
just release    # check, tag v{version}, push branch + tag (see docs/release.md)
```

Release notes live in
[CHANGELOG.md](CHANGELOG.md); maintainers can follow [docs/release.md](docs/release.md)
for versioning, tags, and PyPI.

## Future Work

- **Rate-limiting for `-j` / `--parallel`** — Currently `-j 20` fires all
  requests concurrently with no throttling, which can trigger API rate limits
  (HTTP 429). Failed batches land in the `.err` sidecar and are retried on
  the next run via auto-resume, but adding automatic retry with exponential
  backoff would make high-parallelism runs more robust.

- **Token-limit awareness** — The `--max-chars` flag uses character counts as
  a proxy for token limits. Actual token counts are model-specific and the
  `llm` library does not expose a tokenizer API, so precise per-model token
  budgeting is not feasible in the general case. The current heuristic
  (roughly 4 characters per token for English text) works in practice, and
  context-window errors are caught by the `.err` sidecar and retried on
  re-run.

## See Also

Some other tools offering "run an LLM prompt against every row" features
with different trade-offs:

- [smelt-ai](https://github.com/Cydra-Tech/smelt-ai) — Python library that
  batch-processes `list[dict]` through LLMs with Pydantic-typed outputs, concurrency,
  and retry.
- [Cellm](https://github.com/getcellm/cellm) — `=PROMPT()` formula for Excel.
- [sheets-llm](https://github.com/nicucalcea/sheets-llm) — `=LLM()` custom
  function for Google Sheets.
- [Datablist](https://www.datablist.com/enrichments/run-chatgpt-bulk) — web app
  that runs ChatGPT prompts per CSV row.
- [batch-llm.com](https://batch-llm.com/) — SaaS for uploading CSVs and running
  prompt templates per row via OpenAI, Anthropic, or Google models.
