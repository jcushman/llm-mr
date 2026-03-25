# Incremental Output and Resume

**Date:** 2026-03-24
**Status:** Proposed

## Context

When LLM processing takes minutes or hours (`llm mr map -p "..." -i in.jsonl
-o out.jsonl`), the current design holds all results in memory and writes the
output file once at the end.  If the process is killed or a batch hangs, all
successful work is lost — the only recovery path is the `.err` sidecar plus
`--repair`, which only covers batch-level failures, not interruptions.

A common interactive workflow is: run with `-n 5` to preview, like the
results, run the full job, kill when something hangs, re-run.  This workflow
wants two properties:

1. **Incremental persistence** — results are written to disk as they arrive,
   so a crash or ctrl-c doesn't lose completed work.
2. **Auto-resume** — re-running the same command skips rows that are already
   done, without requiring `--repair`.

This ADR covers the design for both, across all combinations of command type,
output format, and output target.

## Relevant combinatorics

The following dimensions interact:

| Dimension | Variants |
|-----------|----------|
| **Command** | map, reduce, filter |
| **Processing mode** | expression (`-e`), prompt (`-p`), interactive |
| **Output target** | new file path, existing file path, stdout/stream |
| **Output format** | streaming-capable (JSONL, CSV) vs file-only (XLSX) |
| **Parallelism** | serial (`-j 1`) vs parallel (`-j N`) |
| **Row mapping** | 1:1 (normal map/filter), 1:N (`--multiple`), N:1 (reduce) |

Expression mode is deterministic and fast — no WAL or resume needed.  The
rest of this ADR covers prompt and interactive modes only.

## Decision

### Empty target column means unprocessed

A row is considered "done" only if its target column has a non-empty value.
`None`, empty strings, and whitespace-only strings are all treated as
"needs processing."  This is the same rule as the existing `_has_value`
helper used by `--in-place` skip logic.

This is a deliberate trade-off.  In JSONL, a missing key is unambiguously
"not processed," but CSV has no way to distinguish "key absent" from "key
present with empty value" — every row has every column.  Rather than
format-specific logic, we adopt a single rule: **empty = unprocessed**.

The cost is that an LLM which legitimately returns an empty string will
cause that row to be re-processed on resume.  In practice this is rare and
harmless — the LLM will return empty again, and the row is re-written.

### Matching output rows to input rows

When an existing output file is present, we determine which input rows are
already done by **content matching**, not positional indexing.

- **Map:** Each output row is a superset of its input row (input fields plus
  the target column).  Walk input and output in tandem; for each input row,
  consume the next output row if every input key-value pair is present in it
  and the target column is non-empty.  If it matches, the row is done.  If
  not, the row needs processing.

- **Filter:** Output rows are identical to their input rows (no new columns).
  Walk input and output in tandem; if the next output row matches the current
  input row, that input row was kept.  Rows absent from the output could mean
  "filtered out" or "not yet evaluated" — see "Filter resume" below for how
  this ambiguity is resolved.

- **Reduce:** Output rows are keyed by the group key column.  Match by group
  key value — if a group key already exists in the output, that group is
  done.

For **duplicate input rows**, the superset check is greedy: consume the next
matching output row for the current input row, advance both cursors.  Two
identical input rows with one matching output row means the first is done and
the second needs processing.

For **`--multiple`**, one input row may produce N consecutive output rows that
are all supersets of it.  Consume all consecutive superset matches for the
current input row.

### When a WAL is needed vs not

The design uses a write-ahead log (WAL) — a JSONL sidecar file — only when
results cannot be appended directly to the output.  The WAL records completed
results only (not errors — see "Role of `.err`" below).

**No WAL needed:**

- **Streaming output plugin + new/empty file path:** Open the output file in
  append mode, write each row (or batch of rows) as results arrive, flush
  after each write.  The output file itself is the progress record.  On
  resume, superset-match against the partially-written output to find where
  to continue.

- **Stdout/stream output:** No persistence, no resume.  Every run produces
  the full output.  (Repeated `cat foo.csv | llm mr map "prompt"` should
  always print all rows, not just missed ones.)

**WAL needed:**

- **Non-streaming output plugin (XLSX):** Cannot append incrementally.  Write
  completed results to the WAL as they arrive.  On finish, merge input rows
  with WAL results and write the output file.  On resume, load the WAL to
  skip already-done rows.

- **Existing output file with pending rows:** Pending rows fall into two
  classes.  **Gap rows** appear before the last matched output row — they
  must be interleaved among done rows, so they cannot be appended and go to
  the WAL.  **Tail rows** appear after the last matched output row — they
  are purely sequential and can be appended directly to the output file,
  just like case 1.  Only gap rows require the WAL; the (often much larger)
  tail streams to the output file.  On finish, merge gap results from the
  WAL into the existing output via temp-file swap; appended tail rows are
  already in place.  The common case of "ran with `-n 50`, now running the
  full 1000" has 50 done rows, 0 gaps, and 950 tail rows — all appended,
  no WAL at all.

### Role of `.err`

The `.err` sidecar is retained as a **diagnostic log** — it records error
messages so users can see *why* rows failed.  It is not consumed by the
resume machinery.  Resume does not need to distinguish "errored" from "not
yet reached" — both are simply "not in the output" and will be re-processed.

The `.err` file is useful for:
- User-facing warnings: "3 batches failed; see out.jsonl.err"
- Debugging: inspecting error messages, rate limit details, etc.

It is *not* useful for:
- Storing success records (those go to the output file or WAL)
- Determining which rows need processing — *except* for filter, where
  `.err` is needed to distinguish "errored" from "intentionally filtered
  out" (see "Filter resume")

### WAL format

The WAL is always JSONL (cheap append, one entry per line, survives partial
writes).  Path: `<output>.wal` (analogous to current `<output>.err`).
The WAL contains only success entries — error tracking stays in `.err`.

For map:
```jsonl
{"i": 0, "c": "sentiment", "v": "positive"}
{"i": 1, "c": "sentiment", "v": "negative"}
```

For reduce:
```jsonl
{"g": "US", "c": "summary", "v": "The US market ..."}
```

For filter:
```jsonl
{"i": 0, "kept": true}
{"i": 1, "kept": false}
```

Compact keys to keep the file small.  The WAL is not self-contained — it
stores results keyed by input row index, not full rows.  The merge step
reconstructs full output rows by combining input rows with WAL values.
This requires the input file to be available and unchanged (see "Input file
must not change between runs").

### Parallel output ordering

With `-j N`, batches complete out of order.  For streaming output (append to
file), rows must be written in input order to support the superset-matching
resume logic.  The processor maintains a **reorder buffer**:

- Track a write cursor: "rows through index N have been flushed."
- When a batch completes, buffer it if there are gaps before it.
- When the next contiguous batch is available, flush it (and any consecutive
  buffered batches) to the output file.
- Memory cost is bounded: at most `(parallel - 1) * batch_size` rows.

On ctrl-c, the reorder buffer is flushed to the WAL (or to the output file
for tail rows that happen to be contiguous).  The WAL is keyed by row index,
not ordered, so out-of-order appends are fine — on resume, entries are
looked up by index regardless of the order they were written.

### Lifecycle of a run

**Case 1: Streaming output, new/empty file**

1. Open output file in append mode.
2. Process rows via LLM.  As each batch completes, write results to output
   (respecting reorder buffer if parallel).  Flush after each write.
3. Errors are appended to `<output>.err` (unchanged from today).
4. On finish, print summary.  On ctrl-c, output has all flushed rows;
   re-run resumes via superset matching.

**Case 2: Streaming output, existing file with partial results**

1. Load existing output file.  Walk input + output with superset matching
   to identify done rows and classify pending rows as gap or tail.
2. If there are gap rows, create WAL at `<output>.wal`.
3. Process all pending rows (gaps and tail).  Gap results go to the WAL;
   tail results are appended directly to the output file (as in case 1).
4. On finish: if WAL exists, merge input + existing output + WAL via
   temp-file swap, then delete WAL.  (Tail rows are already in the output
   file and included in the merge.)
5. On ctrl-c + re-run: same startup — load output + WAL (if any), skip
   done rows from both, reclassify remaining pending rows, continue.

**Case 3: Non-streaming output (XLSX), new or existing file**

1. If output exists, load it and superset-match to find done rows.
2. Create WAL.  Process remaining rows, append to WAL.
3. On finish: merge all sources, write output via plugin, delete WAL.
4. On ctrl-c + re-run: load output (if exists) + WAL, skip done rows.

**Case 4: Stdout/stream**

1. Process all rows (no resume).
2. Write to stream as results arrive (reorder buffer if parallel).
3. No WAL.  Errors go to stderr (or `.err` if `--err` is given).

### WAL cleanup

The WAL is deleted after a successful merge into the output file.  The
lifecycle is:

- **Created** when the first gap-row result (or non-streamable result)
  arrives.  Not created at all if there are no gap rows and the output
  plugin supports streaming.
- **Appended to** as batches complete (gap rows) or on ctrl-c (reorder
  buffer flush).
- **Merged on ctrl-c.**  The signal handler flushes the reorder buffer to
  the WAL, then performs the full merge: build merged rows in memory, write
  to a temp file via the output plugin, atomic-rename over the output path,
  delete the WAL.  This is safe because the write-then-swap sequence cannot
  corrupt the output file — if interrupted during the temp-file write, the
  output is untouched and the WAL remains; if interrupted after rename but
  before WAL deletion, the next run sees a WAL whose entries are already in
  the output and deletes it harmlessly.  The merge may take a few seconds
  for large XLSX files, which is acceptable cleanup time.
- **Also consumed** on normal finish or on the next run (if the signal
  handler didn't complete the merge).
- **Deleted** once the merged output is successfully written (after the
  temp-file swap or plugin write completes).

If the process is killed between "output written" and "WAL deleted," the
next run sees a WAL whose entries are already in the output.  The superset
match marks those rows as done, the WAL entries are redundant, and the WAL
is deleted without re-merging.  This makes the cleanup crash-safe.

### Interaction with `--repair`

Auto-resume makes `--repair` redundant.  Re-running the same command
automatically skips done rows and reprocesses everything else — there is no
distinction between "errored" and "never reached."  `--repair` is removed.

The `.err` sidecar continues to record error diagnostics in all cases; it is
written during the run and read by humans, but not consumed by resume logic.

### Interaction with `--in-place`

`--in-place` sets output path = input path.  The existing output file *is*
the input file, which already has the right shape (input fields present,
target column may be empty).  Superset matching naturally treats rows with a
populated target column as done.  The merge step writes to a temp file and
swaps, same as case 2.

### Input file must not change between runs

The resume mechanism assumes the input file is unchanged between runs.
Superset matching walks input and output in tandem, and WAL entries
reference input row indices.  If the user adds, removes, or reorders input
rows between runs, matching and WAL indices become invalid.  The tool does
not detect this — it is the user's responsibility.  (A future enhancement
could store an input checksum in the WAL to warn on mismatch.)

### Stdin input

When input comes from stdin (no `-i`), there is no file to re-read on
resume.  This limits what the tool can do:

- **Streaming output to new file:** Works — rows are appended to the output
  as they complete.  But re-running cannot resume (no input to re-walk), so
  the partial output is useful but not continuable.
- **WAL-requiring cases (XLSX output, existing output with gaps):** The WAL
  stores row indices, not full rows, so it cannot reconstruct output without
  the input file.  These cases fall back to current behavior: materialize
  all rows in memory, process, write once at the end.  No WAL is created.
  On ctrl-c, the signal handler writes all completed rows to the output
  file; only in-flight batches are lost.

In short: incremental output (streaming to file) works with stdin;
auto-resume does not.  Resume requires `-i`.

### Existing output file that is not resumable

If `-o` points to an existing file and the superset match fails immediately
(the first output row is not a superset of the first input row), the file
is not a resumable partial — it is unrelated or from a different command.
This is an error:

> Error: out.jsonl exists but does not match the input shape.
> Use --force to overwrite, or remove the file and re-run.

A `--force` flag bypasses the check and overwrites.  Without it, the tool
refuses to proceed rather than silently destroying an unrelated file.

If the superset match succeeds for some rows and then diverges (e.g. the
output has 50 matching rows followed by unrecognizable data), the tool
treats the first 50 as done and the rest of the output as corrupt tail.
It warns and proceeds, overwriting the corrupt portion on merge.

### Interaction with `--where`

Map with `--where` processes only matching rows but writes *all* rows to
output (non-matching rows pass through unchanged).  For resume, this means
three categories of input row:

- **Done:** superset match found, target column non-empty.
- **Not matching `--where`:** written through unchanged (target column
  absent or empty).
- **Matching `--where` but not yet processed:** sent to the LLM.

The "empty = unprocessed" rule creates an interaction here: non-matching
rows have an empty target column, which looks identical to "matching but
not yet processed."  The `--where` filter resolves this — on resume, the
`--where` predicate is re-evaluated, so non-matching rows are identified
and written through without being sent to the LLM.  The superset match
determines *which* rows are already done; `--where` determines which of
the remaining rows should be processed vs passed through.

Changing `--where` between runs is safe but may cause previously-skipped
rows to be processed (if the new filter matches them) or previously-
processed rows to be passed through unchanged (their existing target
column value is preserved by the superset match).

### Interaction with `--limit`

`--limit N` restricts the number of input rows read.  On resume, the
superset match only examines the first N rows of input.  Running with
`-n 5`, then re-running with `-n 10`, naturally processes 5 new rows — the
first 5 are matched as done, rows 6–10 are tail.  Running without `-n`
processes all remaining rows.  No special handling needed.

### Filter resume

Filter is structurally different from map: the output contains only kept
rows, not all input rows.  This makes resume harder — a row absent from the
output could mean "filtered out" or "not yet evaluated."

For filter with an existing output file, the superset match walks input and
output in tandem.  Each input row falls into one of four categories:

- **Matches next output row:** kept, done.
- **Doesn't match, row index appears in `.err`:** errored, needs
  re-processing.
- **Doesn't match, not in `.err`, before last output match:**
  intentionally filtered out.
- **Doesn't match, after last output match:** not yet reached, needs
  processing.

This is the one case where `.err` is consumed by resume logic rather than
being purely diagnostic.  Filter is the only command where the output
doesn't contain all input rows, so "absent from output" is ambiguous
without the error record.  This also means filter gap-fill works — errored
rows before the last match can be retried because `.err` distinguishes them
from intentionally-filtered rows.

### Few-shot examples and interactive planning on resume

`_extract_few_shot_examples` needs rows with a populated target column to
build example prompts.  On resume, these come from the already-done rows in
the existing output file (loaded during the superset-matching step).  The
processor must retain a few completed rows for this purpose rather than
discarding them after matching.

Interactive planning (`_interactive_plan_map`, etc.) shows sample rows to
the planning model and asks the user to confirm.  On resume (output file
exists), planning is skipped — the existence of a partial output implies
the user already approved a plan.  The prompt text must be provided via
`-p` on resume; interactive mode with an existing output file uses the
prompt as-is without re-planning.

### Expression mode

Expression mode is deterministic and fast — no WAL, no resume needed.  In
practice this requires no special "skip the WAL" logic: expression mode
takes the existing streaming code path (`_stream_map_expression`,
`_stream_filter_expression`) which bypasses the LLM batch-processing code
entirely.  The WAL/resume machinery lives inside the LLM path and is simply
never reached.  The `--in-place` expression case materializes (can't read
and write the same file concurrently) but is still deterministic and writes
once.

## Alternatives considered

**Progress journal (WAL for all cases).** An earlier design used a WAL
unconditionally — even when streaming to a new file.  This adds a sidecar
file that must be managed (created, merged, deleted) even in the simple
case.  Since streaming-capable formats can use the output file itself as the
progress record, the WAL is only needed when appending isn't possible.

**Positional matching (count lines to determine progress).** Fragile when
`--multiple` changes the row count, when `--where` filters rows, or when the
output file is truncated by a mid-write crash.  Content-based superset
matching is more robust and handles all these cases uniformly.

**Separate resume command.** A `--resume` flag or subcommand, rather than
auto-detecting.  Adds friction to the common workflow (run, kill, re-run)
and requires users to remember a flag.  Auto-detection via superset matching
is safe — if the output file doesn't match the input shape, the tool
errors out and requires `--force` to overwrite.

**Rewrite output after every batch (XLSX).** openpyxl can build a workbook
incrementally, but `save()` rewrites the entire file.  For large files this
is O(n^2).  The WAL avoids this — write once at the end, with crash recovery
via the WAL.
