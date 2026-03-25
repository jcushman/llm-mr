# Error Handling and Repair

**Date:** 2026-03-13
**Status:** Partially superseded by [Incremental Output and Resume](2026-03-24-incremental_output_and_resume.md)

> **Note (2026-03-24):** The `--repair` flag described below has been removed.
> Auto-resume now handles failure recovery: re-running the same command
> automatically skips already-processed rows and retries everything else.
> The `.err` sidecar, per-batch resilience, and `--err` flag remain as
> described.  See the incremental output ADR for the current design.

## Context

When processing hundreds of rows with `-j 20`, some batches will fail — rate
limits, timeouts, transient API errors. The tool needs to handle partial
failure without losing successful results, and give users a path to retry just
the failures.

## Decision

**Sidecar `.err` file, not tombstones.** Failed rows/groups are written to
`<output>.err` as JSONL, separate from the main output. This keeps the output
file clean — empty string is a valid LLM result, so tombstone values would be
ambiguous. The `.err` file records the original row indices and error messages.

**Per-batch resilience.** A single batch failure does not abort the job.
Errors are caught, logged, and recorded; remaining batches continue. The
warning message directs users to re-run to retry.

**~~`--repair` retries only failures.~~** *(Removed — see note above.)*
Running with `--repair` reads the `.err` sidecar, reprocesses only those
rows, merges successes into the output, and removes repaired entries from
`.err`. It is idempotent — still-failing items stay in `.err`.

**`--err` overrides the error file path.** By default, when `-o` is given the
sidecar is written to `<output>.err`. The `--err PATH` flag overrides that
location. When output goes to stdout, `--err` enables file-based error
logging that would otherwise be unavailable — without it, error records are
written as JSONL to stderr instead.

## Alternatives Considered

**Tombstone values in output (e.g. `__ERROR__`).** Overloads the output
semantics — empty or sentinel strings could collide with real LLM output.
Repair would need to scan the output for tombstones rather than reading a
clean index.

**Abort on first failure.** Wastes all successful work. With parallel
I/O-bound calls, partial failure is the norm, not the exception.

**Automatic retry with backoff.** Useful but orthogonal — it belongs in the
concurrency layer and is listed as future work. The `.err` / auto-resume
mechanism handles the "some still failed after retries" case.
