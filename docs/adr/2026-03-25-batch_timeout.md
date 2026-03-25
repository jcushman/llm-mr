# Batch Timeout

**Date:** 2026-03-25
**Status:** Implemented

## Context

The `llm` library does not expose a timeout parameter on `model.prompt()`.
The underlying OpenAI client defaults to a 10-minute read timeout; other
providers vary.  When a provider accepts a request but stops responding
(server-side hang, silent rate-limit, network stall), a single batch can
block the entire job indefinitely — the user sees 174/178 batches complete
and then nothing.

The `llm` library has an open issue requesting timeout support
(simonw/llm#1279) but no implementation as of v0.29.

## Decision

**`--timeout` flag with a 120-second default.**  Each LLM batch is wrapped
in a deadline enforced via `concurrent.futures`.  If the call does not
return within the timeout, it is treated as a batch failure: recorded in
the `.err` sidecar, counted in the warning message, and retried on the
next auto-resume run.

**Implementation via `ThreadPoolExecutor`.**  The parallel path (`-j N`)
already uses `ThreadPoolExecutor`; adding `timeout=` to `future.result()`
is a one-line change.  The sequential path (`-j 1`) submits each batch to
a single-worker pool so the same `future.result(timeout=…)` mechanism
applies uniformly.  This is encapsulated in a `_submit_and_wait` helper.

**Provider-agnostic.**  Because the timeout wraps the entire
`model.prompt()` + `response.text()` call from the outside, it works with
any `llm` model plugin — OpenAI, Anthropic, Ollama, etc. — without
requiring provider-specific timeout configuration.

**Timed-out threads are not cancelled.**  Python's `ThreadPoolExecutor`
does not support cancelling running threads.  A timed-out batch's thread
continues in the background until the provider eventually responds or the
process exits.  The batch is still recorded as failed and the row data is
not lost — auto-resume handles it on the next run.

## Alternatives Considered

**Signal-based timeout (`SIGALRM`).**  Only works on Unix, only in the
main thread, and is fragile with multi-threaded code.  Not portable.

**Patching the `llm` model's HTTP client timeout.**  Would require
reaching into `model.get_client()` internals, differs per provider plugin,
and would not cover non-HTTP models (e.g. local Ollama).

**No default timeout (opt-in only).**  The 10-minute OpenAI default is
long enough that most users would never set it, and the failure mode
(silent hang) is confusing.  A 120-second default is conservative enough
to avoid false positives on large batches while catching genuine hangs.
