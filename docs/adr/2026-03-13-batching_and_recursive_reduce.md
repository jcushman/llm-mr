# Batching and Recursive Reduce

**Date:** 2026-03-13
**Status:** Accepted

## Context

LLM context windows have finite size. Map operations can batch multiple rows
into one prompt for efficiency, and reduce operations must fit an entire group
into a prompt. Both need a way to limit prompt size, but actual token limits
are model-specific and the `llm` library does not expose a tokenizer API.

## Decision

**Character-based `--max-chars`.** Prompt size is controlled by `--max-chars`,
a character count threshold. The ~4 characters/token heuristic for English
text is coarse but works in practice. Context-window overflows are caught by
the `.err` sidecar and auto-resume, so the heuristic does not need to be exact.

**Recursive summarization for reduce.** When a reduce group exceeds
`--max-chars`, the group is split in half and each half is reduced separately.
The two partial summaries are then reduced together. This keeps every prompt
within the size limit but changes semantics: for large groups, the model sees
intermediate summaries rather than raw rows.

**Map batching with `--batch-size`.** Map and filter batch rows into a single
prompt up to `--batch-size` rows or `--max-chars` characters, whichever is
hit first. Structured output schemas scale with batch size.

## Alternatives Considered

**Token-based limits.** Would require a tokenizer (tiktoken or similar) and
per-model configuration. The `llm` library has no tokenizer API, and different
models use different tokenizers. Character-based limits with error recovery
are simpler and sufficient.

**Truncation instead of recursive reduce.** Dropping rows from a group loses
data silently. Recursive summarization preserves all rows at the cost of the
model seeing summaries for large groups — an explicit and documented trade-off.
