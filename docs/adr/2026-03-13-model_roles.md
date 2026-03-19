# Model Roles

**Date:** 2026-03-13
**Status:** Accepted

## Context

Interactive mode has two distinct LLM workloads: one planning call (synthesize
an expression or prompt) and potentially many per-item calls. These have very
different cost/quality profiles — planning benefits from a capable model while
per-item work can use a cheaper one. Users also need API key configuration,
and the `llm` tool already manages keys per model.

## Decision

**Two optional role overrides on top of `-m`.** The `-m` / `--model` flag sets
the default for everything. `--planning-model` overrides the model used for
the one-shot interactive planning step. `--worker-model` overrides the model
for per-item LLM calls. Either can be set independently; unset roles fall back
to `-m`, which falls back to llm's configured default. The most likely scenarios
are probably that the user just uses their default model; or uses -m for both;
or uses their default model plus one of the specific flags. Using -m and a
specific flag is fine but not common.

In `-p` mode only the worker model matters. In `-e` mode no model is used.

## Alternatives Considered

**Single model only.** Simpler, but forces users to choose between quality
(expensive model for everything) and cost (cheap model that plans poorly).
The two-role split keeps the common case simple (`-m` alone) while enabling
cost optimization.
