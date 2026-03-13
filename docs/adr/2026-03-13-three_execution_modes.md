# Three Execution Modes

**Date:** 2026-03-13
**Status:** Accepted

## Context

Users interact with `llm mr` across a spectrum: some know exactly what Python
expression they want, some know the prompt they'd like to run per row, and some
just want to describe what they need and let the tool figure it out. The
original design had a `--smart-model` flag that opted into LLM-assisted
planning, but this made the common exploratory path opt-in rather than default.

Interactive mode also needs a TTY for confirmation prompts, which conflicts
with stdin piping.

## Decision

Every command accepts a single positional **instruction** argument interpreted
in one of three modes:

| Flag | Mode | Behavior |
|------|------|----------|
| *(default)* | Interactive | One planning LLM call synthesizes a Python expression *or* a crafted prompt. User confirms before execution. Falls back gracefully. |
| `-p` | Prompt | Instruction is a literal LLM prompt, sent per row/group. No magic. |
| `-e` | Expression | Instruction is a Python expression. No LLM calls at all. |

The interactive planning step is a **single** LLM call that decides between
expression and prompt, rather than a two-step "try expression first, then fall
back." This keeps latency low and lets the planner craft a better prompt when
an expression isn't feasible.

Interactive mode requires `-i` (file input) because `click.confirm()` needs a
TTY. When stdin is piped, users must choose `-p` or `-e` explicitly.

## Alternatives Considered

**`--smart-model` as opt-in flag.** The original design required users to opt
into LLM planning. This buried the most useful mode behind a flag most users
wouldn't discover. Making interactive the default matches how people explore
tools — start loose, then lock down to `-p` or `-e` once they know what works.

**Two-step planning (try expression, then fall back to raw instruction).**
Sending the raw user instruction as a prompt is often worse than having the
planner rephrase it for per-row use. A single planning call that can return
either form produces better prompts when expressions aren't possible.

**Separate `--prompt` and `--expression` arguments.** One positional arg with
mode flags is simpler — three interpretations of the same input, not three
input channels.
