# Expression Sandbox

**Date:** 2026-03-13
**Status:** Accepted

## Context

Expression mode (`-e`) runs user-provided Python expressions via `eval()`. We
need to prevent accidental damage (typos calling `open()` or `exit()`) without
claiming to provide a secure sandbox — the user is running their own code on
their own machine.

## Decision

**Restricted `eval`, framed as convenience not security.** Expressions run
with `__builtins__` replaced by a whitelist: `len`, `int`, `float`, `str`,
`bool`, `min`, `max`, `abs`, `round`, `sorted`, `list`, `tuple`, `set`,
`dict`, `sum`, `any`, `all`, `enumerate`, `zip`, `map`, `filter`. No imports,
no `open`, no `exec`. Docs explicitly state this is a "lightweight convenience
restriction, not a security sandbox" — a determined user can escape it.

**Unified builtins for all expression types.** Map (`row`), filter (`row`),
and reduce (`rows`) share one `_EXPR_BUILTINS` set. Keeping them identical
simplifies the code and documentation.

**`--multiple` works with `-e`.** An early version blocked `--multiple` with
expressions. This was lifted — if the expression returns an iterable (that
isn't a string), `_expand_multiple_rows` handles it. The restriction was
unnecessary given the expansion logic already existed.

**Relaxed type checks in `_parse_map_response`.** Return values are not
restricted to strings or lists — any JSON-serializable value is accepted.
Numbers, bools, and nested objects are valid expression and LLM results.

## Alternatives Considered

**True sandbox (RestrictedPython, subprocess jail).** Heavyweight for the use
case. Users are running their own expressions on their own data; the threat
model is accidental damage, not adversarial input.

**No restrictions at all.** Easy to implement but too easy to accidentally
call `exit()` or `open()` in an expression meant to transform spreadsheet
data. The whitelist catches the common footguns.
