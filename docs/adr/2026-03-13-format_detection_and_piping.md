# Format Detection and Piping

**Date:** 2026-03-13
**Status:** Accepted

## Context

`llm mr` supports file-based I/O (`-i` / `-o`) and Unix-style stdin/stdout
piping. Format needs to be inferred automatically in most cases, but users
need escape hatches when the inference is wrong. Status and error messages
must not corrupt piped data.

## Decision

**Five-level format cascade.** Format is resolved independently for input and
output:

1. Specific flag (`--input-format` / `--output-format`)
2. File extension on `-i` / `-o`
3. General `-f` / `--format` flag
4. Match the other direction's resolved format
5. JSONL fallback

This means file-to-file usually needs no flags, piping defaults to JSONL
(the natural streaming format), and `-f csv` covers the "pipe CSV both ways"
case without needing two flags.

**JSONL as default pipe format.** JSONL is self-describing (each line is a
JSON object with keys), streams naturally (line-buffered), and round-trips
without header issues. CSV requires coordinating headers across pipe stages.

**All status and progress to stderr.** Every `click.echo()` call uses
`err=True`. Stdout is exclusively for data, whether writing to `-o` or
piping.

**TTY guard on stdin.** If stdin is a TTY (nothing piped) and no `-i` is
given, the command errors immediately with a message suggesting `-i` or
piping. This prevents the "silently waiting for input" footgun.

## Alternatives Considered

**Combined format flag (`-f csv:jsonl`).** Would allow setting input and
output formats in one flag. Rejected as too clever for a rare case — the
separate `--input-format` / `--output-format` flags are clearer.

**CSV as default pipe format.** CSV is more familiar but requires header
coordination across pipe stages, and JSONL is self-describing. JSONL is the
better streaming default.
