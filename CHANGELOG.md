# Changelog

## [Unreleased]

### Added

- **Incremental output**: Results are written to disk as they arrive, preventing data loss on crashes or interruptions. Streaming-capable formats (CSV, JSONL) append rows directly; non-streaming formats (XLSX) use a `.wal` sidecar.
- **Auto-resume**: Re-running a command automatically skips already-processed rows via content matching (superset comparison), without requiring a manual `--repair` flag.
- **`--force` flag** for `map`, `reduce`, and `filter`: Overwrite an existing output file that doesn't match the input shape.
- **WAL (write-ahead log)**: JSONL sidecar (`.wal`) for crash recovery of in-progress runs.
- **`AppendableOutput` protocol** and `RowAppender` protocol for I/O plugins that support incremental file appends.
- `open_append` implemented for CSV and JSONL output plugins.
- **`ReorderBuffer`** for maintaining input ordering with parallel batches during incremental output.
- **Filter `--err` support**: Filter errors are now caught per-batch and recorded to the `.err` sidecar, enabling filter resume.

### Changed

- Removed `--repair` flag (replaced by automatic resume).
- File output writes are now atomic (temp file + rename) for non-incremental paths.
- `InputPlugin` protocol now requires a `typed: bool` property indicating whether the format preserves Python types on round-trip. Resume matching uses strict equality for typed formats (JSONL, XLSX) and allows string coercion for untyped formats (CSV).
- WAL and `.err` readers tolerate truncated/corrupt lines from mid-write kills.

## [0.1.0] — 2026-03-19

### Added

- Initial PyPI release: `llm mr` commands (`map`, `reduce`, `filter`) for CSV, JSONL, and XLSX.
- Pluggable I/O via the `llm_mr` entry-point group (`register_mr_inputs` / `register_mr_outputs`).

[Unreleased]: https://github.com/jcushman/llm-mr/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/jcushman/llm-mr/releases/tag/v0.1.0
