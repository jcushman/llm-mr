from __future__ import annotations

import json
import shutil
import sys
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)

import click
import llm
from llm.cli import get_default_model

from .registries import (
    AppendableOutput,
    PluginContext,
    StreamableInput,
    StreamableOutput,
    TableStream,
    normalize_extension,
)


@dataclass
class FilterCondition:
    column: str
    op: str
    value: Any

    def matches(self, row: Dict[str, Any]) -> bool:
        raw = row.get(self.column)
        if raw is None:
            return False
        if self.op == "=":
            return raw == self.value
        if self.op == "!=":
            return raw != self.value
        try:
            lhs = _coerce_numeric(raw)
            rhs = _coerce_numeric(self.value)
        except ValueError:
            lhs = raw
            rhs = self.value
        if self.op == ">":
            return lhs > rhs
        if self.op == ">=":
            return lhs >= rhs
        if self.op == "<":
            return lhs < rhs
        if self.op == "<=":
            return lhs <= rhs
        raise ValueError(f"Unsupported operator: {self.op}")


# ---------------------------------------------------------------------------
# Shared CLI options
# ---------------------------------------------------------------------------


def _common_options(f):
    """Options shared by map, reduce, and filter."""
    f = click.argument("instruction")(f)
    f = click.option(
        "-i",
        "--input",
        "input_path",
        type=click.Path(exists=True, dir_okay=False, path_type=Path),
        default=None,
        help="Input file path. Omit to read from stdin.",
    )(f)
    f = click.option(
        "-p",
        "--prompt",
        "prompt_mode",
        is_flag=True,
        help="Treat instruction as a literal LLM prompt (run per item)",
    )(f)
    f = click.option(
        "-e",
        "--expression",
        "expression_mode",
        is_flag=True,
        help="Treat instruction as a Python expression (deterministic)",
    )(f)
    f = click.option("-m", "--model", help="LLM model to use")(f)
    f = click.option(
        "--worker-model", help="Model for per-item LLM work (defaults to -m)"
    )(f)
    f = click.option(
        "--planning-model", help="Model for interactive planning (defaults to -m)"
    )(f)
    f = click.option(
        "-f",
        "--format",
        "format_",
        default=None,
        help="Default format for both input and output (csv, jsonl)",
    )(f)
    f = click.option(
        "--input-format",
        default=None,
        help="Override input format (csv, jsonl)",
    )(f)
    f = click.option(
        "--output-format",
        default=None,
        help="Override output format (csv, jsonl)",
    )(f)
    f = click.option(
        "--where",
        "filters",
        multiple=True,
        help="Pre-filter like status=active or score>=10",
    )(f)
    f = click.option(
        "-n",
        "--limit",
        type=click.IntRange(1),
        default=None,
        help="Only process the first N items",
    )(f)
    f = click.option(
        "--timeout",
        type=click.FloatRange(min=0, min_open=True),
        default=120,
        show_default=True,
        help="Seconds to wait for each LLM batch before failing it",
    )(f)
    f = click.option(
        "--dry-run",
        is_flag=True,
        help="Show a sample prompt and exit without making LLM calls",
    )(f)
    f = click.option(
        "-v",
        "--verbose",
        is_flag=True,
        help="Print each prompt as it is sent to the model",
    )(f)
    return f


def _validate_mode_flags(prompt_mode: bool, expression_mode: bool) -> str:
    """Return the execution mode: 'prompt', 'expression', or 'interactive'."""
    if prompt_mode and expression_mode:
        raise click.UsageError("Cannot use both -p and -e")
    if prompt_mode:
        return "prompt"
    if expression_mode:
        return "expression"
    return "interactive"


def _resolve_formats(
    input_path: Optional[Path],
    output_path: Optional[Path],
    format_: Optional[str],
    input_format: Optional[str],
    output_format: Optional[str],
) -> Tuple[str, str]:
    """Resolve input and output format names using the cascade.

    Priority per direction: specific flag > file extension > general flag >
    match the other end > JSONL fallback.
    """
    in_fmt = input_format
    out_fmt = output_format

    if in_fmt is None and input_path is not None:
        ext = normalize_extension(input_path.suffix)
        if ext:
            in_fmt = ext
    if out_fmt is None and output_path is not None:
        ext = normalize_extension(output_path.suffix)
        if ext:
            out_fmt = ext

    if in_fmt is None and format_ is not None:
        in_fmt = format_
    if out_fmt is None and format_ is not None:
        out_fmt = format_

    if in_fmt is None and out_fmt is not None:
        in_fmt = out_fmt
    if out_fmt is None and in_fmt is not None:
        out_fmt = in_fmt

    if in_fmt is None:
        in_fmt = "jsonl"
    if out_fmt is None:
        out_fmt = "jsonl"

    return in_fmt, out_fmt


WriteFn = Callable[[Iterable, Sequence[str]], None]


@contextmanager
def _open_input(context: PluginContext, input_path: Optional[Path], in_fmt: str):
    """Open the resolved input and yield a TableStream.

    For file paths, delegates to ``plugin.open(path)``.  For piped stdin,
    uses ``plugin.open_stream(stdin)`` when available (StreamableInput) or
    spools stdin to a temp file as a fallback.
    """
    if input_path is not None:
        plugin = context.inputs.for_path(input_path)
        with plugin.open(input_path) as stream:
            yield stream
    else:
        plugin = context.inputs.for_name(in_fmt)
        if isinstance(plugin, StreamableInput):
            with plugin.open_stream(sys.stdin) as stream:
                yield stream
        else:
            with _spooled_stdin() as tmp_path:
                with plugin.open(tmp_path) as stream:
                    yield stream


@contextmanager
def _spooled_stdin():
    """Spool stdin to a temp file for plugins that require a Path."""
    tmp = tempfile.NamedTemporaryFile(
        mode="w", suffix=".tmp", delete=False, encoding="utf-8"
    )
    try:
        shutil.copyfileobj(sys.stdin, tmp)
        tmp.close()
        yield Path(tmp.name)
    finally:
        Path(tmp.name).unlink(missing_ok=True)


def _output_writer(
    context: PluginContext, output_path: Optional[Path], out_fmt: str
) -> WriteFn:
    """Return a ``(rows, fieldnames) -> None`` callable for the resolved output.

    For file paths, delegates to ``plugin.write(path, ...)``.  For piped
    stdout, uses ``plugin.write_stream(stdout, ...)`` when available
    (StreamableOutput) or writes to a temp file and copies as a fallback.
    """
    if output_path is not None:
        plugin = context.outputs.for_path(output_path)

        def write(rows: Iterable, fieldnames: Sequence[str]) -> None:
            plugin.write(output_path, rows, fieldnames)

        return write

    plugin = context.outputs.for_name(out_fmt)
    if isinstance(plugin, StreamableOutput):

        def write(rows: Iterable, fieldnames: Sequence[str]) -> None:
            plugin.write_stream(sys.stdout, rows, fieldnames)

        return write

    def write(rows: Iterable, fieldnames: Sequence[str]) -> None:
        tmp = tempfile.NamedTemporaryFile(suffix="." + out_fmt, delete=False)
        tmp_path = Path(tmp.name)
        tmp.close()
        try:
            plugin.write(tmp_path, rows, fieldnames)
            sys.stdout.buffer.write(tmp_path.read_bytes())
        finally:
            tmp_path.unlink(missing_ok=True)

    return write


def _stdin_guard(input_path: Optional[Path]) -> None:
    """Error if reading from stdin but stdin is a TTY (nothing piped)."""
    if input_path is None and sys.stdin.isatty():
        raise click.UsageError("Provide -i <file> or pipe data to stdin.")


def _interactive_stdin_guard(mode: str, input_path: Optional[Path]) -> None:
    """Error if interactive mode is used while reading data from stdin."""
    if mode == "interactive" and input_path is None:
        raise click.UsageError(
            "Cannot use interactive mode when reading from stdin. "
            "Use -p (prompt) or -e (expression)."
        )


def _resolve_worker_model(model: Optional[str], worker_model: Optional[str]):
    """Resolve the model used for per-item LLM work."""
    return resolve_model(worker_model or model)


def _resolve_planner_model(planning_model: Optional[str], model: Optional[str]):
    """Resolve the model used for interactive planning (one-shot)."""
    return resolve_model(planning_model or model)


# ---------------------------------------------------------------------------
# Map
# ---------------------------------------------------------------------------


class MapProcessor:
    name = "map"

    def register_cli(self, group: click.Group, context: PluginContext) -> None:
        @group.command(name=self.name)
        @_common_options
        @click.option(
            "-o",
            "--output",
            type=click.Path(dir_okay=False, path_type=Path),
            help="Output path. Required unless --in-place is provided.",
        )
        @click.option(
            "--in-place", is_flag=True, help="Overwrite the input file in place"
        )
        @click.option(
            "-c",
            "--column",
            "target_column",
            default="mr_result",
            show_default=True,
            help="Column to populate with LLM output",
        )
        @click.option(
            "--batch-size",
            type=click.IntRange(1),
            default=1,
            show_default=True,
            help="Number of rows per prompt",
        )
        @click.option(
            "--max-chars",
            type=click.IntRange(500, 200000),
            default=6000,
            show_default=True,
            help="Max characters per prompt batch",
        )
        @click.option(
            "--few-shot",
            type=click.IntRange(0),
            default=0,
            show_default=True,
            help="Use N existing values as few-shot examples",
        )
        @click.option(
            "--multiple",
            is_flag=True,
            help="Model emits a list per row; each item becomes its own row",
        )
        @click.option(
            "-j",
            "--parallel",
            type=click.IntRange(1),
            default=1,
            show_default=True,
            help="Concurrent batches",
        )
        @click.option(
            "--force",
            is_flag=True,
            help="Overwrite existing output file even if it doesn't match input",
        )
        @click.option(
            "--err",
            "err_file",
            type=click.Path(dir_okay=False, path_type=Path),
            default=None,
            help="Error sidecar path (default: <output>.err)",
        )
        def map_command(
            input_path: Optional[Path],
            instruction: str,
            prompt_mode: bool,
            expression_mode: bool,
            model: Optional[str],
            worker_model: Optional[str],
            planning_model: Optional[str],
            format_: Optional[str],
            input_format: Optional[str],
            output_format: Optional[str],
            filters: Sequence[str],
            limit: Optional[int],
            output: Optional[Path],
            in_place: bool,
            target_column: str,
            batch_size: int,
            max_chars: int,
            few_shot: int,
            multiple: bool,
            parallel: int,
            force: bool,
            err_file: Optional[Path],
            timeout: float,
            dry_run: bool,
            verbose: bool,
        ) -> None:
            if in_place and input_path is None:
                raise click.UsageError("--in-place requires -i <file>")
            output_path = input_path if in_place else output

            mode = _validate_mode_flags(prompt_mode, expression_mode)
            _stdin_guard(input_path)
            _interactive_stdin_guard(mode, input_path)

            in_fmt, out_fmt = _resolve_formats(
                input_path, output_path, format_, input_format, output_format
            )
            err_path = err_file if err_file is not None else _err_path_for(output_path)
            wal_path = _wal_path_for(output_path)

            parsed_filters = [parse_filter(expr) for expr in filters]

            # Streaming path: expression mode avoids full materialization
            if mode == "expression" and not in_place:
                deterministic = _compile_expression(instruction)
                write_output = _output_writer(context, output_path, out_fmt)
                with _open_input(context, input_path, in_fmt) as stream:
                    fieldnames = list(stream.fieldnames or [])
                    if target_column not in fieldnames:
                        fieldnames.append(target_column)
                    total, written = _stream_map_expression(
                        stream,
                        deterministic,
                        target_column,
                        parsed_filters,
                        limit,
                        multiple,
                        write_output,
                        fieldnames,
                    )
                click.echo(
                    f"Wrote {written} rows"
                    + (f" to {output_path}" if output_path else ""),
                    err=True,
                )
                return

            with _open_input(context, input_path, in_fmt) as stream:
                rows, fieldnames = _materialize(stream, limit=limit)

            target_rows = filter_rows(rows, parsed_filters)

            if target_column not in fieldnames:
                fieldnames.append(target_column)

            # --- Resume: load existing output + WAL ---
            resume_state: Optional[MapResumeState] = None
            existing_output: List[Dict[str, Any]] = []
            wal_records: List[Dict[str, Any]] = []
            output_typed = True

            if (
                output_path
                and output_path.exists()
                and output_path.stat().st_size > 0
                and not in_place
            ):
                if not force:
                    try:
                        output_input_plugin = context.inputs.for_path(output_path)
                        output_typed = output_input_plugin.typed
                        with output_input_plugin.open(output_path) as out_stream:
                            existing_output, _ = _materialize(out_stream)
                    except Exception:
                        existing_output = []

                    if existing_output:
                        resume_state = _match_map_output(
                            rows,
                            existing_output,
                            target_column,
                            multiple,
                            typed=output_typed,
                        )
                        if not resume_state.done_indices and not resume_state.passthrough_indices and existing_output:
                            raise click.ClickException(
                                f"{output_path} exists but does not match the input shape. "
                                "Use --force to overwrite, or remove the file and re-run."
                            )

                        if wal_path:
                            wal_records = _read_wal(wal_path)
                            wal_done: Set[int] = set()
                            for rec in wal_records:
                                wal_done.add(rec["i"])
                            resume_state.done_indices |= wal_done

            # Mark rows already done (from output or WAL) so they're skipped
            if resume_state:
                wal_by_idx: Dict[int, Any] = {rec["i"]: rec["v"] for rec in wal_records}
                output_val_by_idx: Dict[int, Any] = {}
                out_cursor = 0
                for in_idx, in_row in enumerate(rows):
                    if out_cursor < len(existing_output) and _is_superset(
                        existing_output[out_cursor], in_row, typed=output_typed
                    ):
                        out_row = existing_output[out_cursor]
                        if _has_value(out_row.get(target_column)):
                            output_val_by_idx[in_idx] = out_row[target_column]
                            out_cursor += 1
                        elif target_column not in out_row:
                            out_cursor += 1

                for idx in resume_state.done_indices:
                    if idx < len(rows):
                        if idx in wal_by_idx:
                            rows[idx][target_column] = wal_by_idx[idx]
                        elif idx in output_val_by_idx:
                            rows[idx][target_column] = output_val_by_idx[idx]

            use_incremental = False
            deterministic = None
            prompt_text = instruction
            if mode == "expression":
                deterministic = _compile_expression(instruction)
            elif mode == "interactive":
                if resume_state and (resume_state.done_indices or resume_state.passthrough_indices):
                    prompt_text = instruction
                else:
                    plan = _interactive_plan_map(
                        instruction,
                        target_column,
                        fieldnames,
                        planning_model,
                        model,
                        _extract_few_shot_examples(rows, target_column, few_shot),
                    )
                    if plan is None:
                        raise SystemExit(0)
                    elif isinstance(plan, str):
                        prompt_text = plan
                    else:
                        deterministic = plan

            row_id_map = {id(row): i for i, row in enumerate(rows)}
            processed_indices: Set[int] = set()

            if deterministic:
                for row in target_rows:
                    if _has_value(row.get(target_column)):
                        continue
                    try:
                        row[target_column] = deterministic.evaluate(row)
                        processed_indices.add(row_id_map[id(row)])
                    except Exception as exc:
                        raise click.ClickException(f"Expression failed: {exc}")
            else:
                worker = _resolve_worker_model(model, worker_model)
                _require_schema_support(worker)

                few_shot_examples = _extract_few_shot_examples(
                    rows, target_column, few_shot
                )
                pending = [
                    (row_id_map[id(row)], row)
                    for row in target_rows
                    if not _has_value(row.get(target_column))
                ]
                processed_indices = {i for i, _ in pending}
                if not pending:
                    click.echo(
                        "No rows required LLM processing; nothing to do", err=True
                    )
                    # Clean up WAL if all rows are done
                    _delete_wal(wal_path)
                else:
                    if dry_run:
                        _dry_run_map(
                            pending,
                            prompt_text,
                            target_column,
                            few_shot_examples,
                            multiple,
                            batch_size,
                            max_chars,
                        )
                        return

                    _clear_err_file(err_path)

                    # Determine if we can stream-append rows directly.
                    # Two cases qualify:
                    #  - Resuming with only tail rows (existing output ends
                    #    exactly at the last match, no gaps).
                    #  - Fresh run to a new/empty file with an appendable
                    #    format (ADR Case 1).
                    output_plugin = (
                        context.outputs.for_path(output_path) if output_path else None
                    )
                    can_append = output_path is not None and isinstance(
                        output_plugin, AppendableOutput
                    )
                    has_gaps = bool(resume_state and resume_state.gap_indices)
                    output_ends_at_match = resume_state is not None and len(
                        existing_output
                    ) == len(resume_state.done_indices) + len(resume_state.passthrough_indices)
                    is_fresh_file = (
                        resume_state is None
                        and output_path is not None
                        and (
                            not output_path.exists() or output_path.stat().st_size == 0
                        )
                    )
                    # For a fresh file, only use incremental if every row
                    # is pending (no interleaved already-done rows that
                    # would need to be written in order).
                    all_rows_pending = len(pending) == len(rows)
                    use_incremental = (
                        can_append
                        and not multiple
                        and (
                            (
                                resume_state is not None
                                and (resume_state.done_indices or resume_state.passthrough_indices)
                                and not has_gaps
                                and output_ends_at_match
                            )
                            or (is_fresh_file and all_rows_pending)
                        )
                    )

                    if use_incremental:
                        failed = _run_map_batches_incremental(
                            context,
                            pending,
                            worker,
                            prompt_text,
                            target_column,
                            few_shot_examples,
                            multiple,
                            batch_size,
                            max_chars,
                            parallel,
                            err_path,
                            output_path,
                            fieldnames,
                            wal_path,
                            verbose,
                            timeout=timeout,
                        )
                    else:
                        failed = _run_map_batches(
                            pending,
                            worker,
                            prompt_text,
                            target_column,
                            few_shot_examples,
                            multiple,
                            batch_size,
                            max_chars,
                            parallel,
                            err_path,
                            verbose,
                            wal_path=wal_path,
                            timeout=timeout,
                        )

                    if failed:
                        click.echo(
                            f"Warning: {failed} batches failed"
                            + (
                                f"; see {err_path} — rerun to retry" if err_path else ""
                            ),
                            err=True,
                        )

            if multiple:
                rows = _expand_multiple_rows(rows, target_column, processed_indices)

            # Write final output.
            # If we used incremental append, rows were already appended to
            # the output file — just clean up the WAL.
            # Otherwise, write the full output now.
            did_incremental = (
                not deterministic and processed_indices and use_incremental
            )

            if did_incremental:
                _delete_wal(wal_path)
                click.echo(f"Wrote {len(rows)} rows to {output_path}", err=True)
            elif output_path is not None:
                _write_via_temp_swap(context, output_path, out_fmt, rows, fieldnames)
                _delete_wal(wal_path)
                click.echo(f"Wrote {len(rows)} rows to {output_path}", err=True)
            else:
                write_output = _output_writer(context, output_path, out_fmt)
                write_output(rows, fieldnames)
                _delete_wal(wal_path)
                click.echo(f"Wrote {len(rows)} rows", err=True)

            if not deterministic and processed_indices:
                pending_rows = [
                    row for i, row in enumerate(rows) if i in processed_indices
                ]
                if pending_rows:
                    n_batches = len(
                        _prepare_batches(pending_rows, batch_size, max_chars)
                    )
                    _echo_log_tip(n_batches)


# ---------------------------------------------------------------------------
# Reduce
# ---------------------------------------------------------------------------


class ReduceProcessor:
    name = "reduce"

    def register_cli(self, group: click.Group, context: PluginContext) -> None:
        @group.command(name=self.name)
        @_common_options
        @click.option(
            "--group-by",
            "group_by",
            multiple=True,
            required=True,
            help="Column(s) to group by",
        )
        @click.option(
            "-o",
            "--output",
            type=click.Path(dir_okay=False, path_type=Path),
            default=None,
            help="Destination path for reduced output (default: stdout)",
        )
        @click.option(
            "-c",
            "--column",
            "result_column",
            default="mr_result",
            show_default=True,
            help="Column name for reduced value",
        )
        @click.option(
            "--group-key-column",
            "group_key_column",
            default="group",
            show_default=True,
            help="Column name for the group key in reduced output",
        )
        @click.option(
            "--max-chars",
            type=click.IntRange(500, 200000),
            default=8000,
            show_default=True,
            help="Max characters per reduction prompt",
        )
        @click.option(
            "-j",
            "--parallel",
            type=click.IntRange(1),
            default=1,
            show_default=True,
            help="Concurrent groups",
        )
        @click.option(
            "--force",
            is_flag=True,
            help="Overwrite existing output file even if it doesn't match",
        )
        @click.option(
            "--err",
            "err_file",
            type=click.Path(dir_okay=False, path_type=Path),
            default=None,
            help="Error sidecar path (default: <output>.err)",
        )
        def reduce_command(
            input_path: Optional[Path],
            instruction: str,
            prompt_mode: bool,
            expression_mode: bool,
            model: Optional[str],
            worker_model: Optional[str],
            planning_model: Optional[str],
            format_: Optional[str],
            input_format: Optional[str],
            output_format: Optional[str],
            filters: Sequence[str],
            limit: Optional[int],
            group_by: Sequence[str],
            output: Optional[Path],
            result_column: str,
            group_key_column: str,
            max_chars: int,
            parallel: int,
            force: bool,
            err_file: Optional[Path],
            timeout: float,
            dry_run: bool,
            verbose: bool,
        ) -> None:
            mode = _validate_mode_flags(prompt_mode, expression_mode)
            _stdin_guard(input_path)
            _interactive_stdin_guard(mode, input_path)

            if group_key_column == result_column:
                raise click.ClickException(
                    "--group-key-column and --column must differ (both would name the same output column)"
                )

            in_fmt, out_fmt = _resolve_formats(
                input_path, output, format_, input_format, output_format
            )
            err_path = err_file if err_file is not None else _err_path_for(output)
            wal_path = _wal_path_for(output)

            with _open_input(context, input_path, in_fmt) as stream:
                rows, fieldnames = _materialize(stream)

            for col in group_by:
                if col not in fieldnames:
                    raise click.ClickException(
                        f"Column '{col}' specified in --group-by does not exist. "
                        f"Available columns: {', '.join(fieldnames)}"
                    )

            parsed_filters = [parse_filter(expr) for expr in filters]
            filtered_rows = filter_rows(rows, parsed_filters)

            groups = _group_rows(filtered_rows, group_by)
            if limit is not None:
                groups = dict(list(groups.items())[:limit])

            # --- Resume: load existing output + WAL ---
            resume_state: Optional[ReduceResumeState] = None
            existing_results: List[Dict[str, Any]] = []

            if output and output.exists() and output.stat().st_size > 0 and not force:
                try:
                    output_input_plugin = context.inputs.for_path(output)
                    with output_input_plugin.open(output) as out_stream:
                        existing_results, _ = _materialize(out_stream)
                except Exception:
                    existing_results = []

                if existing_results:
                    resume_state = _match_reduce_output(
                        existing_results, group_key_column, result_column
                    )
                    if wal_path:
                        for rec in _read_wal(wal_path):
                            key = rec.get("g")
                            if key is not None:
                                resume_state.done_keys.add(str(key))
                                resume_state.done_rows.append(
                                    {group_key_column: key, result_column: rec["v"]}
                                )

            deterministic = None
            prompt_text = instruction
            if mode == "expression":
                deterministic = _compile_reduce_expression(instruction)
            elif mode == "interactive":
                if resume_state and resume_state.done_keys:
                    prompt_text = instruction
                else:
                    sample_group = next(iter(groups.values()), [])
                    plan = _interactive_plan_reduce(
                        instruction,
                        result_column,
                        fieldnames,
                        planning_model,
                        model,
                        sample_group,
                    )
                    if plan is None:
                        raise SystemExit(0)
                    elif isinstance(plan, str):
                        prompt_text = plan
                    else:
                        deterministic = plan

            if deterministic:
                results: List[Dict[str, Any]] = []
                for group_key, group_rows in groups.items():
                    try:
                        value = deterministic.evaluate(group_rows)
                    except Exception as exc:
                        raise click.ClickException(
                            f"Expression failed on group '{group_key}': {exc}"
                        )
                    results.append({group_key_column: group_key, result_column: value})
                click.echo(f"Reduced {len(results)} groups deterministically", err=True)
            else:
                worker = _resolve_worker_model(model, worker_model)
                _require_schema_support(worker)

                # Filter out already-done groups
                if resume_state:
                    pending_groups = [
                        (k, v)
                        for k, v in groups.items()
                        if str(k) not in resume_state.done_keys
                    ]
                else:
                    pending_groups = list(groups.items())

                if not pending_groups:
                    click.echo(
                        "No groups required LLM processing; nothing to do", err=True
                    )
                    results = existing_results if existing_results else []
                    _delete_wal(wal_path)
                else:
                    if dry_run:
                        _dry_run_reduce(
                            pending_groups,
                            prompt_text,
                            group_by,
                            max_chars,
                            result_column,
                        )
                        return

                    click.echo(
                        f"Reducing {len(pending_groups)} groups (parallel={parallel})",
                        err=True,
                    )
                    _clear_err_file(err_path)
                    new_results, failed = _run_reduce_groups(
                        pending_groups,
                        worker,
                        prompt_text,
                        group_by,
                        max_chars,
                        result_column,
                        group_key_column,
                        parallel,
                        err_path,
                        verbose,
                        wal_path=wal_path,
                        timeout=timeout,
                    )
                    if failed:
                        click.echo(
                            f"Warning: {failed}/{len(pending_groups)} groups failed"
                            + (
                                f"; see {err_path} — rerun to retry" if err_path else ""
                            ),
                            err=True,
                        )

                    # Merge with existing results
                    if resume_state and existing_results:
                        done_keys = {
                            str(r.get(group_key_column)) for r in existing_results
                        }
                        results = list(existing_results)
                        for r in new_results:
                            if str(r.get(group_key_column)) not in done_keys:
                                results.append(r)
                    else:
                        results = new_results

            result_fieldnames = [group_key_column, result_column]
            if output is not None:
                _write_via_temp_swap(
                    context, output, out_fmt, results, result_fieldnames
                )
            else:
                write_output = _output_writer(context, output, out_fmt)
                write_output(results, result_fieldnames)
            _delete_wal(wal_path)
            click.echo(
                f"Wrote {len(results)} group results"
                + (f" to {output}" if output else ""),
                err=True,
            )
            if not deterministic and pending_groups:
                _echo_log_tip(len(pending_groups))


# ---------------------------------------------------------------------------
# Filter
# ---------------------------------------------------------------------------


class FilterProcessor:
    name = "filter"

    def register_cli(self, group: click.Group, context: PluginContext) -> None:
        @group.command(name=self.name)
        @_common_options
        @click.option(
            "-o",
            "--output",
            type=click.Path(dir_okay=False, path_type=Path),
            default=None,
            help="Output path for filtered rows (default: stdout)",
        )
        @click.option(
            "--batch-size",
            type=click.IntRange(1),
            default=1,
            show_default=True,
            help="Number of rows per prompt",
        )
        @click.option(
            "--max-chars",
            type=click.IntRange(500, 200000),
            default=6000,
            show_default=True,
            help="Max characters per prompt batch",
        )
        @click.option(
            "-j",
            "--parallel",
            type=click.IntRange(1),
            default=1,
            show_default=True,
            help="Concurrent batches",
        )
        @click.option(
            "--force",
            is_flag=True,
            help="Overwrite existing output file even if it doesn't match",
        )
        @click.option(
            "--err",
            "err_file",
            type=click.Path(dir_okay=False, path_type=Path),
            default=None,
            help="Error sidecar path (default: <output>.err)",
        )
        def filter_command(
            input_path: Optional[Path],
            instruction: str,
            prompt_mode: bool,
            expression_mode: bool,
            model: Optional[str],
            worker_model: Optional[str],
            planning_model: Optional[str],
            format_: Optional[str],
            input_format: Optional[str],
            output_format: Optional[str],
            filters: Sequence[str],
            limit: Optional[int],
            output: Optional[Path],
            batch_size: int,
            max_chars: int,
            parallel: int,
            force: bool,
            err_file: Optional[Path],
            timeout: float,
            dry_run: bool,
            verbose: bool,
        ) -> None:
            mode = _validate_mode_flags(prompt_mode, expression_mode)
            _stdin_guard(input_path)
            _interactive_stdin_guard(mode, input_path)

            in_fmt, out_fmt = _resolve_formats(
                input_path, output, format_, input_format, output_format
            )
            err_path = err_file if err_file is not None else _err_path_for(output)
            wal_path = _wal_path_for(output)

            parsed_filters = [parse_filter(expr) for expr in filters]

            # Streaming path: expression mode avoids full materialization
            if mode == "expression":
                deterministic = _compile_expression(instruction)
                write_output = _output_writer(context, output, out_fmt)
                with _open_input(context, input_path, in_fmt) as stream:
                    fieldnames = list(stream.fieldnames or [])
                    candidates, kept_count = _stream_filter_expression(
                        stream,
                        deterministic,
                        parsed_filters,
                        limit,
                        write_output,
                        fieldnames,
                    )
                click.echo(
                    f"Kept {kept_count}/{candidates} rows"
                    + (f" → {output}" if output else ""),
                    err=True,
                )
                return

            with _open_input(context, input_path, in_fmt) as stream:
                rows, fieldnames = _materialize(stream, limit=limit)

            rows = filter_rows(rows, parsed_filters)

            # --- Resume: load existing output + WAL + .err ---
            resume_state: Optional[FilterResumeState] = None

            if output and output.exists() and output.stat().st_size > 0 and not force:
                output_typed = True
                try:
                    output_input_plugin = context.inputs.for_path(output)
                    output_typed = output_input_plugin.typed
                    with output_input_plugin.open(output) as out_stream:
                        existing_output, _ = _materialize(out_stream)
                except Exception:
                    existing_output = []

                if existing_output:
                    err_records = _read_errors(err_path) if err_path else []
                    wal_records = _read_wal(wal_path) if wal_path else []

                    resume_state = _match_filter_output(
                        rows, existing_output, err_records, typed=output_typed
                    )

                    # Incorporate WAL records
                    for rec in wal_records:
                        idx = rec["i"]
                        if rec.get("kept"):
                            resume_state.done_indices.add(idx)
                            resume_state.kept_indices.add(idx)
                        else:
                            resume_state.done_indices.add(idx)
                            resume_state.filtered_out_indices.add(idx)

            deterministic = None
            prompt_text = instruction
            if mode == "interactive":
                if resume_state and resume_state.done_indices:
                    prompt_text = instruction
                else:
                    plan = _interactive_plan_filter(
                        instruction,
                        fieldnames,
                        planning_model,
                        model,
                        rows[:3],
                    )
                    if plan is None:
                        raise SystemExit(0)
                    elif isinstance(plan, str):
                        prompt_text = plan
                    else:
                        deterministic = plan

            if deterministic:
                kept = [row for row in rows if deterministic.evaluate(row)]
            else:
                worker = _resolve_worker_model(model, worker_model)
                _require_schema_support(worker)

                # Filter out already-done rows
                if resume_state:
                    pending_rows = [
                        row
                        for i, row in enumerate(rows)
                        if i not in resume_state.done_indices
                    ]
                    pending_indices = [
                        i
                        for i in range(len(rows))
                        if i not in resume_state.done_indices
                    ]
                else:
                    pending_rows = rows
                    pending_indices = list(range(len(rows)))

                if not pending_rows:
                    click.echo(
                        "No rows required LLM processing; nothing to do", err=True
                    )
                    kept = (
                        [rows[i] for i in sorted(resume_state.kept_indices)]
                        if resume_state
                        else []
                    )
                    _delete_wal(wal_path)
                else:
                    if dry_run:
                        _dry_run_filter(
                            pending_rows, prompt_text, batch_size, max_chars
                        )
                        return

                    _clear_err_file(err_path)

                    new_kept = _run_filter_llm(
                        pending_rows,
                        worker,
                        prompt_text,
                        batch_size,
                        max_chars,
                        parallel,
                        verbose,
                        err_path=err_path,
                        row_indices=pending_indices,
                        wal_path=wal_path,
                        timeout=timeout,
                    )

                    # Merge with already-kept rows from resume
                    if resume_state:
                        kept_set = set(resume_state.kept_indices)
                        new_kept_set = {id(r) for r in new_kept}
                        for i, row in enumerate(pending_rows):
                            if id(row) in new_kept_set:
                                kept_set.add(pending_indices[i])
                        kept = [rows[i] for i in sorted(kept_set)]
                    else:
                        kept = new_kept

            if output is not None:
                _write_via_temp_swap(context, output, out_fmt, kept, fieldnames)
            else:
                write_output = _output_writer(context, output, out_fmt)
                write_output(kept, fieldnames)
            _delete_wal(wal_path)
            click.echo(
                f"Kept {len(kept)}/{len(rows)} rows"
                + (f" → {output}" if output else ""),
                err=True,
            )
            if not deterministic and pending_rows:
                n_batches = len(_prepare_batches(pending_rows, batch_size, max_chars))
                _echo_log_tip(n_batches)


# ---------------------------------------------------------------------------
# Helpers — model resolution
# ---------------------------------------------------------------------------


def resolve_model(model_name: Optional[str]):
    model_id = model_name or get_default_model()
    model_obj = llm.get_model(model_id)
    if model_obj.needs_key:
        model_obj.key = llm.get_key("", model_obj.needs_key, model_obj.key_env_var)
    return model_obj


def _require_schema_support(model_obj) -> None:
    if not getattr(model_obj, "supports_schema", False):
        raise click.ClickException(
            f"Model '{model_obj.model_id}' does not support schemas. "
            f"Please use a model that supports structured output. "
            f"Run llm models --schemas to see which models support schemas."
        )


# ---------------------------------------------------------------------------
# Deterministic expressions
# ---------------------------------------------------------------------------

_EXPR_BUILTINS = {
    "__builtins__": {},
    "len": len,
    "int": int,
    "float": float,
    "str": str,
    "min": min,
    "max": max,
    "abs": abs,
    "round": round,
    "sorted": sorted,
    "bool": bool,
    "list": list,
    "tuple": tuple,
    "set": set,
    "dict": dict,
    "sum": sum,
    "any": any,
    "all": all,
    "enumerate": enumerate,
    "zip": zip,
    "map": map,
    "filter": filter,
}


@dataclass
class DeterministicExpression:
    expression: str
    code: Any

    def evaluate(self, row: Dict[str, Any]) -> Any:
        return eval(self.code, dict(_EXPR_BUILTINS), {"row": row})


@dataclass
class DeterministicReduceExpression:
    expression: str
    code: Any

    def evaluate(self, rows: List[Dict[str, Any]]) -> Any:
        return eval(self.code, dict(_EXPR_BUILTINS), {"rows": rows})


def _compile_expression(expression: str) -> DeterministicExpression:
    try:
        code = compile(expression, "<expression>", "eval")
    except SyntaxError as exc:
        raise click.ClickException(f"Invalid Python expression: {exc}")
    return DeterministicExpression(expression=expression, code=code)


def _compile_reduce_expression(expression: str) -> DeterministicReduceExpression:
    try:
        code = compile(expression, "<expression>", "eval")
    except SyntaxError as exc:
        raise click.ClickException(f"Invalid Python expression: {exc}")
    return DeterministicReduceExpression(expression=expression, code=code)


# ---------------------------------------------------------------------------
# Interactive planning
# ---------------------------------------------------------------------------

_PLAN_INSTRUCTIONS = (
    "Return ONLY a JSON object. "
    'If the task can be done deterministically, set "deterministic" to true '
    'and provide a Python "expression". '
    'If it requires LLM reasoning, set "deterministic" to false '
    'and provide a "prompt" — a clear, specific instruction to send to an LLM '
    "for each item."
)


def _interactive_plan_map(
    instruction: str,
    target_column: str,
    fieldnames: Sequence[str],
    planning_model: Optional[str],
    model_name: Optional[str],
    few_shot_examples: Sequence[Tuple[Dict[str, Any], Any]],
) -> Union[DeterministicExpression, str, None]:
    try:
        model_obj = _resolve_planner_model(planning_model, model_name)
    except Exception:
        return None
    system = "\n".join(
        [
            "You translate spreadsheet instructions into either a deterministic Python "
            "expression or a per-row LLM prompt.",
            "Columns available: " + ", ".join(fieldnames),
            f"Target column: '{target_column}'.",
            _PLAN_INSTRUCTIONS,
            "When deterministic is true, 'expression' must be a Python expression "
            "using the variable 'row' (a dict).",
            "Available builtins: len, int, float, str, min, max, abs, round, sorted, "
            "bool, list, tuple, set, dict, sum, any, all, enumerate, zip, map, filter.",
        ]
    )
    payload = {
        "instruction": instruction,
        "examples": [
            {"input": row, "output": value} for row, value in few_shot_examples
        ],
    }
    response = model_obj.prompt(system + "\n" + json.dumps(payload, ensure_ascii=False))
    return _confirm_plan(response, instruction, "row")


def _interactive_plan_reduce(
    instruction: str,
    result_column: str,
    fieldnames: Sequence[str],
    planning_model: Optional[str],
    model_name: Optional[str],
    sample_rows: List[Dict[str, Any]],
) -> Union[DeterministicReduceExpression, str, None]:
    try:
        model_obj = _resolve_planner_model(planning_model, model_name)
    except Exception:
        return None
    system = "\n".join(
        [
            "You translate spreadsheet reduction instructions into either a deterministic "
            "Python expression or a per-group LLM prompt.",
            "Columns available: " + ", ".join(fieldnames),
            f"Result column: '{result_column}'.",
            _PLAN_INSTRUCTIONS,
            "When deterministic is true, 'expression' must be a Python expression "
            "using the variable 'rows' (a list of dicts).",
            "Available builtins: len, int, float, str, min, max, abs, round, sorted, "
            "bool, list, tuple, set, dict, sum, any, all, enumerate, zip, map, filter.",
        ]
    )
    payload = {
        "instruction": instruction,
        "sample_rows": sample_rows[:3],
    }
    response = model_obj.prompt(system + "\n" + json.dumps(payload, ensure_ascii=False))
    return _confirm_plan(response, instruction, "rows", reduce=True)


def _interactive_plan_filter(
    instruction: str,
    fieldnames: Sequence[str],
    planning_model: Optional[str],
    model_name: Optional[str],
    sample_rows: List[Dict[str, Any]],
) -> Union[DeterministicExpression, str, None]:
    try:
        model_obj = _resolve_planner_model(planning_model, model_name)
    except Exception:
        return None
    system = "\n".join(
        [
            "You translate natural language filter descriptions into either a deterministic "
            "Python expression or a per-row LLM prompt.",
            "Columns available: " + ", ".join(fieldnames),
            "The expression should return True for rows that should be KEPT.",
            _PLAN_INSTRUCTIONS,
            "When deterministic is true, 'expression' must be a Python expression "
            "using the variable 'row' (a dict).",
            "Available builtins: len, int, float, str, min, max, abs, round, sorted, "
            "bool, list, tuple, set, dict, sum, any, all, enumerate, zip, map, filter.",
        ]
    )
    payload = {
        "instruction": instruction,
        "sample_rows": sample_rows[:3],
    }
    response = model_obj.prompt(system + "\n" + json.dumps(payload, ensure_ascii=False))
    return _confirm_plan(response, instruction, "row")


def _confirm_plan(
    response,
    instruction: str,
    var_name: str,
    reduce: bool = False,
) -> Union[DeterministicExpression, DeterministicReduceExpression, str, None]:
    """Parse the planner response, confirm with the user, and return the result.

    Returns a DeterministicExpression/DeterministicReduceExpression if the user
    accepts an expression, a str prompt if the user accepts a prompt, or None
    if the user declines.
    """
    try:
        data = json.loads(str(response))
    except json.JSONDecodeError:
        data = {}

    expression = data.get("expression") if data.get("deterministic") else None
    suggested_prompt = data.get("prompt")

    if isinstance(expression, str):
        try:
            code = compile(expression, "<deterministic>", "eval")
        except SyntaxError:
            expression = None
        else:
            if click.confirm(
                f"Use deterministic expression?\n  {expression}", default=True
            ):
                click.echo(f"Using deterministic expression: {expression}", err=True)
                if reduce:
                    return DeterministicReduceExpression(
                        expression=expression, code=code
                    )
                return DeterministicExpression(expression=expression, code=code)

    item_label = "group" if reduce else "row"
    prompt_text = suggested_prompt if isinstance(suggested_prompt, str) else instruction
    if not click.confirm(
        f"Run as prompt per {item_label}?\n  {prompt_text}", default=True
    ):
        return None
    return prompt_text


# ---------------------------------------------------------------------------
# Timeout helper
# ---------------------------------------------------------------------------


def _submit_and_wait(fn, args, timeout):
    """Run *fn(*args)* in a thread with an optional timeout.

    Used by the sequential (parallel=1) paths so that a single hung LLM call
    doesn't block forever.  The parallel paths use ``future.result(timeout=…)``
    directly on the executor.
    """
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(fn, *args)
        return future.result(timeout=timeout)


# ---------------------------------------------------------------------------
# Filter via LLM
# ---------------------------------------------------------------------------


def _run_filter_llm(
    rows: List[Dict[str, Any]],
    model_obj,
    instruction: str,
    batch_size: int,
    max_chars: int,
    parallel: int,
    verbose: bool = False,
    err_path: Optional[Path] = None,
    row_indices: Optional[List[int]] = None,
    wal_path: Optional[Path] = None,
    timeout: Optional[float] = None,
) -> List[Dict[str, Any]]:
    """Classify each row with the LLM and keep rows where result is truthy."""
    batches = _prepare_batches(rows, batch_size, max_chars)

    if row_indices is None:
        row_indices = list(range(len(rows)))

    idx_offset = 0
    index_batches: List[List[int]] = []
    for batch in batches:
        index_batches.append(row_indices[idx_offset : idx_offset + len(batch)])
        idx_offset += len(batch)

    def _process_one(batch):
        prompt_text = _build_filter_prompt(instruction, batch)
        schema = _build_filter_schema(batch)
        if verbose:
            _echo_verbose(prompt_text)
        response = model_obj.prompt(prompt_text, schema=schema)
        return _parse_filter_response(response.text(), len(batch))

    results: List[bool] = []
    if parallel == 1:
        for i, (batch, indices) in enumerate(zip(batches, index_batches)):
            try:
                verdicts = _submit_and_wait(_process_one, (batch,), timeout)
            except Exception as exc:
                _append_error(err_path, {"row_indices": indices, "error": str(exc)})
                results.extend([False] * len(batch))
                click.echo(f"  Batch {i + 1}/{len(batches)} failed: {exc}", err=True)
                continue
            results.extend(verdicts)
            if wal_path:
                for idx, kept in zip(indices, verdicts):
                    _append_wal(wal_path, {"i": idx, "kept": kept})
            click.echo(f"  Filtered batch {i + 1}/{len(batches)}", err=True)
    else:
        batch_verdicts: Dict[int, List[bool]] = {}
        with ThreadPoolExecutor(max_workers=parallel) as executor:
            futures = {
                executor.submit(_process_one, batch): (i, indices)
                for i, (batch, indices) in enumerate(zip(batches, index_batches))
            }
            for future in as_completed(futures):
                idx, indices = futures[future]
                try:
                    verdicts = future.result(timeout=timeout)
                    batch_verdicts[idx] = verdicts
                except Exception as exc:
                    _append_error(err_path, {"row_indices": indices, "error": str(exc)})
                    batch_verdicts[idx] = [False] * len(indices)
                    click.echo(
                        f"  Batch {idx + 1}/{len(batches)} failed: {exc}", err=True
                    )
                    continue
                if wal_path:
                    for row_idx, kept in zip(indices, verdicts):
                        _append_wal(wal_path, {"i": row_idx, "kept": kept})
                click.echo(f"  Filtered batch {idx + 1}/{len(batches)}", err=True)
        for i in range(len(batches)):
            results.extend(batch_verdicts.get(i, [False] * len(batches[i])))

    return [row for row, keep in zip(rows, results) if keep]


def _build_filter_prompt(instruction: str, rows: Sequence[Dict[str, Any]]) -> str:
    lines = [
        "You are filtering spreadsheet rows based on a criterion.",
        "<spreadsheet_rows>",
    ]
    for i, row in enumerate(rows):
        lines.append(f"<row_{i}>")
        lines.append(json.dumps(row, ensure_ascii=False))
        lines.append(f"</row_{i}>")
    lines.append("</spreadsheet_rows>")
    lines.append("<filter_criterion>")
    lines.append(instruction)
    lines.append("</filter_criterion>")
    lines.append(
        "For each row, respond with 'keep' if the row matches the criterion, "
        "or 'discard' if it does not."
    )
    return "\n".join(lines)


def _build_filter_schema(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    properties = {}
    required = []
    for i in range(len(rows)):
        row_key = f"row_{i}"
        required.append(row_key)
        properties[row_key] = {
            "type": "object",
            "properties": {
                "verdict": {
                    "type": "string",
                    "enum": ["keep", "discard"],
                }
            },
            "required": ["verdict"],
            "additionalProperties": False,
        }
    return {
        "type": "object",
        "properties": properties,
        "required": required,
        "title": "FilterResponse",
    }


def _parse_filter_response(response: str, expected: int) -> List[bool]:
    response = response.strip()
    if not response:
        raise click.ClickException("Model returned an empty filter response")
    try:
        data = json.loads(response)
    except json.JSONDecodeError as exc:
        raise click.ClickException(
            f"Filter response was not valid JSON: {response}"
        ) from exc
    verdicts = []
    for i in range(expected):
        row_key = f"row_{i}"
        if row_key not in data:
            raise click.ClickException(f"Missing '{row_key}' in filter response")
        verdict = data[row_key].get("verdict", "").lower()
        verdicts.append(verdict == "keep")
    return verdicts


# ---------------------------------------------------------------------------
# Reduce helpers
# ---------------------------------------------------------------------------


def _run_reduce_groups(
    group_items: List[Tuple[str, List[Dict[str, Any]]]],
    model_obj,
    prompt: str,
    group_by: Sequence[str],
    max_chars: int,
    result_column: str,
    group_key_column: str,
    parallel: int,
    err_path: Path,
    verbose: bool = False,
    wal_path: Optional[Path] = None,
    timeout: Optional[float] = None,
) -> Tuple[List[Dict[str, Any]], int]:
    def _reduce_one(group_key, group_rows):
        value = _reduce_rows(
            model_obj, prompt, group_rows, group_by, max_chars, result_column, verbose
        )
        return {group_key_column: group_key, result_column: value}

    results: List[Dict[str, Any]] = []
    failed = 0

    if parallel == 1:
        for i, (group_key, group_rows) in enumerate(group_items):
            try:
                result = _submit_and_wait(
                    _reduce_one, (group_key, group_rows), timeout
                )
                results.append(result)
            except Exception as exc:
                failed += 1
                _append_error(err_path, {"group_key": group_key, "error": str(exc)})
                click.echo(
                    f"  Group {i + 1}/{len(group_items)} failed: {exc}", err=True
                )
                continue
            if wal_path:
                _append_wal(
                    wal_path,
                    {"g": group_key, "c": result_column, "v": result[result_column]},
                )
            click.echo(f"  Completed group {i + 1}/{len(group_items)}", err=True)
    else:
        with ThreadPoolExecutor(max_workers=parallel) as executor:
            futures = {
                executor.submit(_reduce_one, gk, gr): (i, gk)
                for i, (gk, gr) in enumerate(group_items)
            }
            completed = 0
            for future in as_completed(futures):
                group_idx, group_key = futures[future]
                try:
                    result = future.result(timeout=timeout)
                    results.append(result)
                except Exception as exc:
                    failed += 1
                    _append_error(err_path, {"group_key": group_key, "error": str(exc)})
                    click.echo(
                        f"  Group {group_idx + 1}/{len(group_items)} failed: {exc}",
                        err=True,
                    )
                    continue
                if wal_path:
                    _append_wal(
                        wal_path,
                        {
                            "g": group_key,
                            "c": result_column,
                            "v": result[result_column],
                        },
                    )
                completed += 1
                click.echo(
                    f"  Completed group {completed}/{len(group_items)}", err=True
                )

    return results, failed


# ---------------------------------------------------------------------------
# Helpers — parsing, filtering, batching
# ---------------------------------------------------------------------------


def _coerce_numeric(value: Any) -> Any:
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, str):
        value = value.strip()
        if not value:
            raise ValueError("Empty string cannot be coerced")
        try:
            if "." in value:
                return float(value)
            return int(value)
        except ValueError as exc:
            raise ValueError("Not numeric") from exc
    return value


def parse_filter(expr: str) -> FilterCondition:
    for op in ("<=", ">=", "!=", "=", "<", ">"):
        if op in expr:
            column, value = expr.split(op, 1)
            return FilterCondition(column.strip(), op, value.strip())
    raise click.BadParameter(
        "Filters must look like column=value or column>=10", ctx=None, param=None
    )


def filter_rows(
    rows: List[Dict[str, Any]], filters: Sequence[FilterCondition]
) -> List[Dict[str, Any]]:
    if not filters:
        return rows
    return [row for row in rows if all(f.matches(row) for f in filters)]


def _has_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip() != ""
    return True


def _err_path_for(output_path: Optional[Path]) -> Optional[Path]:
    if output_path is None:
        return None
    return Path(str(output_path) + ".err")


def _wal_path_for(output_path: Optional[Path]) -> Optional[Path]:
    if output_path is None:
        return None
    return Path(str(output_path) + ".wal")


# ---------------------------------------------------------------------------
# WAL (write-ahead log) infrastructure
# ---------------------------------------------------------------------------


def _append_wal(wal_path: Path, record: Dict[str, Any]) -> None:
    with wal_path.open("a", encoding="utf-8") as fp:
        fp.write(json.dumps(record, ensure_ascii=False) + "\n")


def _read_wal(wal_path: Path) -> List[Dict[str, Any]]:
    if not wal_path.exists():
        return []
    records: List[Dict[str, Any]] = []
    with wal_path.open("r", encoding="utf-8") as fp:
        for line in fp:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return records


def _delete_wal(wal_path: Optional[Path]) -> None:
    if wal_path is not None and wal_path.exists():
        wal_path.unlink()


# ---------------------------------------------------------------------------
# Resume: superset matching
# ---------------------------------------------------------------------------


def _is_superset(
    output_row: Dict[str, Any],
    input_row: Dict[str, Any],
    typed: bool = True,
) -> bool:
    """True if output_row contains all key-value pairs from input_row.

    When *typed* is True (the output was read from a type-preserving
    format like JSONL), values are compared with strict equality only.

    When *typed* is False (the output was read from a string-only format
    like CSV), values that differ in type but match after str() coercion
    are considered equal — e.g. int 1 matches string "1".
    """
    for key, value in input_row.items():
        if key not in output_row:
            return False
        out_val = output_row[key]
        if out_val == value:
            continue
        if typed:
            return False
        if out_val is None or value is None:
            return False
        if str(out_val) == str(value):
            continue
        return False
    return True


@dataclass
class MapResumeState:
    """Result of walking input + output to determine resume state for map."""

    done_indices: Set[int] = field(default_factory=set)
    passthrough_indices: Set[int] = field(default_factory=set)
    done_rows: List[Dict[str, Any]] = field(default_factory=list)
    gap_indices: List[int] = field(default_factory=list)
    tail_start: int = 0
    last_match_index: int = -1
    output_rows: List[Dict[str, Any]] = field(default_factory=list)


def _match_map_output(
    input_rows: List[Dict[str, Any]],
    output_rows: List[Dict[str, Any]],
    target_column: str,
    multiple: bool = False,
    typed: bool = True,
) -> MapResumeState:
    """Walk input and output in tandem using superset matching for map resume.

    Returns a MapResumeState with done/gap/tail classification.
    """
    state = MapResumeState(output_rows=output_rows)
    out_idx = 0

    for in_idx, in_row in enumerate(input_rows):
        if out_idx >= len(output_rows):
            break

        out_row = output_rows[out_idx]
        if _is_superset(out_row, in_row, typed=typed) and _has_value(
            out_row.get(target_column)
        ):
            state.done_indices.add(in_idx)
            state.done_rows.append(out_row)
            state.last_match_index = in_idx
            out_idx += 1

            if multiple:
                while out_idx < len(output_rows) and _is_superset(
                    output_rows[out_idx], in_row, typed=typed
                ):
                    state.done_rows.append(output_rows[out_idx])
                    out_idx += 1
        elif (
            _is_superset(out_row, in_row, typed=typed)
            and target_column not in out_row
        ):
            state.passthrough_indices.add(in_idx)
            state.last_match_index = in_idx
            out_idx += 1

    for in_idx in range(len(input_rows)):
        if in_idx not in state.done_indices and in_idx not in state.passthrough_indices and in_idx <= state.last_match_index:
            state.gap_indices.append(in_idx)

    state.tail_start = state.last_match_index + 1
    return state


@dataclass
class ReduceResumeState:
    done_keys: Set[str] = field(default_factory=set)
    done_rows: List[Dict[str, Any]] = field(default_factory=list)


def _match_reduce_output(
    output_rows: List[Dict[str, Any]],
    group_key_column: str,
    result_column: str,
) -> ReduceResumeState:
    """Match existing reduce output by group key."""
    state = ReduceResumeState()
    for row in output_rows:
        key = row.get(group_key_column)
        if key is not None and _has_value(row.get(result_column)):
            state.done_keys.add(str(key))
            state.done_rows.append(row)
    return state


@dataclass
class FilterResumeState:
    done_indices: Set[int] = field(default_factory=set)
    kept_indices: Set[int] = field(default_factory=set)
    filtered_out_indices: Set[int] = field(default_factory=set)
    errored_indices: Set[int] = field(default_factory=set)
    output_rows: List[Dict[str, Any]] = field(default_factory=list)
    last_match_input_index: int = -1


def _match_filter_output(
    input_rows: List[Dict[str, Any]],
    output_rows: List[Dict[str, Any]],
    err_records: List[Dict[str, Any]],
    typed: bool = True,
) -> FilterResumeState:
    """Walk input and output for filter resume.

    Uses .err to distinguish 'filtered out' from 'errored' for rows
    before the last match.
    """
    state = FilterResumeState(output_rows=output_rows)

    errored_indices: Set[int] = set()
    for record in err_records:
        for idx in record.get("row_indices", []):
            errored_indices.add(idx)
    state.errored_indices = errored_indices

    out_idx = 0
    for in_idx, in_row in enumerate(input_rows):
        if out_idx < len(output_rows):
            out_row = output_rows[out_idx]
            if out_row == in_row or _is_superset(out_row, in_row, typed=typed):
                state.done_indices.add(in_idx)
                state.kept_indices.add(in_idx)
                state.last_match_input_index = in_idx
                out_idx += 1
                continue

    for in_idx in range(len(input_rows)):
        if in_idx in state.done_indices:
            continue
        if in_idx <= state.last_match_input_index:
            if in_idx in errored_indices:
                pass
            else:
                state.filtered_out_indices.add(in_idx)
                state.done_indices.add(in_idx)

    return state


# ---------------------------------------------------------------------------
# Reorder buffer for parallel output ordering
# ---------------------------------------------------------------------------


class ReorderBuffer:
    """Buffers out-of-order batch results and flushes them in input order.

    Used when parallel > 1 to maintain input ordering in the output.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._buffer: Dict[int, List[Tuple[int, Dict[str, Any]]]] = {}
        self._next_flush_index: int = 0

    def add_batch(
        self, batch_index: int, rows: List[Tuple[int, Dict[str, Any]]]
    ) -> List[Tuple[int, Dict[str, Any]]]:
        """Add a completed batch and return rows ready to flush (in order).

        Returns rows that can be flushed now (contiguous from next_flush_index).
        """
        with self._lock:
            self._buffer[batch_index] = rows
            ready: List[Tuple[int, Dict[str, Any]]] = []
            while self._next_flush_index in self._buffer:
                ready.extend(self._buffer.pop(self._next_flush_index))
                self._next_flush_index += 1
            return ready

    def drain(self) -> List[Tuple[int, Dict[str, Any]]]:
        """Return all buffered rows (for WAL flush on interrupt)."""
        with self._lock:
            all_rows: List[Tuple[int, Dict[str, Any]]] = []
            for batch_idx in sorted(self._buffer.keys()):
                all_rows.extend(self._buffer[batch_idx])
            self._buffer.clear()
            return all_rows


# ---------------------------------------------------------------------------
# Merge helpers
# ---------------------------------------------------------------------------


def _merge_map_output(
    input_rows: List[Dict[str, Any]],
    existing_output: List[Dict[str, Any]],
    wal_records: List[Dict[str, Any]],
    target_column: str,
    fieldnames: List[str],
    multiple: bool = False,
    typed: bool = True,
) -> List[Dict[str, Any]]:
    """Merge input rows with existing output and WAL records for map.

    Builds the complete output by combining:
    - Existing output rows (already matched)
    - WAL records (gap fills)
    - Input rows (for any remaining)
    """
    wal_by_index: Dict[int, Any] = {}
    for record in wal_records:
        wal_by_index[record["i"]] = record["v"]

    resume_state = _match_map_output(
        input_rows, existing_output, target_column, multiple, typed=typed
    )

    merged: List[Dict[str, Any]] = []
    out_idx = 0

    for in_idx, in_row in enumerate(input_rows):
        if in_idx in resume_state.done_indices:
            merged.append(existing_output[out_idx])
            out_idx += 1
            if multiple:
                while out_idx < len(existing_output) and _is_superset(
                    existing_output[out_idx], in_row, typed=typed
                ):
                    merged.append(existing_output[out_idx])
                    out_idx += 1
        elif in_idx in resume_state.passthrough_indices:
            merged.append(existing_output[out_idx])
            out_idx += 1
        elif in_idx in wal_by_index:
            row = dict(in_row)
            row[target_column] = wal_by_index[in_idx]
            if multiple and isinstance(row[target_column], list):
                for item in row[target_column]:
                    new_row = dict(in_row)
                    new_row[target_column] = item
                    merged.append(new_row)
            else:
                merged.append(row)
        else:
            merged.append(dict(in_row))

    return merged


def _clear_err_file(err_path: Optional[Path]) -> None:
    if err_path is not None and err_path.exists():
        err_path.unlink()


def _append_error(err_path: Optional[Path], record: Dict[str, Any]) -> None:
    line = json.dumps(record, ensure_ascii=False)
    if err_path is not None:
        with err_path.open("a", encoding="utf-8") as fp:
            fp.write(line + "\n")
    else:
        click.echo(line, err=True)


def _read_errors(err_path: Path) -> List[Dict[str, Any]]:
    if not err_path.exists():
        return []
    records: List[Dict[str, Any]] = []
    with err_path.open("r", encoding="utf-8") as fp:
        for line in fp:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return records


def _extract_few_shot_examples(
    rows: Sequence[Dict[str, Any]], target_column: str, few_shot: int
) -> List[Tuple[Dict[str, Any], Any]]:
    examples: List[Tuple[Dict[str, Any], Any]] = []
    if few_shot <= 0:
        return examples
    for row in rows:
        value = row.get(target_column)
        if _has_value(value):
            example_row = {k: v for k, v in row.items() if k != target_column}
            examples.append((example_row, value))
            if len(examples) >= few_shot:
                break
    return examples


def _prepare_batches(
    rows: Sequence[Dict[str, Any]], batch_size: int, max_chars: int
) -> List[List[Dict[str, Any]]]:
    batches: List[List[Dict[str, Any]]] = []
    current_batch: List[Dict[str, Any]] = []
    current_len = 0
    for row in rows:
        row_text = json.dumps(row, ensure_ascii=False)
        if current_batch and (
            len(current_batch) >= batch_size or current_len + len(row_text) > max_chars
        ):
            batches.append(current_batch)
            current_batch = []
            current_len = 0
        current_batch.append(row)
        current_len += len(row_text)
    if current_batch:
        batches.append(current_batch)
    return batches


# ---------------------------------------------------------------------------
# Map prompt/schema/response
# ---------------------------------------------------------------------------


def _process_batch(
    model_obj,
    prompt: str,
    target_column: str,
    batch: List[Dict[str, Any]],
    few_shot_examples: Sequence[Tuple[Dict[str, Any], Any]],
    multiple: bool,
    verbose: bool = False,
) -> None:
    prompt_text = _build_map_prompt(
        prompt, target_column, batch, few_shot_examples, multiple
    )
    schema = _build_map_schema(batch, target_column, multiple)
    if verbose:
        _echo_verbose(prompt_text)
    response = model_obj.prompt(prompt_text, schema=schema)
    values = _parse_map_response(response.text(), target_column, len(batch), multiple)
    for row, value in zip(batch, values):
        row[target_column] = value


def _run_map_batches(
    pending: List[Tuple[int, Dict[str, Any]]],
    model_obj,
    prompt: str,
    target_column: str,
    few_shot_examples: Sequence[Tuple[Dict[str, Any], Any]],
    multiple: bool,
    batch_size: int,
    max_chars: int,
    parallel: int,
    err_path: Path,
    verbose: bool = False,
    wal_path: Optional[Path] = None,
    timeout: Optional[float] = None,
) -> int:
    pending_indices = [i for i, _ in pending]
    pending_rows = [row for _, row in pending]
    batches = _prepare_batches(pending_rows, batch_size, max_chars)

    idx_offset = 0
    index_batches: List[List[int]] = []
    for batch in batches:
        index_batches.append(pending_indices[idx_offset : idx_offset + len(batch)])
        idx_offset += len(batch)

    click.echo(f"Processing {len(batches)} batches (parallel={parallel})", err=True)
    failed = 0
    if parallel == 1:
        for i, (batch, indices) in enumerate(zip(batches, index_batches)):
            try:
                _submit_and_wait(
                    _process_batch,
                    (
                        model_obj,
                        prompt,
                        target_column,
                        batch,
                        few_shot_examples,
                        multiple,
                        verbose,
                    ),
                    timeout,
                )
            except Exception as exc:
                failed += 1
                _append_error(err_path, {"row_indices": indices, "error": str(exc)})
                click.echo(f"  Batch {i + 1}/{len(batches)} failed: {exc}", err=True)
                continue
            if wal_path:
                for idx, row in zip(indices, batch):
                    _append_wal(
                        wal_path,
                        {"i": idx, "c": target_column, "v": row.get(target_column)},
                    )
            click.echo(f"  Completed batch {i + 1}/{len(batches)}", err=True)
    else:
        with ThreadPoolExecutor(max_workers=parallel) as executor:
            futures = {
                executor.submit(
                    _process_batch,
                    model_obj,
                    prompt,
                    target_column,
                    batch,
                    few_shot_examples,
                    multiple,
                    verbose,
                ): (i, indices, batch)
                for i, (batch, indices) in enumerate(zip(batches, index_batches))
            }
            completed = 0
            for future in as_completed(futures):
                batch_idx, indices, batch = futures[future]
                try:
                    future.result(timeout=timeout)
                except Exception as exc:
                    failed += 1
                    _append_error(err_path, {"row_indices": indices, "error": str(exc)})
                    click.echo(
                        f"  Batch {batch_idx + 1}/{len(batches)} failed: {exc}",
                        err=True,
                    )
                    continue
                if wal_path:
                    for idx, row in zip(indices, batch):
                        _append_wal(
                            wal_path,
                            {
                                "i": idx,
                                "c": target_column,
                                "v": row.get(target_column),
                            },
                        )
                completed += 1
                click.echo(f"  Completed batch {completed}/{len(batches)}", err=True)
    return failed


def _run_map_batches_incremental(
    context: PluginContext,
    pending: List[Tuple[int, Dict[str, Any]]],
    model_obj,
    prompt: str,
    target_column: str,
    few_shot_examples: Sequence[Tuple[Dict[str, Any], Any]],
    multiple: bool,
    batch_size: int,
    max_chars: int,
    parallel: int,
    err_path: Optional[Path],
    output_path: Path,
    fieldnames: List[str],
    wal_path: Optional[Path],
    verbose: bool = False,
    timeout: Optional[float] = None,
) -> int:
    """Run map batches with incremental append to the output file.

    Rows are written to the output file as each batch completes, using
    a reorder buffer to maintain input ordering when parallel > 1.
    """
    pending_indices = [i for i, _ in pending]
    pending_rows = [row for _, row in pending]
    batches = _prepare_batches(pending_rows, batch_size, max_chars)

    idx_offset = 0
    index_batches: List[List[int]] = []
    for batch in batches:
        index_batches.append(pending_indices[idx_offset : idx_offset + len(batch)])
        idx_offset += len(batch)

    click.echo(f"Processing {len(batches)} batches (parallel={parallel})", err=True)

    output_plugin = context.outputs.for_path(output_path)
    assert isinstance(output_plugin, AppendableOutput)

    reorder = ReorderBuffer()
    failed = 0

    with output_plugin.open_append(output_path, fieldnames) as appender:

        def _flush_rows(rows_to_flush: List[Tuple[int, Dict[str, Any]]]) -> None:
            for _, row in rows_to_flush:
                appender.append(row)
            appender.flush()

        if parallel == 1:
            for batch_num, (batch, indices) in enumerate(zip(batches, index_batches)):
                batch_failed = False
                try:
                    _submit_and_wait(
                        _process_batch,
                        (
                            model_obj,
                            prompt,
                            target_column,
                            batch,
                            few_shot_examples,
                            multiple,
                            verbose,
                        ),
                        timeout,
                    )
                except Exception as exc:
                    batch_failed = True
                    failed += 1
                    _append_error(err_path, {"row_indices": indices, "error": str(exc)})
                    click.echo(
                        f"  Batch {batch_num + 1}/{len(batches)} failed: {exc}",
                        err=True,
                    )
                completed_rows = [(idx, row) for idx, row in zip(indices, batch)]
                ready = reorder.add_batch(batch_num, completed_rows)
                _flush_rows(ready)
                if not batch_failed:
                    click.echo(
                        f"  Completed batch {batch_num + 1}/{len(batches)}", err=True
                    )
        else:
            with ThreadPoolExecutor(max_workers=parallel) as executor:
                futures = {
                    executor.submit(
                        _process_batch,
                        model_obj,
                        prompt,
                        target_column,
                        batch,
                        few_shot_examples,
                        multiple,
                        verbose,
                    ): (batch_num, indices, batch)
                    for batch_num, (batch, indices) in enumerate(
                        zip(batches, index_batches)
                    )
                }
                completed = 0
                for future in as_completed(futures):
                    batch_num, indices, batch = futures[future]
                    batch_failed = False
                    try:
                        future.result(timeout=timeout)
                    except Exception as exc:
                        batch_failed = True
                        failed += 1
                        _append_error(
                            err_path,
                            {"row_indices": indices, "error": str(exc)},
                        )
                        click.echo(
                            f"  Batch {batch_num + 1}/{len(batches)} failed: {exc}",
                            err=True,
                        )
                    completed_rows = [(idx, row) for idx, row in zip(indices, batch)]
                    ready = reorder.add_batch(batch_num, completed_rows)
                    _flush_rows(ready)
                    if not batch_failed:
                        completed += 1
                        click.echo(
                            f"  Completed batch {completed}/{len(batches)}",
                            err=True,
                        )

                # Drain inside the executor context so it runs even if
                # KeyboardInterrupt fires during as_completed iteration.
                # NOTE: a true ctrl-c signal handler that flushes the
                # reorder buffer to WAL before exit is not yet implemented;
                # this only covers the normal-exit and exception-unwind
                # paths.
                remaining = reorder.drain()
                if remaining and wal_path:
                    for idx, row in remaining:
                        _append_wal(
                            wal_path,
                            {
                                "i": idx,
                                "c": target_column,
                                "v": row.get(target_column),
                            },
                        )

    return failed


def _write_via_temp_swap(
    context: PluginContext,
    output_path: Path,
    out_fmt: str,
    rows: List[Dict[str, Any]],
    fieldnames: List[str],
) -> None:
    """Write rows to a temp file, then atomically swap over the output."""
    suffix = output_path.suffix or ("." + out_fmt)
    tmp = tempfile.NamedTemporaryFile(
        dir=output_path.parent, suffix=suffix, delete=False
    )
    tmp_path = Path(tmp.name)
    tmp.close()
    try:
        plugin = context.outputs.for_path(output_path)
        plugin.write(tmp_path, rows, fieldnames)
        tmp_path.replace(output_path)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise


def _build_map_prompt(
    prompt: str,
    target_column: str,
    rows: Sequence[Dict[str, Any]],
    few_shot_examples: Sequence[Tuple[Dict[str, Any], Any]],
    multiple: bool = False,
) -> str:
    lines = [
        "You are assisting with spreadsheet transformations.",
    ]
    if few_shot_examples:
        lines.append("Here are example inputs and desired outputs:")
        for example_row, value in few_shot_examples:
            lines.append(
                json.dumps({"input": example_row, "output": value}, ensure_ascii=False)
            )
    lines.append("<spreadsheet_rows>")
    for i, row in enumerate(rows):
        lines.append(f"<row_{i}>")
        lines.append(json.dumps(row, ensure_ascii=False))
        lines.append(f"</row_{i}>")
    lines.append("</spreadsheet_rows>")
    lines.append("<user_instruction>")
    lines.append(prompt)
    lines.append("</user_instruction>")

    if multiple:
        lines.append(
            f"For each row, provide zero or more values for column '{target_column}'. "
            f"Each value in the list will be used to create a separate output row."
        )
    else:
        lines.append(
            f"For each row, provide a single value for column '{target_column}' "
            f"that answers the user_instruction."
        )

    return "\n".join(lines)


def _build_map_schema(
    rows: Sequence[Dict[str, Any]],
    target_column: str,
    multiple: bool = False,
) -> Dict[str, Any]:
    properties = {}
    required = []

    if multiple:
        column_schema: Dict[str, Any] = {
            "type": "array",
            "items": {"type": "string"},
        }
    else:
        column_schema = {
            "type": "string",
        }

    for i in range(len(rows)):
        row_key = f"row_{i}"
        required.append(row_key)
        properties[row_key] = {
            "type": "object",
            "properties": {target_column: column_schema},
            "required": [target_column],
            "additionalProperties": False,
            "title": f"Row {i}",
        }

    return {
        "type": "object",
        "properties": properties,
        "required": required,
        "title": "MapResponse",
    }


def _parse_map_response(
    response: str, target_column: str, expected: int, multiple: bool = False
) -> List[Any]:
    response = response.strip()
    if not response:
        raise click.ClickException("Model returned an empty response")
    try:
        data = json.loads(response)
    except json.JSONDecodeError as exc:
        raise click.ClickException(f"Response was not valid JSON: {response}") from exc

    if not isinstance(data, dict):
        raise click.ClickException(
            f"Expected a JSON object with row properties, got: {response}"
        )

    values = []
    for i in range(expected):
        row_key = f"row_{i}"
        if row_key not in data:
            raise click.ClickException(
                f"Missing expected property '{row_key}' in response"
            )
        row_data = data[row_key]

        if not isinstance(row_data, dict):
            raise click.ClickException(
                f"Expected '{row_key}' to be an object, got: {row_data}"
            )

        if target_column not in row_data:
            raise click.ClickException(
                f"Missing expected column '{target_column}' in '{row_key}'"
            )

        value = row_data[target_column]
        values.append(value)

    return values


# ---------------------------------------------------------------------------
# Reduce prompt/schema/response
# ---------------------------------------------------------------------------


def _reduce_rows(
    model_obj,
    prompt: str,
    rows: Sequence[Dict[str, Any]],
    group_by: Sequence[str],
    max_chars: int,
    result_column: str,
    verbose: bool = False,
) -> Any:
    rendered = json.dumps(list(rows), ensure_ascii=False)
    is_combine = not (len(rendered) <= max_chars or len(rows) == 1)

    if is_combine:
        midpoint = max(1, len(rows) // 2)
        first = _reduce_rows(
            model_obj,
            prompt,
            rows[:midpoint],
            group_by,
            max_chars,
            result_column,
            verbose,
        )
        second = _reduce_rows(
            model_obj,
            prompt,
            rows[midpoint:],
            group_by,
            max_chars,
            result_column,
            verbose,
        )
        prompt_rows = [first, second]
    else:
        prompt_rows = rows

    prompt_text = _build_reduce_prompt(
        prompt, prompt_rows, group_by, result_column, is_combine=is_combine
    )
    schema = _build_reduce_schema(result_column, prompt, is_combine)
    if verbose:
        _echo_verbose(prompt_text)
    response = model_obj.prompt(prompt_text, schema=schema)
    return _parse_reduce_response(response.text(), result_column)


def _build_reduce_schema(
    result_column: str, prompt: str, is_combine: bool = False
) -> Dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            result_column: {
                "type": "string",
                "description": f"Answer to '{prompt}' {'based on combining previous summaries' if is_combine else ''}.",
            }
        },
        "required": [result_column],
        "additionalProperties": False,
        "title": "ReduceResponse",
    }


def _build_reduce_prompt(
    prompt: str,
    rows: Sequence[Dict[str, Any]],
    group_by: Sequence[str],
    result_column: str,
    is_combine: bool = False,
) -> str:
    if is_combine:
        lines = [
            "You previously produced partial summaries of a group of rows in a spreadsheet."
        ]
    else:
        lines = ["You are summarizing a group of spreadsheet rows."]
    if group_by and rows:
        descriptor = [
            f"'{col}'='{rows[0].get(col)}'" for col in group_by if col in rows[0]
        ]
        if descriptor:
            lines.append(f"They have in common that {', '.join(descriptor)}")
    lines.append(f"The summary will be stored as '{result_column}'.")
    if is_combine:
        lines.append("Previous groups of rows have been combined into these summaries:")
        lines.append("<summaries>")
        for i, row in enumerate(rows):
            lines.append(f"<summary_{i}>")
            lines.append(json.dumps(row, ensure_ascii=False))
            lines.append(f"</summary_{i}>")
        lines.append("</summaries>")
        lines.append(
            "Combine the summaries in a way that answers the user's instruction:"
        )
    else:
        lines.append("<rows>")
        for i, row in enumerate(rows):
            lines.append(f"<row_{i}>")
            lines.append(json.dumps(row, ensure_ascii=False))
            lines.append(f"</row_{i}>")
        lines.append("</rows>")
        lines.append("Summarize the rows in a way that answers the user's instruction:")
    lines.append("<user_instruction>")
    lines.append(prompt)
    lines.append("</user_instruction>")
    return "\n".join(lines)


def _parse_reduce_response(response: str, result_column: str) -> Any:
    response = response.strip()
    if not response:
        raise click.ClickException("Model returned an empty response")
    try:
        data = json.loads(response)
    except json.JSONDecodeError as exc:
        raise click.ClickException(
            f"Reduction response was not valid JSON: {response}"
        ) from exc

    if not isinstance(data, dict):
        raise click.ClickException(
            f"Expected a JSON object with '{result_column}' property, got: {response}"
        )

    if result_column not in data:
        raise click.ClickException(
            f"Missing expected column '{result_column}' in response"
        )

    return data[result_column]


# ---------------------------------------------------------------------------
# Misc helpers
# ---------------------------------------------------------------------------


def _group_rows(
    rows: Sequence[Dict[str, Any]], group_by: Sequence[str]
) -> Dict[str, List[Dict[str, Any]]]:
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        key_parts = [str(row.get(col, "")) for col in group_by]
        key = " | ".join(key_parts)
        groups.setdefault(key, []).append(row)
    return groups


def _materialize(
    stream: TableStream, limit: Optional[int] = None
) -> Tuple[List[Dict[str, Any]], List[str]]:
    rows: List[Dict[str, Any]] = []
    for row in stream.rows:
        rows.append(dict(row))
        if limit is not None and len(rows) >= limit:
            break
    fieldnames = _merge_fieldnames(stream.fieldnames, rows)
    return rows, fieldnames


def _merge_fieldnames(
    provided: Optional[Sequence[str]], rows: Iterable[Dict[str, Any]]
) -> List[str]:
    seen = set()
    merged: List[str] = []
    if provided:
        for name in provided:
            if name not in seen:
                merged.append(name)
                seen.add(name)
    for row in rows:
        for name in row.keys():
            if name not in seen:
                merged.append(name)
                seen.add(name)
    return merged


def _expand_multiple_rows(
    rows: List[Dict[str, Any]],
    target_column: str,
    processed_indices: Set[int],
) -> List[Dict[str, Any]]:
    expanded: List[Dict[str, Any]] = []
    for i, row in enumerate(rows):
        value = row.get(target_column)
        if (
            i in processed_indices
            and not isinstance(value, str)
            and isinstance(value, Iterable)
        ):
            for item in value:
                new_row = row.copy()
                new_row[target_column] = item
                expanded.append(new_row)
        else:
            expanded.append(row)
    return expanded


# ---------------------------------------------------------------------------
# Streaming helpers for expression mode
# ---------------------------------------------------------------------------


def _stream_map_expression(
    stream: TableStream,
    deterministic: DeterministicExpression,
    target_column: str,
    parsed_filters: List[FilterCondition],
    limit: Optional[int],
    multiple: bool,
    write_fn: WriteFn,
    fieldnames: List[str],
) -> Tuple[int, int]:
    """Process expression-mode map without full materialization.

    Reads rows from the stream one at a time, evaluates the expression,
    and writes results immediately.  Returns (total_rows_read, rows_written).
    """
    total = 0
    written = 0

    def _generate():
        nonlocal total, written
        for raw_row in stream.rows:
            if limit is not None and total >= limit:
                break
            row = dict(raw_row)
            total += 1
            should_process = not parsed_filters or all(
                f.matches(row) for f in parsed_filters
            )
            processed = False
            if should_process and not _has_value(row.get(target_column)):
                try:
                    row[target_column] = deterministic.evaluate(row)
                    processed = True
                except Exception as exc:
                    raise click.ClickException(f"Expression failed: {exc}")

            if multiple and processed:
                value = row.get(target_column)
                if not isinstance(value, str) and isinstance(value, Iterable):
                    for item in value:
                        new_row = row.copy()
                        new_row[target_column] = item
                        written += 1
                        yield new_row
                    continue

            written += 1
            yield row

    write_fn(_generate(), fieldnames)
    return total, written


def _stream_filter_expression(
    stream: TableStream,
    deterministic: DeterministicExpression,
    parsed_filters: List[FilterCondition],
    limit: Optional[int],
    write_fn: WriteFn,
    fieldnames: List[str],
) -> Tuple[int, int]:
    """Process expression-mode filter without full materialization.

    Reads rows from the stream, evaluates the predicate, and writes
    matching rows immediately.  Returns (candidates, kept_count) where
    candidates is the number of rows that passed --where pre-filters.
    """
    total_read = 0
    candidates = 0
    kept = 0

    def _generate():
        nonlocal total_read, candidates, kept
        for raw_row in stream.rows:
            if limit is not None and total_read >= limit:
                break
            row = dict(raw_row)
            total_read += 1
            if parsed_filters and not all(f.matches(row) for f in parsed_filters):
                continue
            candidates += 1
            if deterministic.evaluate(row):
                kept += 1
                yield row

    write_fn(_generate(), fieldnames)
    return candidates, kept


# ---------------------------------------------------------------------------
# Dry-run & verbose helpers
# ---------------------------------------------------------------------------


def _echo_verbose(prompt_text: str) -> None:
    click.echo("--- prompt ---", err=True)
    click.echo(prompt_text, err=True)
    click.echo("--- end prompt ---", err=True)


def _echo_log_tip(n_calls: int) -> None:
    click.echo(
        f"Made {n_calls} LLM calls; run 'llm logs -n {n_calls}' to review", err=True
    )


def _echo_dry_run(
    prompt_text: str, schema: Dict[str, Any], total: int, unit: str
) -> None:
    click.echo("--- DRY RUN: sample prompt (1st batch) ---", err=True)
    click.echo(prompt_text, err=True)
    click.echo("--- schema ---", err=True)
    click.echo(json.dumps(schema, indent=2), err=True)
    click.echo(f"--- Would process {total} {unit} ---", err=True)


def _dry_run_map(
    pending: List[Tuple[int, Dict[str, Any]]],
    prompt: str,
    target_column: str,
    few_shot_examples: Sequence[Tuple[Dict[str, Any], Any]],
    multiple: bool,
    batch_size: int,
    max_chars: int,
) -> None:
    pending_rows = [row for _, row in pending]
    batches = _prepare_batches(pending_rows, batch_size, max_chars)
    sample = batches[0]
    prompt_text = _build_map_prompt(
        prompt, target_column, sample, few_shot_examples, multiple
    )
    schema = _build_map_schema(sample, target_column, multiple)
    _echo_dry_run(prompt_text, schema, len(batches), "batches")


def _dry_run_reduce(
    group_items: List[Tuple[str, List[Dict[str, Any]]]],
    prompt: str,
    group_by: Sequence[str],
    max_chars: int,
    result_column: str,
) -> None:
    group_key, group_rows = group_items[0]
    prompt_text = _build_reduce_prompt(prompt, group_rows, group_by, result_column)
    schema = _build_reduce_schema(result_column, prompt)
    _echo_dry_run(prompt_text, schema, len(group_items), "groups")


def _dry_run_filter(
    rows: List[Dict[str, Any]],
    instruction: str,
    batch_size: int,
    max_chars: int,
) -> None:
    batches = _prepare_batches(rows, batch_size, max_chars)
    sample = batches[0]
    prompt_text = _build_filter_prompt(instruction, sample)
    schema = _build_filter_schema(sample)
    _echo_dry_run(prompt_text, schema, len(batches), "batches")
