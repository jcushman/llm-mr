from __future__ import annotations

import json
import shutil
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass
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
            "--repair", is_flag=True, help="Retry failed rows from the .err sidecar"
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
            repair: bool,
            err_file: Optional[Path],
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

            if repair:
                if err_path is None:
                    raise click.UsageError("--repair requires --output or --err")
                if output_path is None:
                    raise click.UsageError(
                        "--repair requires --output to merge results"
                    )
                _map_repair(
                    context,
                    output_path,
                    err_path,
                    instruction,
                    target_column,
                    batch_size,
                    max_chars,
                    few_shot,
                    worker_model or model,
                    multiple,
                    parallel,
                )
                return

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

            deterministic = None
            prompt_text = instruction
            if mode == "expression":
                deterministic = _compile_expression(instruction)
            elif mode == "interactive":
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
                    )
                    if failed:
                        click.echo(
                            f"Warning: {failed} batches failed"
                            + (
                                f"; see {err_path} — rerun with --repair"
                                if err_path
                                else ""
                            ),
                            err=True,
                        )

            if multiple:
                rows = _expand_multiple_rows(rows, target_column, processed_indices)

            write_output = _output_writer(context, output_path, out_fmt)
            write_output(rows, fieldnames)
            click.echo(
                f"Wrote {len(rows)} rows"
                + (f" to {output_path}" if output_path else ""),
                err=True,
            )
            if not deterministic and pending:
                pending_rows = [row for _, row in pending]
                n_batches = len(_prepare_batches(pending_rows, batch_size, max_chars))
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
            "--repair", is_flag=True, help="Retry failed groups from the .err sidecar"
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
            repair: bool,
            err_file: Optional[Path],
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

            if repair:
                if err_path is None:
                    raise click.UsageError("--repair requires --output or --err")
                if output is None:
                    raise click.UsageError(
                        "--repair requires --output to merge results"
                    )
                _reduce_repair(
                    context,
                    output,
                    err_path,
                    groups,
                    instruction,
                    group_by,
                    result_column,
                    group_key_column,
                    worker_model or model,
                    max_chars,
                    parallel,
                )
                return

            deterministic = None
            prompt_text = instruction
            if mode == "expression":
                deterministic = _compile_reduce_expression(instruction)
            elif mode == "interactive":
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

                group_items = list(groups.items())

                if dry_run:
                    _dry_run_reduce(
                        group_items,
                        prompt_text,
                        group_by,
                        max_chars,
                        result_column,
                    )
                    return

                click.echo(
                    f"Reducing {len(group_items)} groups (parallel={parallel})",
                    err=True,
                )
                _clear_err_file(err_path)
                results, failed = _run_reduce_groups(
                    group_items,
                    worker,
                    prompt_text,
                    group_by,
                    max_chars,
                    result_column,
                    group_key_column,
                    parallel,
                    err_path,
                    verbose,
                )
                if failed:
                    click.echo(
                        f"Warning: {failed}/{len(group_items)} groups failed"
                        + (
                            f"; see {err_path} — rerun with --repair"
                            if err_path
                            else ""
                        ),
                        err=True,
                    )

            write_output = _output_writer(context, output, out_fmt)
            write_output(results, [group_key_column, result_column])
            click.echo(
                f"Wrote {len(results)} group results"
                + (f" to {output}" if output else ""),
                err=True,
            )
            if not deterministic:
                _echo_log_tip(len(group_items))


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
            dry_run: bool,
            verbose: bool,
        ) -> None:
            mode = _validate_mode_flags(prompt_mode, expression_mode)
            _stdin_guard(input_path)
            _interactive_stdin_guard(mode, input_path)

            in_fmt, out_fmt = _resolve_formats(
                input_path, output, format_, input_format, output_format
            )

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

            deterministic = None
            prompt_text = instruction
            if mode == "interactive":
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

                if dry_run:
                    _dry_run_filter(rows, prompt_text, batch_size, max_chars)
                    return

                kept = _run_filter_llm(
                    rows,
                    worker,
                    prompt_text,
                    batch_size,
                    max_chars,
                    parallel,
                    verbose,
                )

            write_output = _output_writer(context, output, out_fmt)
            write_output(kept, fieldnames)
            click.echo(
                f"Kept {len(kept)}/{len(rows)} rows"
                + (f" → {output}" if output else ""),
                err=True,
            )
            if not deterministic:
                n_batches = len(_prepare_batches(rows, batch_size, max_chars))
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
) -> List[Dict[str, Any]]:
    """Classify each row with the LLM and keep rows where result is truthy."""
    batches = _prepare_batches(rows, batch_size, max_chars)

    def _process_one(batch):
        prompt_text = _build_filter_prompt(instruction, batch)
        schema = _build_filter_schema(batch)
        if verbose:
            _echo_verbose(prompt_text)
        response = model_obj.prompt(prompt_text, schema=schema)
        return _parse_filter_response(response.text(), len(batch))

    results: List[bool] = []
    if parallel == 1:
        for i, batch in enumerate(batches):
            verdicts = _process_one(batch)
            results.extend(verdicts)
            click.echo(f"  Filtered batch {i + 1}/{len(batches)}", err=True)
    else:
        batch_verdicts: Dict[int, List[bool]] = {}
        with ThreadPoolExecutor(max_workers=parallel) as executor:
            futures = {
                executor.submit(_process_one, batch): i
                for i, batch in enumerate(batches)
            }
            for future in as_completed(futures):
                idx = futures[future]
                batch_verdicts[idx] = future.result()
                click.echo(f"  Filtered batch {idx + 1}/{len(batches)}", err=True)
        for i in range(len(batches)):
            results.extend(batch_verdicts[i])

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
                results.append(_reduce_one(group_key, group_rows))
            except Exception as exc:
                failed += 1
                _append_error(err_path, {"group_key": group_key, "error": str(exc)})
                click.echo(
                    f"  Group {i + 1}/{len(group_items)} failed: {exc}", err=True
                )
                continue
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
                    results.append(future.result())
                except Exception as exc:
                    failed += 1
                    _append_error(err_path, {"group_key": group_key, "error": str(exc)})
                    click.echo(
                        f"  Group {group_idx + 1}/{len(group_items)} failed: {exc}",
                        err=True,
                    )
                    continue
                completed += 1
                click.echo(
                    f"  Completed group {completed}/{len(group_items)}", err=True
                )

    return results, failed


def _reduce_repair(
    context: PluginContext,
    output: Path,
    err_path: Path,
    groups: Dict[str, List[Dict[str, Any]]],
    prompt: str,
    group_by: Sequence[str],
    result_column: str,
    group_key_column: str,
    model: Optional[str],
    max_chars: int,
    parallel: int,
) -> None:
    error_records = _read_errors(err_path)
    if not error_records:
        click.echo("No errors to repair; nothing to do", err=True)
        return

    failed_keys = {r["group_key"] for r in error_records}
    pending_groups = [(k, v) for k, v in groups.items() if k in failed_keys]

    if not pending_groups:
        click.echo("No matching groups to repair", err=True)
        _clear_err_file(err_path)
        return

    existing_results: List[Dict[str, Any]] = []
    if output.exists():
        try:
            output_input_plugin = context.inputs.for_path(output)
            with output_input_plugin.open(output) as stream:
                existing_rows, _ = _materialize(stream)
            existing_results = list(existing_rows)
        except Exception:
            pass

    model_obj = resolve_model(model)
    _require_schema_support(model_obj)

    _clear_err_file(err_path)
    click.echo(
        f"Repairing {len(pending_groups)} groups (parallel={parallel})", err=True
    )
    new_results, failed = _run_reduce_groups(
        pending_groups,
        model_obj,
        prompt,
        group_by,
        max_chars,
        result_column,
        group_key_column,
        parallel,
        err_path,
    )

    if failed:
        click.echo(f"Warning: {failed} groups still failing; see {err_path}", err=True)

    merged = existing_results + new_results
    output_plugin = context.outputs.for_path(output)
    output_plugin.write(output, merged, [group_key_column, result_column])
    click.echo(f"Wrote {len(merged)} group results to {output}", err=True)


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
            if line:
                records.append(json.loads(line))
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
                _process_batch(
                    model_obj,
                    prompt,
                    target_column,
                    batch,
                    few_shot_examples,
                    multiple,
                    verbose,
                )
            except Exception as exc:
                failed += 1
                _append_error(err_path, {"row_indices": indices, "error": str(exc)})
                click.echo(f"  Batch {i + 1}/{len(batches)} failed: {exc}", err=True)
                continue
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
                ): (i, indices)
                for i, (batch, indices) in enumerate(zip(batches, index_batches))
            }
            completed = 0
            for future in as_completed(futures):
                batch_idx, indices = futures[future]
                try:
                    future.result()
                except Exception as exc:
                    failed += 1
                    _append_error(err_path, {"row_indices": indices, "error": str(exc)})
                    click.echo(
                        f"  Batch {batch_idx + 1}/{len(batches)} failed: {exc}",
                        err=True,
                    )
                    continue
                completed += 1
                click.echo(f"  Completed batch {completed}/{len(batches)}", err=True)
    return failed


def _map_repair(
    context: PluginContext,
    output_path: Path,
    err_path: Path,
    prompt: str,
    target_column: str,
    batch_size: int,
    max_chars: int,
    few_shot: int,
    model: Optional[str],
    multiple: bool,
    parallel: int,
) -> None:
    error_records = _read_errors(err_path)
    if not error_records:
        click.echo("No errors to repair; nothing to do", err=True)
        return

    if not output_path.exists():
        raise click.ClickException(
            f"Output file {output_path} does not exist. Run without --repair first."
        )

    output_input_plugin = context.inputs.for_path(output_path)
    with output_input_plugin.open(output_path) as stream:
        rows, fieldnames = _materialize(stream)

    failed_indices = set()
    for record in error_records:
        failed_indices.update(record.get("row_indices", []))

    pending = [(i, rows[i]) for i in sorted(failed_indices) if i < len(rows)]
    if not pending:
        click.echo("No rows to repair", err=True)
        _clear_err_file(err_path)
        return

    processed_indices = {i for i, _ in pending}

    model_obj = resolve_model(model)
    _require_schema_support(model_obj)

    few_shot_examples = _extract_few_shot_examples(rows, target_column, few_shot)
    _clear_err_file(err_path)
    click.echo(f"Repairing {len(pending)} rows", err=True)
    failed = _run_map_batches(
        pending,
        model_obj,
        prompt,
        target_column,
        few_shot_examples,
        multiple,
        batch_size,
        max_chars,
        parallel,
        err_path,
    )
    if failed:
        click.echo(f"Warning: {failed} batches still failing; see {err_path}", err=True)

    if multiple:
        rows = _expand_multiple_rows(rows, target_column, processed_indices)

    output_plugin = context.outputs.for_path(output_path)
    output_plugin.write(output_path, rows, fieldnames)
    click.echo(f"Wrote {len(rows)} rows to {output_path}", err=True)


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
