from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

import click
import llm
from llm.cli import get_default_model

from .registries import PluginContext, ProcessorRegistry, TableStream


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


def register_builtin_processors(registry: ProcessorRegistry) -> None:
    registry.register(MapProcessor())
    registry.register(ReduceProcessor())


class MapProcessor:
    name = "map"

    def register_cli(self, group: click.Group, context: PluginContext) -> None:
        @group.command(name=self.name)
        @click.argument("input_path", type=click.Path(exists=True, dir_okay=False, path_type=Path))
        @click.option("-p", "--prompt", required=True, help="Prompt describing the transformation")
        @click.option(
            "-o",
            "--output",
            type=click.Path(dir_okay=False, path_type=Path),
            help="Output path. Required unless --in-place is provided.",
        )
        @click.option("--in-place", is_flag=True, help="Overwrite the input file in place")
        @click.option(
            "-c",
            "--column",
            "target_column",
            default="mr_result",
            show_default=True,
            help="Column to populate with LLM output",
        )
        @click.option(
            "--where",
            "filters",
            multiple=True,
            help="Row filter like status=active or score>=10",
        )
        @click.option(
            "--batch-size",
            type=click.IntRange(1),
            default=1,
            show_default=True,
            help="Number of rows to include in a single prompt",
        )
        @click.option(
            "--max-chars",
            type=click.IntRange(500, 200000),
            default=6000,
            show_default=True,
            help="Maximum characters permitted in one prompt batch",
        )
        @click.option(
            "--few-shot",
            type=click.IntRange(0),
            default=0,
            show_default=True,
            help="Use this many existing values as few-shot examples",
        )
        @click.option("--model", help="LLM model to use")
        @click.option(
            "--smart-model",
            help="Model used to synthesize deterministic expressions before prompting",
        )
        @click.option("--key", help="API key to use for the primary model")
        @click.option(
            "--smart-key",
            help="API key for the smart model (defaults to the primary key if omitted)",
        )
        def map_command(
            input_path: Path,
            prompt: str,
            output: Optional[Path],
            in_place: bool,
            target_column: str,
            filters: Sequence[str],
            batch_size: int,
            max_chars: int,
            few_shot: int,
            model: Optional[str],
            smart_model: Optional[str],
            key: Optional[str],
            smart_key: Optional[str],
        ) -> None:
            if not output and not in_place:
                raise click.UsageError("Provide --output or use --in-place")
            output_path = input_path if in_place else output
            assert output_path is not None

            input_plugin = context.inputs.for_path(input_path)
            with input_plugin.open(input_path) as stream:
                rows, fieldnames = _materialize(stream)

            parsed_filters = [parse_filter(expr) for expr in filters]
            target_rows = filter_rows(rows, parsed_filters)

            if target_column not in fieldnames:
                fieldnames.append(target_column)
            for row in rows:
                row.setdefault(target_column, "")

            deterministic = None
            if smart_model:
                deterministic = _attempt_deterministic(
                    prompt,
                    target_column,
                    fieldnames,
                    smart_model,
                    smart_key or key,
                    few_shot_examples=_extract_few_shot_examples(rows, target_column, few_shot),
                )
                if deterministic:
                    click.echo(f"Using deterministic expression: {deterministic.expression}")

            if deterministic:
                for row in target_rows:
                    if _has_value(row.get(target_column)):
                        continue
                    try:
                        row[target_column] = deterministic.evaluate(row)
                    except Exception as exc:  # pragma: no cover - runtime safety
                        raise click.ClickException(f"Deterministic expression failed: {exc}")
            else:
                model_obj = resolve_model(model, key)
                few_shot_examples = _extract_few_shot_examples(rows, target_column, few_shot)
                pending_rows = [
                    row for row in target_rows if not _has_value(row.get(target_column))
                ]
                if not pending_rows:
                    click.echo("No rows required LLM processing; nothing to do")
                else:
                    for batch in _prepare_batches(pending_rows, batch_size, max_chars):
                        response = model_obj.prompt(
                            _build_map_prompt(prompt, target_column, batch, few_shot_examples)
                        )
                        values = _parse_map_response(str(response), len(batch))
                        for row, value in zip(batch, values):
                            row[target_column] = value

            output_plugin = context.outputs.for_path(output_path)
            output_plugin.write(output_path, rows, fieldnames)
            click.echo(f"Wrote {len(rows)} rows to {output_path}")


class ReduceProcessor:
    name = "reduce"

    def register_cli(self, group: click.Group, context: PluginContext) -> None:
        @group.command(name=self.name)
        @click.argument("input_path", type=click.Path(exists=True, dir_okay=False, path_type=Path))
        @click.option("-p", "--prompt", required=True, help="Prompt describing the reduction")
        @click.option(
            "--group-by",
            "group_by",
            multiple=True,
            required=True,
            help="Column(s) to group by",
        )
        @click.option(
            "--where",
            "filters",
            multiple=True,
            help="Row filter like status=active or score>=10",
        )
        @click.option(
            "-o",
            "--output",
            type=click.Path(dir_okay=False, path_type=Path),
            required=True,
            help="Destination path for reduced output",
        )
        @click.option(
            "-c",
            "--column",
            "result_column",
            default="mr_result",
            show_default=True,
            help="Column name for reduced value",
        )
        @click.option("--model", help="LLM model to use")
        @click.option("--key", help="API key for the model")
        @click.option(
            "--max-chars",
            type=click.IntRange(500, 200000),
            default=8000,
            show_default=True,
            help="Maximum characters permitted in a single reduction prompt",
        )
        def reduce_command(
            input_path: Path,
            prompt: str,
            group_by: Sequence[str],
            filters: Sequence[str],
            output: Path,
            result_column: str,
            model: Optional[str],
            key: Optional[str],
            max_chars: int,
        ) -> None:
            input_plugin = context.inputs.for_path(input_path)
            with input_plugin.open(input_path) as stream:
                rows, _ = _materialize(stream)

            parsed_filters = [parse_filter(expr) for expr in filters]
            filtered_rows = filter_rows(rows, parsed_filters)

            groups = _group_rows(filtered_rows, group_by)
            model_obj = resolve_model(model, key)
            results: List[Dict[str, Any]] = []
            for group_key, group_rows in groups.items():
                value = _reduce_rows(model_obj, prompt, group_rows, group_by, max_chars)
                result_row = {"group": group_key, result_column: value}
                results.append(result_row)

            output_plugin = context.outputs.for_path(output)
            output_plugin.write(output, results, ["group", result_column])
            click.echo(f"Wrote {len(results)} group results to {output}")


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
        except ValueError as exc:  # pragma: no cover - defensive
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


def filter_rows(rows: List[Dict[str, Any]], filters: Sequence[FilterCondition]) -> List[Dict[str, Any]]:
    if not filters:
        return rows
    return [row for row in rows if all(f.matches(row) for f in filters)]


def _has_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip() != ""
    return True


def resolve_model(model_name: Optional[str], key: Optional[str]):
    model_id = model_name or get_default_model()
    model_obj = llm.get_model(model_id)
    if model_obj.needs_key:
        model_obj.key = llm.get_key(key, model_obj.needs_key, model_obj.key_env_var)
    return model_obj


@dataclass
class DeterministicExpression:
    expression: str
    code: Any

    def evaluate(self, row: Dict[str, Any]) -> Any:
        return eval(self.code, {"__builtins__": {}}, {"row": row})


def _attempt_deterministic(
    prompt: str,
    target_column: str,
    fieldnames: Sequence[str],
    smart_model_name: str,
    key: Optional[str],
    few_shot_examples: Sequence[Tuple[Dict[str, Any], Any]],
) -> Optional[DeterministicExpression]:
    try:
        model_obj = resolve_model(smart_model_name, key)
    except Exception:
        return None
    instructions = [
        "You translate spreadsheet prompts into deterministic Python expressions.",
        "Columns available: " + ", ".join(fieldnames),
        f"The expression should compute the value for the column '{target_column}'.",
        "Return ONLY a JSON object with keys 'expression' and 'deterministic'.",
        "If the prompt cannot be answered deterministically, set deterministic to false.",
        "When deterministic is true, 'expression' must be a Python expression using the variable 'row'.",
    ]
    payload = {
        "prompt": prompt,
        "examples": [
            {"input": row, "output": value} for row, value in few_shot_examples
        ],
    }
    response = model_obj.prompt(
        "\n".join(instructions) + "\n" + json.dumps(payload, ensure_ascii=False)
    )
    try:
        data = json.loads(str(response))
    except json.JSONDecodeError:
        return None
    if not data or not data.get("deterministic"):
        return None
    expression = data.get("expression")
    if not isinstance(expression, str):
        return None
    try:
        code = compile(expression, "<deterministic>", "eval")
    except SyntaxError:
        return None
    if not click.confirm(
        f"Use deterministic expression for column '{target_column}'?\n{expression}", default=True
    ):
        return None
    return DeterministicExpression(expression=expression, code=code)


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


def _build_map_prompt(
    prompt: str,
    target_column: str,
    rows: Sequence[Dict[str, Any]],
    few_shot_examples: Sequence[Tuple[Dict[str, Any], Any]],
) -> str:
    lines = [
        "You are assisting with spreadsheet transformations.",
        "Return valid JSON with no additional commentary.",
    ]
    if few_shot_examples:
        lines.append("Here are example inputs and desired outputs:")
        for example_row, value in few_shot_examples:
            lines.append(
                json.dumps({"input": example_row, "output": value}, ensure_ascii=False)
            )
    lines.append("User instruction:")
    lines.append(prompt)
    lines.append("Rows to process (JSON objects):")
    for row in rows:
        lines.append(json.dumps(row, ensure_ascii=False))
    if len(rows) == 1:
        lines.append(
            f"Return a JSON string or number representing the value for column '{target_column}'."
        )
    else:
        lines.append(
            f"Return a JSON array of length {len(rows)} with the values for column '{target_column}' in order."
        )
    return "\n".join(lines)


def _parse_map_response(response: str, expected: int) -> List[Any]:
    response = response.strip()
    if not response:
        raise click.ClickException("Model returned an empty response")
    try:
        data = json.loads(response)
    except json.JSONDecodeError as exc:
        raise click.ClickException(f"Response was not valid JSON: {response}") from exc
    if expected == 1:
        return [data]
    if not isinstance(data, list) or len(data) != expected:
        raise click.ClickException(
            f"Expected a JSON array of {expected} items, got: {response}"
        )
    return data


def _group_rows(
    rows: Sequence[Dict[str, Any]], group_by: Sequence[str]
) -> Dict[str, List[Dict[str, Any]]]:
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        key_parts = [str(row.get(col, "")) for col in group_by]
        key = " | ".join(key_parts)
        groups.setdefault(key, []).append(row)
    return groups


def _reduce_rows(
    model_obj,
    prompt: str,
    rows: Sequence[Dict[str, Any]],
    group_by: Sequence[str],
    max_chars: int,
) -> Any:
    rendered = json.dumps(list(rows), ensure_ascii=False)
    if len(rendered) <= max_chars or len(rows) == 1:
        response = model_obj.prompt(
            _build_reduce_prompt(prompt, rows, group_by)
        )
        return _parse_reduce_response(str(response))
    else:
        midpoint = max(1, len(rows) // 2)
        first = _reduce_rows(model_obj, prompt, rows[:midpoint], group_by, max_chars)
        second = _reduce_rows(model_obj, prompt, rows[midpoint:], group_by, max_chars)
        response = model_obj.prompt(
            _build_combine_prompt(prompt, [first, second])
        )
        return _parse_reduce_response(str(response))


def _build_reduce_prompt(
    prompt: str,
    rows: Sequence[Dict[str, Any]],
    group_by: Sequence[str],
) -> str:
    lines = [
        "You are reducing a group of spreadsheet rows.",
        "Return a JSON string or number with no extra commentary.",
    ]
    if group_by and rows:
        descriptor = {col: rows[0].get(col) for col in group_by if col in rows[0]}
        lines.append("Group descriptor:")
        lines.append(json.dumps(descriptor, ensure_ascii=False))
    lines.append("Rows (JSON list):")
    lines.append(json.dumps(list(rows), ensure_ascii=False))
    lines.append("Reduction instruction:")
    lines.append(prompt)
    return "\n".join(lines)


def _build_combine_prompt(prompt: str, partial_results: Sequence[Any]) -> str:
    lines = [
        "You previously produced partial reductions for a spreadsheet prompt.",
        "Combine them into a single final result.",
        "Return a JSON string or number with no commentary.",
        "Original prompt:",
        prompt,
        "Partial results (JSON array):",
        json.dumps(list(partial_results), ensure_ascii=False),
    ]
    return "\n".join(lines)


def _parse_reduce_response(response: str) -> Any:
    response = response.strip()
    if not response:
        raise click.ClickException("Model returned an empty response")
    try:
        return json.loads(response)
    except json.JSONDecodeError as exc:
        raise click.ClickException(f"Reduction response was not valid JSON: {response}") from exc


def _materialize(stream: TableStream) -> Tuple[List[Dict[str, Any]], List[str]]:
    rows = [dict(row) for row in stream.rows]
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
