import csv
import json
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import llm
import pytest
from click.testing import CliRunner
from llm.cli import cli
from llm.plugins import pm

from llm_mr.plugin import register as register_commands


class MockModel(llm.Model):
    model_id = "demo"

    def __init__(self):
        self.responses = []
        self.last_prompt = None
        self.prompt_history = []

    def queue_response(self, text: str) -> None:
        self.responses.append(text)

    def execute(self, prompt, stream, response, conversation):
        self.last_prompt = prompt
        self.prompt_history.append(prompt.prompt)
        text = self.responses.pop(0) if self.responses else ""
        return [text]


@pytest.fixture(scope="module", autouse=True)
def ensure_commands_registered():
    if "mr" not in cli.commands:
        register_commands(cli)
    yield


@pytest.fixture
def mock_model():
    model = MockModel()

    class TestPlugin:
        __name__ = "TestModelPlugin"

        @llm.hookimpl
        def register_models(self, register):
            register(model)

    plugin = TestPlugin()
    pm.register(plugin, name="llm-mr-test-model")
    try:
        yield model
    finally:
        pm.unregister(plugin=plugin)


def _write_csv(path: Path, rows):
    fieldnames = list(rows[0].keys()) if rows else []
    with path.open("w", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _read_csv(path: Path):
    with path.open("r", encoding="utf-8", newline="") as fp:
        reader = csv.DictReader(fp)
        return list(reader)


def test_map_command_filters_and_updates_rows(tmp_path, mock_model):
    input_path = tmp_path / "input.csv"
    output_path = tmp_path / "output.csv"
    rows = [
        {"id": "1", "status": "active", "note": "alpha", "mr_result": "existing"},
        {"id": "2", "status": "active", "note": "beta", "mr_result": ""},
        {"id": "3", "status": "inactive", "note": "gamma", "mr_result": ""},
    ]
    _write_csv(input_path, rows)

    mock_model.queue_response(json.dumps("Processed beta"))

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "mr",
            "map",
            str(input_path),
            "-p",
            "Return uppercase note",
            "--where",
            "status=active",
            "--few-shot",
            "1",
            "--output",
            str(output_path),
            "--model",
            "demo",
        ],
    )
    assert result.exit_code == 0
    assert f"Wrote 3 rows to {output_path}" in result.output

    written_rows = _read_csv(output_path)
    assert written_rows[0]["mr_result"] == "existing"
    assert written_rows[1]["mr_result"] == "Processed beta"
    assert written_rows[2]["mr_result"] == ""

    expected_lines = [
        "You are assisting with spreadsheet transformations.",
        "Return valid JSON with no additional commentary.",
        "Here are example inputs and desired outputs:",
        "{\"input\": {\"id\": \"1\", \"status\": \"active\", \"note\": \"alpha\"}, \"output\": \"existing\"}",
        "User instruction:",
        "Return uppercase note",
        "Rows to process (JSON objects):",
        "{\"id\": \"2\", \"status\": \"active\", \"note\": \"beta\", \"mr_result\": \"\"}",
        "Return a JSON string or number representing the value for column 'mr_result'.",
    ]
    assert mock_model.prompt_history[-1].splitlines() == expected_lines


def test_reduce_command_groups_rows(tmp_path, mock_model):
    input_path = tmp_path / "input.csv"
    output_path = tmp_path / "output.csv"
    rows = [
        {"team": "A", "score": "10"},
        {"team": "A", "score": "12"},
        {"team": "B", "score": "5"},
    ]
    _write_csv(input_path, rows)

    mock_model.queue_response(json.dumps("Summary A"))
    mock_model.queue_response(json.dumps("Summary B"))

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "mr",
            "reduce",
            str(input_path),
            "-p",
            "Summarize group",
            "--group-by",
            "team",
            "--output",
            str(output_path),
            "--model",
            "demo",
        ],
    )
    assert result.exit_code == 0
    assert f"Wrote 2 group results to {output_path}" in result.output

    written_rows = _read_csv(output_path)
    assert written_rows == [
        {"group": "A", "mr_result": "Summary A"},
        {"group": "B", "mr_result": "Summary B"},
    ]

    assert len(mock_model.prompt_history) == 2
    for prompt in mock_model.prompt_history:
        lines = prompt.splitlines()
        assert lines[0] == "You are reducing a group of spreadsheet rows."
        assert "Rows (JSON list):" in lines
        assert "Reduction instruction:" in lines
