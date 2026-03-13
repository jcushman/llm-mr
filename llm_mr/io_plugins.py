from __future__ import annotations

import csv
import io
import json
from contextlib import contextmanager
from pathlib import Path
from typing import IO, Iterable, Iterator, Sequence, Union

from openpyxl import Workbook, load_workbook

from .registries import InputRegistry, OutputRegistry, Row, TableStream


class CSVInputPlugin:
    name = "csv"
    extensions = [".csv"]

    @contextmanager
    def open(self, source: Union[Path, IO[str]]) -> Iterator[TableStream]:
        if isinstance(source, Path):
            with source.open("r", encoding="utf-8", newline="") as fp:
                yield self._read(fp)
        else:
            yield self._read(source)

    @staticmethod
    def _read(fp: IO[str]) -> TableStream:
        reader = csv.DictReader(fp)
        fieldnames = list(reader.fieldnames or [])
        return TableStream(rows=reader, fieldnames=fieldnames)


class CSVOutputPlugin:
    name = "csv"
    extensions = [".csv"]

    def write(
        self, dest: Union[Path, IO[str]], rows: Iterable[Row], fieldnames: Iterable[str]
    ) -> None:
        field_list = list(fieldnames)
        if isinstance(dest, Path):
            with dest.open("w", encoding="utf-8", newline="") as fp:
                self._write_fp(fp, rows, field_list)
        else:
            self._write_fp(dest, rows, field_list)

    @staticmethod
    def _write_fp(fp: IO[str], rows: Iterable[Row], field_list: list) -> None:
        writer = csv.DictWriter(fp, fieldnames=field_list)
        if field_list:
            writer.writeheader()
        for row in rows:
            writer.writerow(row)


class JSONLInputPlugin:
    name = "jsonl"
    extensions = [".jsonl"]

    @contextmanager
    def open(self, source: Union[Path, IO[str]]) -> Iterator[TableStream]:
        if isinstance(source, Path):
            with source.open("r", encoding="utf-8") as fp:
                yield self._read(fp)
        else:
            yield self._read(source)

    @staticmethod
    def _read(fp: IO[str]) -> TableStream:
        def generator():
            for line in fp:
                line = line.strip()
                if not line:
                    continue
                yield json.loads(line)

        iterator = generator()
        try:
            first = next(iterator)
        except StopIteration:
            return TableStream(rows=iter(()), fieldnames=[])

        def chain():
            yield first
            yield from iterator

        fieldnames = list(first.keys()) if isinstance(first, dict) else []
        return TableStream(rows=chain(), fieldnames=fieldnames)


class JSONLOutputPlugin:
    name = "jsonl"
    extensions = [".jsonl"]

    def write(
        self, dest: Union[Path, IO[str]], rows: Iterable[Row], fieldnames: Sequence[str]
    ) -> None:
        if isinstance(dest, Path):
            with dest.open("w", encoding="utf-8") as fp:
                self._write_fp(fp, rows)
        else:
            self._write_fp(dest, rows)

    @staticmethod
    def _write_fp(fp: IO[str], rows: Iterable[Row]) -> None:
        for row in rows:
            fp.write(json.dumps(row, ensure_ascii=False))
            fp.write("\n")


class XLSXInputPlugin:
    name = "xlsx"
    extensions = [".xlsx"]

    @contextmanager
    def open(self, path: Path) -> Iterator[TableStream]:
        workbook = load_workbook(path, read_only=True, data_only=True)
        try:
            sheet = workbook.active
            rows_iter = sheet.iter_rows(values_only=True)
            header = next(rows_iter, None)
            if header is None:
                yield TableStream(rows=iter(()), fieldnames=[])
                return
            fieldnames = ["" if value is None else str(value) for value in header]

            def generator():
                for values in rows_iter:
                    row = {
                        fieldnames[i]: values[i] if i < len(values) else None
                        for i in range(len(fieldnames))
                    }
                    yield {
                        key: ("" if value is None else value)
                        for key, value in row.items()
                    }

            yield TableStream(rows=generator(), fieldnames=fieldnames)
        finally:
            workbook.close()


class XLSXOutputPlugin:
    name = "xlsx"
    extensions = [".xlsx"]

    def write(self, path: Path, rows: Iterable[Row], fieldnames: Sequence[str]) -> None:
        field_list = list(fieldnames)
        workbook = Workbook()
        sheet = workbook.active
        if field_list:
            sheet.append(list(field_list))
        for row in rows:
            sheet.append([row.get(field) for field in field_list])
        workbook.save(path)


def register_builtin_io(inputs: InputRegistry, outputs: OutputRegistry) -> None:
    inputs.register(CSVInputPlugin())
    outputs.register(CSVOutputPlugin())

    inputs.register(JSONLInputPlugin())
    outputs.register(JSONLOutputPlugin())

    inputs.register(XLSXInputPlugin())
    outputs.register(XLSXOutputPlugin())
