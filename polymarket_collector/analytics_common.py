from __future__ import annotations

import gzip
import json
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any


ANALYTICS_EXTRA_MESSAGE = (
    "The analytics commands require the optional dependencies. "
    "Install them with: pip install -e '.[analytics]'"
)


def require_pyarrow() -> tuple[Any, Any]:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover - depends on the environment
        raise RuntimeError(ANALYTICS_EXTRA_MESSAGE) from exc
    return pa, pq


def resolve_jsonl_inputs(paths: Iterable[str | Path]) -> list[Path]:
    resolved: set[Path] = set()
    for raw in paths:
        path = Path(raw).expanduser().resolve()
        if path.is_dir():
            resolved.update(path.rglob("*.jsonl"))
            resolved.update(path.rglob("*.jsonl.gz"))
        elif path.is_file() and (
            path.name.endswith(".jsonl") or path.name.endswith(".jsonl.gz")
        ):
            resolved.add(path)
        else:
            raise FileNotFoundError(f"No JSONL input found at {path}")
    return sorted(resolved)


def open_jsonl(path: Path) -> Any:
    if path.name.endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8")
    return path.open("rt", encoding="utf-8")


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


class ParquetSink:
    def __init__(
        self,
        path: Path,
        schema: Any,
        *,
        batch_size: int,
        compression: str = "zstd",
    ) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be positive")
        self.pa, self.pq = require_pyarrow()
        self.path = path
        self.temporary = path.with_suffix(path.suffix + ".tmp")
        self.schema = schema
        self.batch_size = batch_size
        self.compression = compression
        self.rows: list[dict[str, Any]] = []
        self.writer: Any | None = None
        self.count = 0

    def add(self, row: dict[str, Any]) -> None:
        self.rows.append(row)
        if len(self.rows) >= self.batch_size:
            self.flush()

    def flush(self) -> None:
        if not self.rows:
            return
        if self.writer is None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.temporary.unlink(missing_ok=True)
            self.writer = self.pq.ParquetWriter(
                self.temporary,
                self.schema,
                compression=self.compression,
                use_dictionary=True,
                write_statistics=True,
            )
        table = self.pa.Table.from_pylist(self.rows, schema=self.schema)
        self.writer.write_table(table)
        self.count += len(self.rows)
        self.rows.clear()

    def close(self) -> int:
        self.flush()
        if self.writer is None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            table = self.pa.Table.from_pylist([], schema=self.schema)
            self.pq.write_table(
                table,
                self.temporary,
                compression=self.compression,
            )
        else:
            self.writer.close()
        self.writer = None
        self.temporary.replace(self.path)
        return self.count

    def abort(self) -> None:
        if self.writer is not None:
            self.writer.close()
            self.writer = None
        self.temporary.unlink(missing_ok=True)


def iter_parquet_rows(
    path: str | Path,
    *,
    columns: list[str] | None = None,
    batch_size: int = 65_536,
) -> Iterator[dict[str, Any]]:
    _, pq = require_pyarrow()
    parquet = pq.ParquetFile(Path(path))
    for batch in parquet.iter_batches(batch_size=batch_size, columns=columns):
        for row in batch.to_pylist():
            yield row

