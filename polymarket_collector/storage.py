from __future__ import annotations

import asyncio
import gzip
import json
import os
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, BinaryIO


def _compress_part(path: Path) -> Path:
    destination = path.with_suffix("")
    if destination.suffix != ".jsonl":
        destination = Path(str(path).removesuffix(".part"))
    destination = destination.with_suffix(destination.suffix + ".gz")
    temporary = Path(str(destination) + ".tmp")
    try:
        with path.open("rb") as source, gzip.open(
            temporary, "wb", compresslevel=5
        ) as target:
            shutil.copyfileobj(source, target, length=1024 * 1024)
        os.replace(temporary, destination)
        path.unlink(missing_ok=True)
        return destination
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


class RotatingJsonlWriter:
    def __init__(
        self,
        root: Path,
        source: str,
        *,
        rotation_seconds: int,
        max_file_bytes: int,
        flush_interval_seconds: int,
    ) -> None:
        self.root = root
        self.source = source
        self.rotation_seconds = rotation_seconds
        self.max_file_bytes = max_file_bytes
        self.flush_interval_seconds = flush_interval_seconds
        self._file: BinaryIO | None = None
        self._path: Path | None = None
        self._opened_monotonic = 0.0
        self._last_flush_monotonic = 0.0
        self._size = 0
        self._sequence = 0
        self._compression_tasks: set[asyncio.Task[Path]] = set()
        self._compression_errors: list[BaseException] = []

    @property
    def current_path(self) -> str | None:
        return str(self._path) if self._path else None

    async def start(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        await asyncio.to_thread(self._recover_parts)

    def _recover_parts(self) -> None:
        for temporary in self.root.rglob("*.jsonl.gz.tmp"):
            temporary.unlink(missing_ok=True)
        for part in sorted(self.root.rglob("*.jsonl.part")):
            if part.stat().st_size == 0:
                part.unlink(missing_ok=True)
            else:
                _compress_part(part)

    def _open(self) -> None:
        now = datetime.now(timezone.utc)
        directory = self.root / now.strftime("%Y/%m/%d")
        directory.mkdir(parents=True, exist_ok=True)
        self._sequence += 1
        stamp = now.strftime("%Y%m%dT%H%M%S.%fZ")
        self._path = directory / (
            f"{self.source}-{stamp}-{os.getpid()}-{self._sequence:04d}.jsonl.part"
        )
        self._file = self._path.open("ab", buffering=64 * 1024)
        self._size = self._path.stat().st_size
        self._opened_monotonic = time.monotonic()
        self._last_flush_monotonic = self._opened_monotonic

    async def write(self, record: dict[str, Any]) -> None:
        if self._file is None:
            self._open()
        encoded = json.dumps(
            record, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8") + b"\n"
        assert self._file is not None
        self._file.write(encoded)
        self._size += len(encoded)

        now = time.monotonic()
        if now - self._last_flush_monotonic >= self.flush_interval_seconds:
            self._file.flush()
            self._last_flush_monotonic = now
        if (
            self._size >= self.max_file_bytes
            or now - self._opened_monotonic >= self.rotation_seconds
        ):
            self._rotate()

    def _rotate(self) -> None:
        if self._file is None or self._path is None:
            return
        self._file.flush()
        os.fsync(self._file.fileno())
        self._file.close()
        part = self._path
        self._file = None
        self._path = None
        task = asyncio.create_task(asyncio.to_thread(_compress_part, part))
        self._compression_tasks.add(task)
        task.add_done_callback(self._compression_done)

    def _compression_done(self, task: asyncio.Task[Path]) -> None:
        self._compression_tasks.discard(task)
        try:
            task.result()
        except BaseException as exc:
            self._compression_errors.append(exc)

    async def close(self) -> None:
        self._rotate()
        if self._compression_tasks:
            await asyncio.gather(
                *tuple(self._compression_tasks), return_exceptions=True
            )
        if self._compression_errors:
            raise RuntimeError(f"Compression failed: {self._compression_errors[0]}")
