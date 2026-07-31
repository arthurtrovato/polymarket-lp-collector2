"""Idempotent, partitioned ETL for append-only raw archive collections.

The collector writes immutable JSONL archives.  This module converts each
archive exactly once, records its content hash and quality report in an
atomic manifest, and writes one Parquet file per source archive.  Keeping the
archive boundary in the output makes retries cheap and prevents a failed
window from rewriting the whole historical dataset.
"""

from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import fcntl
import hashlib
import json
import logging
import os
import re
import tempfile
from collections import Counter
from pathlib import Path
from typing import Any, Iterator

from .analytics_common import resolve_jsonl_inputs
from .etl import convert


LOGGER = logging.getLogger(__name__)
MANIFEST_VERSION = 1
SCHEMA_VERSION = 1
TIMESTAMP_RE = re.compile(r"(\d{8}T\d{6}(?:\.\d+)?Z)")
DATE_RE = re.compile(r"(?:^|/)(\d{4})/(\d{2})/(\d{2})(?:/|$)")
SUMMARY_COUNTERS = (
    "raw_rows",
    "invalid_json_rows",
    "invalid_record_rows",
    "duplicate_records",
    "normalized_events",
    "book_level_rows",
    "market_token_rows",
    "timestamp_regressions",
    "negative_capture_latency",
)


def _utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def _empty_summary() -> dict[str, Any]:
    return {
        "input_files": 0,
        "input_bytes": 0,
        "raw_rows": 0,
        "invalid_json_rows": 0,
        "invalid_record_rows": 0,
        "duplicate_records": 0,
        "normalized_events": 0,
        "book_level_rows": 0,
        "market_token_rows": 0,
        "timestamp_regressions": 0,
        "negative_capture_latency": 0,
        "record_types": {},
        "event_types": {},
    }


def _new_manifest() -> dict[str, Any]:
    return {
        "manifest_version": MANIFEST_VERSION,
        "schema_version": SCHEMA_VERSION,
        "updated_at": _utc_now(),
        "next_sequence": 0,
        "processed_files": {},
        "summary": _empty_summary(),
    }


def _load_manifest(path: Path) -> dict[str, Any]:
    if not path.exists():
        return _new_manifest()
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid ETL manifest: {path}") from exc
    if not isinstance(manifest, dict):
        raise ValueError(f"ETL manifest must be a JSON object: {path}")
    if manifest.get("manifest_version", MANIFEST_VERSION) != MANIFEST_VERSION:
        raise ValueError(
            f"Unsupported manifest version: {manifest.get('manifest_version')}"
        )
    if manifest.get("schema_version", SCHEMA_VERSION) != SCHEMA_VERSION:
        raise ValueError(
            f"Unsupported analytics schema version: {manifest.get('schema_version')}"
        )
    manifest.setdefault("processed_files", {})
    manifest.setdefault("summary", _empty_summary())
    manifest.setdefault("next_sequence", 0)
    manifest.setdefault("manifest_version", MANIFEST_VERSION)
    manifest.setdefault("schema_version", SCHEMA_VERSION)
    return manifest


def _save_manifest(path: Path, manifest: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    manifest["updated_at"] = _utc_now()
    temporary = path.with_name(f".{path.name}.tmp")
    payload = json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    with temporary.open("w", encoding="utf-8") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    temporary.replace(path)


@contextlib.contextmanager
def _manifest_lock(output_dir: Path) -> Iterator[None]:
    """Serialize writers without ever uploading the lock as a data file."""

    output_dir.mkdir(parents=True, exist_ok=True)
    lock_path = output_dir / ".manifest.lock"
    with lock_path.open("a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _archive_key(path: Path, root: Path) -> str:
    try:
        relative = path.resolve().relative_to(root.resolve())
    except ValueError:
        relative = Path(path.name)
    return relative.as_posix()


def _date_partition(key: str, path: Path) -> str:
    date_match = DATE_RE.search(key)
    if date_match:
        return "-".join(date_match.groups())
    timestamp_match = TIMESTAMP_RE.search(key)
    if timestamp_match:
        stamp = timestamp_match.group(1).replace("Z", "+00:00")
        try:
            return dt.datetime.fromisoformat(stamp).date().isoformat()
        except ValueError:
            pass
    return dt.datetime.fromtimestamp(path.stat().st_mtime, dt.timezone.utc).date().isoformat()


def _archive_stem(key: str) -> str:
    for suffix in (".jsonl.gz", ".jsonl"):
        if key.endswith(suffix):
            key = key[: -len(suffix)]
            break
    normalized = re.sub(r"[^A-Za-z0-9_.-]+", "_", key.replace("/", "__"))
    return normalized or "archive"


def _sort_key(path: Path, root: Path) -> tuple[int, str, str]:
    key = _archive_key(path, root)
    match = TIMESTAMP_RE.search(key)
    return (0 if match else 1, match.group(1) if match else "", key)


def _safe_output_path(output_dir: Path, relative: str) -> Path:
    candidate = (output_dir / relative).resolve()
    try:
        candidate.relative_to(output_dir.resolve())
    except ValueError as exc:
        raise ValueError(f"Manifest output escapes analytics directory: {relative}") from exc
    return candidate


def _cleanup_outputs(output_dir: Path, outputs: Any) -> None:
    if not isinstance(outputs, dict):
        return
    for relative in outputs.values():
        if not isinstance(relative, str):
            continue
        target = _safe_output_path(output_dir, relative)
        target.unlink(missing_ok=True)


def _manifest_quality(
    report: dict[str, Any], key: str, outputs: dict[str, str], sequence_start: int
) -> dict[str, Any]:
    quality = dict(report)
    quality["input_files"] = [key]
    quality["input_file_count"] = 1
    quality["sequence_start"] = sequence_start
    quality["sequence_end"] = int(report.get("sequence_end", sequence_start))
    quality["outputs"] = {
        table: {"path": path, "rows": int(report["outputs"][table]["rows"])}
        for table, path in outputs.items()
    }
    return quality


def _merge_summary(summary: dict[str, Any], report: dict[str, Any]) -> None:
    summary["input_files"] = int(summary.get("input_files", 0)) + 1
    summary["input_bytes"] = int(summary.get("input_bytes", 0)) + int(
        report.get("input_bytes", 0)
    )
    for field in SUMMARY_COUNTERS:
        summary[field] = int(summary.get(field, 0)) + int(report.get(field, 0))
    for field in ("record_types", "event_types"):
        counter = Counter(summary.get(field, {}))
        counter.update(report.get(field, {}))
        summary[field] = dict(sorted(counter.items()))


def _common_root(paths: list[Path]) -> Path:
    if not paths:
        raise ValueError("No input files")
    common = Path(os.path.commonpath([str(path) for path in paths]))
    return common if common.is_dir() else common.parent


def incremental_convert(
    inputs: list[str | Path],
    output_dir: str | Path,
    *,
    input_root: str | Path | None = None,
    batch_size: int = 50_000,
) -> dict[str, Any]:
    """Convert unseen raw archives and return the updated manifest summary."""

    paths = resolve_jsonl_inputs(inputs)
    if not paths:
        raise ValueError("No input files")
    output = Path(output_dir).expanduser().resolve()
    root = (
        Path(input_root).expanduser().resolve()
        if input_root is not None
        else _common_root(paths)
    )
    manifest_path = output / "manifest.json"
    processed = 0
    skipped = 0
    outputs_created: list[str] = []

    with _manifest_lock(output):
        manifest = _load_manifest(manifest_path)
        processed_files = manifest["processed_files"]
        manifest.setdefault("summary", _empty_summary())
        summary = manifest["summary"]
        for path in sorted(paths, key=lambda item: _sort_key(item, root)):
            key = _archive_key(path, root)
            size = path.stat().st_size
            digest = _sha256(path)
            previous = processed_files.get(key)
            if isinstance(previous, dict) and previous.get("status") == "completed":
                if previous.get("size") != size or previous.get("sha256") != digest:
                    raise ValueError(f"Raw archive changed after processing: {key}")
                skipped += 1
                continue

            if isinstance(previous, dict) and previous.get("status") == "processing":
                if (
                    previous.get("size") not in (None, size)
                    or previous.get("sha256") not in (None, digest)
                ):
                    raise ValueError(f"Raw archive changed during processing: {key}")
                _cleanup_outputs(output, previous.get("outputs"))
                sequence_start = int(previous.get("sequence_start", manifest["next_sequence"]))
            else:
                if previous is not None:
                    raise ValueError(f"Unsupported manifest status for {key}")
                sequence_start = int(manifest["next_sequence"])

            partition = _date_partition(key, path)
            output_stem = f"{sequence_start:020d}__{_archive_stem(key)}"
            relative_outputs = {
                table: f"{table}/date={partition}/{output_stem}.parquet"
                for table in ("events", "book_levels", "markets")
            }
            # A pre-existing destination that is not tied to a retry is safer to
            # reject than to overwrite silently.
            if previous is None:
                for relative in relative_outputs.values():
                    if _safe_output_path(output, relative).exists():
                        raise FileExistsError(
                            f"Output already exists for untracked archive {key}: {relative}"
                        )
            processed_files[key] = {
                "status": "processing",
                "size": size,
                "sha256": digest,
                "sequence_start": sequence_start,
                "outputs": relative_outputs,
                "started_at": _utc_now(),
            }
            _save_manifest(manifest_path, manifest)

            try:
                with tempfile.TemporaryDirectory(
                    prefix=".incremental-etl-", dir=str(output.parent)
                ) as temporary:
                    converted_dir = Path(temporary) / "converted"
                    report = convert(
                        [path],
                        converted_dir,
                        batch_size=batch_size,
                        sequence_start=sequence_start,
                    )
                    for table, relative in relative_outputs.items():
                        source = converted_dir / f"{table}.parquet"
                        target = _safe_output_path(output, relative)
                        target.parent.mkdir(parents=True, exist_ok=True)
                        source.replace(target)
                        outputs_created.append(relative)
            except BaseException:
                # Leave the processing record in place.  A later invocation will
                # remove only these generated outputs and retry the same sequence.
                _save_manifest(manifest_path, manifest)
                raise

            quality = _manifest_quality(report, key, relative_outputs, sequence_start)
            sequence_end = int(report.get("sequence_end", sequence_start))
            processed_files[key] = {
                "status": "completed",
                "size": size,
                "sha256": digest,
                "processed_at": _utc_now(),
                "sequence_start": sequence_start,
                "sequence_end": sequence_end,
                "outputs": relative_outputs,
                "quality": quality,
            }
            manifest["next_sequence"] = sequence_end
            _merge_summary(summary, report)
            _save_manifest(manifest_path, manifest)
            processed += 1

        _save_manifest(manifest_path, manifest)

    return {
        "processed": processed,
        "skipped": skipped,
        "manifest": str(manifest_path),
        "outputs": outputs_created,
        "summary": manifest["summary"],
        "next_sequence": manifest["next_sequence"],
    }


def cli() -> None:
    parser = argparse.ArgumentParser(
        description="Incrementally convert raw JSONL archives to partitioned Parquet."
    )
    parser.add_argument("inputs", nargs="+", help="JSONL files or directories")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--input-root")
    parser.add_argument("--batch-size", type=int, default=50_000)
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    result = incremental_convert(
        args.inputs,
        args.output_dir,
        input_root=args.input_root,
        batch_size=args.batch_size,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    cli()
