#!/usr/bin/env python3
"""Build new analytics partitions from the public raw Hugging Face dataset.

The job is intentionally stateless apart from ``analytics/manifest.json`` in
the dataset.  A fresh GitHub runner downloads that manifest and only the raw
archives that are not marked completed, then commits the new Parquet files
and the updated manifest in one Hugging Face commit.
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

from huggingface_hub import CommitOperationAdd, HfApi, hf_hub_download
from huggingface_hub.utils import EntryNotFoundError

from polymarket_collector.incremental import incremental_convert


REPO_ID = os.environ.get("HF_DATASET_REPO", "Houroux/polymarket-l2-history")
ANALYTICS_PREFIX = os.environ.get("ANALYTICS_PREFIX", "analytics").strip("/")


def _required_token() -> str:
    token = os.environ.get("HF_TOKEN", "").strip()
    if not token:
        raise RuntimeError("HF_TOKEN is required to update the analytics dataset")
    return token


def _max_input_files() -> int:
    value = int(os.environ.get("MAX_INPUT_FILES", "24"))
    if value < 1:
        raise ValueError("MAX_INPUT_FILES must be positive")
    return value


def _timestamp_sort_key(path: str) -> tuple[int, str, str]:
    import re

    match = re.search(r"(\d{8}T\d{6}(?:\.\d+)?Z)", path)
    return (0 if match else 1, match.group(1) if match else "", path)


def _download_manifest(repo_id: str, token: str, work_dir: Path) -> Path | None:
    try:
        downloaded = Path(
            hf_hub_download(
                repo_id=repo_id,
                filename=f"{ANALYTICS_PREFIX}/manifest.json",
                repo_type="dataset",
                token=token,
                local_dir=str(work_dir),
            )
        )
    except EntryNotFoundError:
        return None
    expected = work_dir / ANALYTICS_PREFIX / "manifest.json"
    expected.parent.mkdir(parents=True, exist_ok=True)
    if downloaded.resolve() != expected.resolve():
        shutil.copyfile(downloaded, expected)
    return expected


def _completed_keys(manifest_path: Path | None) -> set[str]:
    if manifest_path is None:
        return set()
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return set()
    files = manifest.get("processed_files", {}) if isinstance(manifest, dict) else {}
    if not isinstance(files, dict):
        return set()
    return {
        str(key)
        for key, value in files.items()
        if isinstance(value, dict) and value.get("status") == "completed"
    }


def _upload_analytics(api: HfApi, token: str, analytics_dir: Path) -> str:
    operations: list[CommitOperationAdd] = []
    for path in sorted(analytics_dir.rglob("*")):
        if not path.is_file() or path.name.endswith(".tmp") or path.name == ".manifest.lock":
            continue
        relative = path.relative_to(analytics_dir).as_posix()
        operations.append(
            CommitOperationAdd(
                path_in_repo=f"{ANALYTICS_PREFIX}/{relative}",
                path_or_fileobj=str(path),
            )
        )
    if not operations:
        raise RuntimeError("Incremental ETL produced no uploadable files")
    last_error: Exception | None = None
    for attempt in range(1, 4):
        try:
            commit = api.create_commit(
                repo_id=REPO_ID,
                repo_type="dataset",
                operations=operations,
                commit_message="Update incremental analytics partitions and manifest",
                token=token,
            )
            return str(commit.commit_url or commit.pr_url or commit)
        except Exception as exc:  # retries cover concurrent raw collector commits
            last_error = exc
            if attempt < 3:
                time.sleep(5 * attempt)
    assert last_error is not None
    raise last_error


def main() -> int:
    token = _required_token()
    max_files = _max_input_files()
    api = HfApi(token=token)
    with tempfile.TemporaryDirectory(prefix="polymarket-incremental-etl-") as temporary:
        work_dir = Path(temporary)
        analytics_dir = work_dir / ANALYTICS_PREFIX
        raw_dir = work_dir / "raw"
        analytics_dir.mkdir(parents=True, exist_ok=True)
        raw_dir.mkdir(parents=True, exist_ok=True)

        manifest_path = _download_manifest(REPO_ID, token, work_dir)
        completed = _completed_keys(manifest_path)
        remote_files = api.list_repo_files(repo_id=REPO_ID, repo_type="dataset", token=token)
        candidates = sorted(
            (
                path
                for path in remote_files
                if path.endswith((".jsonl", ".jsonl.gz"))
                and not path.startswith(f"{ANALYTICS_PREFIX}/")
                and path not in completed
            ),
            key=_timestamp_sort_key,
        )[:max_files]

        downloaded_paths: list[Path] = []
        for remote_path in candidates:
            local_path = Path(
                hf_hub_download(
                    repo_id=REPO_ID,
                    filename=remote_path,
                    repo_type="dataset",
                    token=token,
                    local_dir=str(raw_dir),
                )
            )
            downloaded_paths.append(local_path)

        if not downloaded_paths:
            print(
                json.dumps(
                    {"processed": 0, "skipped": 0, "candidates": 0, "message": "up to date"},
                    sort_keys=True,
                )
            )
            return 0

        result = incremental_convert(
            downloaded_paths,
            analytics_dir,
            input_root=raw_dir,
        )
        if result["processed"]:
            result["commit"] = _upload_analytics(api, token, analytics_dir)
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"incremental ETL failed: {exc}", file=sys.stderr)
        raise
