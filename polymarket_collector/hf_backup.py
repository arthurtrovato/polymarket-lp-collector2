from __future__ import annotations

import argparse
import logging
import os
import time
from pathlib import Path

from huggingface_hub import CommitOperationAdd, HfApi


LOGGER = logging.getLogger(__name__)


def completed_archives(
    data_dir: Path,
    *,
    min_age_seconds: int,
    max_files: int = 80,
) -> list[Path]:
    cutoff = time.time() - min_age_seconds
    paths = [
        path
        for path in data_dir.rglob("*.jsonl.gz")
        if path.is_file() and path.stat().st_mtime <= cutoff
    ]
    return sorted(paths)[:max_files]


def upload_once(
    *,
    data_dir: Path,
    repo_id: str,
    token: str,
    min_age_seconds: int,
    api: HfApi | None = None,
) -> int:
    paths = completed_archives(
        data_dir,
        min_age_seconds=min_age_seconds,
    )
    if not paths:
        return 0

    client = api or HfApi(token=token)
    operations = [
        CommitOperationAdd(
            path_in_repo=path.relative_to(data_dir).as_posix(),
            path_or_fileobj=path,
        )
        for path in paths
    ]
    client.create_commit(
        repo_id=repo_id,
        repo_type="dataset",
        operations=operations,
        commit_message=f"Add {len(paths)} Polymarket archive(s)",
    )
    for path in paths:
        path.unlink()
    LOGGER.info("Uploaded and removed %d local archive(s)", len(paths))
    return len(paths)


def run_loop() -> None:
    data_dir = Path(os.getenv("DATA_DIR", "/data")).resolve()
    repo_id = os.environ["HF_DATASET_REPO"]
    token = os.environ["HF_TOKEN"]
    interval = int(os.getenv("BACKUP_INTERVAL_SECONDS", "3600"))
    min_age = int(os.getenv("BACKUP_MIN_AGE_SECONDS", "120"))
    if interval < 60 or min_age < 0:
        raise ValueError("Invalid backup interval or minimum age")

    api = HfApi(token=token)
    while True:
        try:
            upload_once(
                data_dir=data_dir,
                repo_id=repo_id,
                token=token,
                min_age_seconds=min_age,
                api=api,
            )
        except Exception:
            LOGGER.exception("Hugging Face backup failed; retrying later")
        time.sleep(interval)


def cli() -> None:
    parser = argparse.ArgumentParser(
        description="Upload completed collector archives to a HF dataset."
    )
    parser.add_argument("--loop", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    if args.loop:
        run_loop()
        return

    upload_once(
        data_dir=Path(os.getenv("DATA_DIR", "/data")).resolve(),
        repo_id=os.environ["HF_DATASET_REPO"],
        token=os.environ["HF_TOKEN"],
        min_age_seconds=int(os.getenv("BACKUP_MIN_AGE_SECONDS", "120")),
    )


if __name__ == "__main__":
    cli()
