from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from polymarket_collector.hf_backup import completed_archives, upload_once


class FakeApi:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.calls: list[dict[str, object]] = []

    def create_commit(self, **kwargs: object) -> None:
        self.calls.append(kwargs)
        if self.fail:
            raise RuntimeError("upload failed")


class HuggingFaceBackupTests(unittest.TestCase):
    def test_lists_only_completed_archives(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            archive = root / "a.jsonl.gz"
            archive.write_bytes(b"archive")
            os.utime(archive, (1, 1))
            (root / "active.jsonl.part").write_bytes(b"active")
            self.assertEqual(
                completed_archives(root, min_age_seconds=0), [archive]
            )

    def test_deletes_only_after_successful_commit(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            archive = root / "market_ws" / "a.jsonl.gz"
            archive.parent.mkdir()
            archive.write_bytes(b"archive")
            os.utime(archive, (1, 1))

            api = FakeApi()
            count = upload_once(
                data_dir=root,
                repo_id="user/data",
                token="token",
                min_age_seconds=0,
                api=api,  # type: ignore[arg-type]
            )
            self.assertEqual(count, 1)
            self.assertFalse(archive.exists())
            operations = api.calls[0]["operations"]
            self.assertEqual(operations[0].path_in_repo, "market_ws/a.jsonl.gz")

    def test_keeps_files_when_commit_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            archive = root / "a.jsonl.gz"
            archive.write_bytes(b"archive")
            os.utime(archive, (1, 1))
            with self.assertRaises(RuntimeError):
                upload_once(
                    data_dir=root,
                    repo_id="user/data",
                    token="token",
                    min_age_seconds=0,
                    api=FakeApi(fail=True),  # type: ignore[arg-type]
                )
            self.assertTrue(archive.exists())


if __name__ == "__main__":
    unittest.main()
