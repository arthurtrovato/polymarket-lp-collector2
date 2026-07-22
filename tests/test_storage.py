from __future__ import annotations

import gzip
import json
import tempfile
import unittest
from pathlib import Path

from polymarket_collector.storage import RotatingJsonlWriter


class StorageTests(unittest.IsolatedAsyncioTestCase):
    async def test_rotates_and_compresses_jsonl(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            writer = RotatingJsonlWriter(
                root,
                "test",
                rotation_seconds=3600,
                max_file_bytes=1,
                flush_interval_seconds=1,
            )
            await writer.start()
            await writer.write({"hello": "world"})
            await writer.close()

            files = list(root.rglob("*.jsonl.gz"))
            self.assertEqual(len(files), 1)
            with gzip.open(files[0], "rt", encoding="utf-8") as handle:
                self.assertEqual(json.loads(handle.readline()), {"hello": "world"})
            self.assertEqual(list(root.rglob("*.part")), [])

    async def test_recovers_part_file_on_start(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            part = root / "2026/07/22/crashed.jsonl.part"
            part.parent.mkdir(parents=True)
            part.write_text('{"recovered":true}\n', encoding="utf-8")

            writer = RotatingJsonlWriter(
                root,
                "test",
                rotation_seconds=3600,
                max_file_bytes=1024,
                flush_interval_seconds=1,
            )
            await writer.start()
            await writer.close()

            recovered = list(root.rglob("*.jsonl.gz"))
            self.assertEqual(len(recovered), 1)
            with gzip.open(recovered[0], "rt", encoding="utf-8") as handle:
                self.assertEqual(json.loads(handle.readline()), {"recovered": True})


if __name__ == "__main__":
    unittest.main()

