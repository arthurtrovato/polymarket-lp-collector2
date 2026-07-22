from __future__ import annotations

import getpass
import os
from pathlib import Path

from huggingface_hub import HfApi


PROJECT_DIR = Path(__file__).resolve().parents[1]


def main() -> None:
    token = os.getenv("HF_TOKEN") or getpass.getpass("Hugging Face write token: ")
    if not token.startswith("hf_"):
        raise SystemExit("The token does not look like a Hugging Face token.")

    api = HfApi(token=token)
    account = api.whoami()
    namespace = account["name"]
    space_id = os.getenv(
        "HF_SPACE_REPO", f"{namespace}/polymarket-lp-collector"
    )
    dataset_id = os.getenv(
        "HF_DATASET_REPO", f"{namespace}/polymarket-l2-history"
    )

    api.create_repo(
        repo_id=dataset_id,
        repo_type="dataset",
        private=False,
        exist_ok=True,
    )
    api.upload_file(
        path_or_fileobj=PROJECT_DIR / "deploy/hf-dataset-card.md",
        path_in_repo="README.md",
        repo_id=dataset_id,
        repo_type="dataset",
        commit_message="Add dataset card",
    )

    api.create_repo(
        repo_id=space_id,
        repo_type="space",
        space_sdk="docker",
        private=False,
        exist_ok=True,
    )
    api.add_space_secret(
        repo_id=space_id,
        key="HF_TOKEN",
        value=token,
        description="Uploads collector archives to the public dataset.",
    )
    variables = {
        "HF_DATASET_REPO": dataset_id,
        "MAX_MARKETS": "40",
        "DATA_DIR": "/data",
        "HEALTH_HOST": "0.0.0.0",
        "PORT": "8080",
        "ROTATION_SECONDS": "3600",
        "MAX_FILE_MIB": "256",
        "BACKUP_INTERVAL_SECONDS": "3600",
        "BACKUP_MIN_AGE_SECONDS": "120",
        "REQUIRE_REMOTE_BACKUP": "true",
    }
    for key, value in variables.items():
        api.add_space_variable(repo_id=space_id, key=key, value=value)

    api.upload_folder(
        folder_path=PROJECT_DIR,
        repo_id=space_id,
        repo_type="space",
        ignore_patterns=[
            ".git/**",
            ".venv/**",
            ".venv-hf/**",
            ".tools/**",
            "data/**",
            "upload/**",
            "**/__pycache__/**",
            "*.zip",
            ".env",
            ".koyeb.yaml",
        ],
        commit_message="Deploy Polymarket LP collector",
    )
    api.request_space_hardware(repo_id=space_id, hardware="cpu-basic")
    runtime = api.get_space_runtime(repo_id=space_id)

    print(f"Space: https://huggingface.co/spaces/{space_id}")
    print(f"Dataset: https://huggingface.co/datasets/{dataset_id}")
    print(f"Hardware requested: {runtime.requested_hardware or runtime.hardware}")


if __name__ == "__main__":
    main()
