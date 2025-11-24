"""
Kaggle Dataset Uploader

Uploads processed data to Kaggle Datasets with version control.
Designed to be extensible for multiple datasets.

Usage:
    python kaggle_uploader.py                 # Upload all enabled datasets
    python kaggle_uploader.py --dataset crude_oil_brent  # Upload specific dataset
    python kaggle_uploader.py --list          # List all configured datasets
"""

import json
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml
from kaggle.api.kaggle_api_extended import KaggleApi


class KaggleUploader:
    """Handle uploading datasets to Kaggle with metadata management."""

    def __init__(self, config_path: str = "kaggle_config.yaml"):
        """Initialize uploader with configuration."""
        self.config_path = Path(config_path)
        self.config = self._load_config()
        self.api = self._initialize_kaggle_api()
        self.project_root = Path.cwd()

    def _load_config(self) -> dict[str, Any]:
        """Load and validate configuration file."""
        if not self.config_path.exists():
            print(f"✗ Config file not found: {self.config_path}")
            sys.exit(1)

        with open(self.config_path) as f:
            return yaml.safe_load(f)

    def _initialize_kaggle_api(self) -> KaggleApi:
        """Initialize Kaggle API with credentials."""
        api = KaggleApi()
        try:
            api.authenticate()
            print("✓ Kaggle API authenticated")
            return api
        except Exception as e:
            print(f"✗ Failed to authenticate Kaggle API: {e}")
            print("\nSetup Instructions:")
            print("1. Create Kaggle account: https://kaggle.com")
            print("2. Go to Account → Settings → API → Create New API Token")
            print("3. Save kaggle.json to ~/.kaggle/")
            print("4. chmod 600 ~/.kaggle/kaggle.json")
            sys.exit(1)

    def list_datasets(self) -> None:
        """List all configured datasets."""
        print("\n" + "=" * 80)
        print("Configured Kaggle Datasets")
        print("=" * 80)

        datasets = self.config.get("datasets", {})
        for name, config in datasets.items():
            status = "✓ ENABLED" if config.get("enabled") else "✗ DISABLED"
            print(f"\n{status} | {name}")
            print(f"  Title: {config.get('title')}")
            print(f"  Kaggle: {config.get('kaggle_dataset')}")
            print(f"  Files: {len(config.get('files', []))} file(s)")

        print("\n" + "=" * 80)

    def upload_dataset(self, dataset_name: str | None = None) -> None:
        """Upload dataset(s) to Kaggle."""
        datasets = self.config.get("datasets", {})

        if dataset_name:
            if dataset_name not in datasets:
                print(f"✗ Dataset not found: {dataset_name}")
                sys.exit(1)
            datasets_to_upload = {dataset_name: datasets[dataset_name]}
        else:
            datasets_to_upload = {
                name: config for name, config in datasets.items() if config.get("enabled", False)
            }

        if not datasets_to_upload:
            print("✗ No enabled datasets found")
            sys.exit(1)

        for dataset_name, config in datasets_to_upload.items():
            self._upload_single_dataset(dataset_name, config)

    def _upload_single_dataset(self, dataset_name: str, config: dict[str, Any]) -> None:
        """Upload a single dataset to Kaggle."""
        print(f"\n{'=' * 80}")
        print(f"Uploading: {dataset_name}")
        print(f"{'=' * 80}")

        kaggle_slug = config.get("kaggle_slug")
        files = config.get("files", [])

        # Validate files exist
        file_paths = []
        for file in files:
            file_path = self.project_root / file
            if not file_path.exists():
                print(f"✗ File not found: {file}")
                continue
            file_paths.append(file_path)

        if not file_paths:
            print("✗ No valid files found for upload")
            return

        print(f"📦 Files to upload ({len(file_paths)}):")
        for fp in file_paths:
            size_mb = fp.stat().st_size / (1024 * 1024)
            print(f"  • {fp.relative_to(self.project_root)} ({size_mb:.2f} MB)")

        # Create metadata
        metadata = self._create_metadata(dataset_name, config)

        # Upload to Kaggle
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                tmpdir_path = Path(tmpdir)

                # Copy files to temporary directory
                for file_path in file_paths:
                    dest = tmpdir_path / file_path.name
                    dest.write_bytes(file_path.read_bytes())

                # Write dataset metadata
                metadata_file = tmpdir_path / "dataset-metadata.json"
                with open(metadata_file, "w") as f:
                    json.dump(metadata, f, indent=2)

                print(f"\n📤 Uploading to Kaggle ({kaggle_slug})...")
                self.api.dataset_create_version(
                    folder=str(tmpdir_path),
                    version_notes=f"Auto-update: {datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')} UTC",
                    quiet=self.config.get("upload", {}).get("quiet", False),
                    delete_old_versions=True,
                )
                print(f"✓ Successfully uploaded: {dataset_name}")

        except Exception as e:
            print(f"✗ Upload failed: {e}")
            return

    def _create_metadata(self, dataset_name: str, config: dict[str, Any]) -> dict[str, Any]:
        """Create Kaggle dataset metadata."""
        files = config.get("files", [])
        file_names = [Path(f).name for f in files]

        return {
            "title": config.get("title", dataset_name),
            "id": f"datasets/deepu-peddineni/{config.get('kaggle_slug')}",
            "licenses": [{"name": config.get("license", "CC0")}],
            "keywords": config.get("keywords", []),
            "resources": [
                {
                    "path": fname,
                    "description": config.get("title", dataset_name),
                }
                for fname in file_names
            ],
            "datasetId": None,
            "ownerRef": "deepu-peddineni",
            "datasetSlug": config.get("kaggle_slug"),
            "isPrivate": not self.config.get("upload", {}).get("is_public", True),
        }


def main() -> None:
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Upload datasets to Kaggle",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python kaggle_uploader.py                              # Upload all enabled datasets
  python kaggle_uploader.py --dataset crude_oil_brent    # Upload specific dataset
  python kaggle_uploader.py --list                       # List all datasets
        """,
    )

    parser.add_argument(
        "--list",
        action="store_true",
        help="List all configured datasets",
    )
    parser.add_argument(
        "--dataset",
        type=str,
        help="Upload specific dataset by name",
    )
    parser.add_argument(
        "--config",
        type=str,
        default="kaggle_config.yaml",
        help="Path to configuration file",
    )

    args = parser.parse_args()

    uploader = KaggleUploader(config_path=args.config)

    if args.list:
        uploader.list_datasets()
    else:
        uploader.upload_dataset(dataset_name=args.dataset)


if __name__ == "__main__":
    main()
