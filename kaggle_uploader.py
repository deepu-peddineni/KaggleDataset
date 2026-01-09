"""
Kaggle Dataset Uploader

Uploads processed data to Kaggle Datasets with version control.
Designed to be extensible for multiple datasets.

Usage:
    python kaggle_uploader.py                 # Upload all enabled datasets
    python kaggle_uploader.py --dataset crude_oil_brent  # Upload specific dataset
    python kaggle_uploader.py --list          # List all configured datasets
"""

import contextlib
import io
import json
import os
import subprocess
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml
from kaggle.api.kaggle_api_extended import KaggleApi


class KaggleUploader:
    """Handle uploading datasets to Kaggle with metadata management."""

    def __init__(
        self,
        config_path: str = "kaggle_config.yaml",
        dry_run: bool = False,
        confirm_yes: bool = False,
    ):
        """Initialize uploader with configuration.

        If `dry_run` is True, the uploader will not authenticate or call the Kaggle API;
        it will only validate files and write dataset metadata to the temporary folder.
        """
        self.config_path = Path(config_path)
        self.config = self._load_config()
        self.dry_run = bool(dry_run)
        self.confirm_yes = bool(confirm_yes)
        self.api = None if self.dry_run else self._initialize_kaggle_api()
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

    def _run_with_filtered_stderr(self, func):
        """Run `func` while capturing stderr and filter known noisy Kaggle client warnings.

        Returns the value returned by `func`. If `func` raises, the exception is propagated.
        """
        buf = io.StringIO()
        with contextlib.redirect_stderr(buf):
            result = func()

        stderr_out = buf.getvalue()
        if stderr_out:
            # Filter out the known 'token' bug message from older kaggle client versions
            filtered = []
            for line in stderr_out.splitlines():
                if "KaggleObject.from_dict() got an unexpected keyword argument 'token'" in line:
                    # suppress this known benign warning
                    continue
                filtered.append(line)

            if filtered:
                print("[kaggle-client-stderr]:")
                for line in filtered:
                    print(line)

        return result

    def _perform_version_or_create(self, tmpdir_path: Path, config: dict[str, Any]) -> None:
        """Attempt to create a dataset version, and fall back to creating the dataset if allowed.

        Raises the original exception if creation is not permitted or fails.
        """
        try:
            self._run_with_filtered_stderr(
                lambda: self.api.dataset_create_version(
                    folder=str(tmpdir_path),
                    version_notes=f"Auto-update: {datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')} UTC",
                    quiet=self.config.get("upload", {}).get("quiet", False),
                    delete_old_versions=True,
                )
            )
            return
        except Exception as e:
            msg = str(e).lower()
            create_if_missing = config.get("create_if_missing", False) or self.config.get(
                "upload", {}
            ).get("create_if_missing", False)

            if create_if_missing and (
                "not found" in msg
                or "404" in msg
                or "forbidden" in msg
                or "forbidden" in str(e).lower()
            ):
                if not (self.confirm_yes or self.dry_run):
                    raise RuntimeError(
                        "Dataset creation required but not confirmed. Re-run with --yes to allow creation."
                    ) from None

                is_public = self.config.get("upload", {}).get("is_public", True)
                self._run_with_filtered_stderr(
                    lambda: self.api.dataset_create_new(
                        folder=str(tmpdir_path),
                        public=is_public,
                        quiet=self.config.get("upload", {}).get("quiet", False),
                    )
                )
                return

            # Re-raise the original exception if we couldn't handle it
            raise

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

    def _collect_file_paths(self, files: list[str]) -> list[Path]:
        """Validate files exist and print a summary. Returns list of Path objects."""
        file_paths: list[Path] = []
        for file in files:
            file_path = self.project_root / file
            if not file_path.exists():
                print(f"✗ File not found: {file}")
                continue
            file_paths.append(file_path)

        if not file_paths:
            return []

        print(f"📦 Files to upload ({len(file_paths)}):")
        for fp in file_paths:
            size_mb = fp.stat().st_size / (1024 * 1024)
            print(f"  • {fp.relative_to(self.project_root)} ({size_mb:.2f} MB)")

        return file_paths

    def _resolve_image(self, config: dict[str, Any], metadata: dict) -> Path | None:
        """Resolve image path relative to project root and update metadata.image to basename."""
        image_rel = config.get("image")
        if not image_rel:
            return None

        candidate = self.project_root / image_rel
        if candidate.exists():
            metadata["image"] = candidate.name
            return candidate

        return None

    def _run_pre_upload_script(self, config: dict[str, Any]) -> bool:
        """Run pre-upload script specified in dataset config (path relative to project root).

        Config entry example:
          pre_upload:
            script: "CrudeOil/crude_oil_brent.py"
            allow_fail: false
            args: ["--flag"]

        Returns True on success, False on failure.
        """
        pre = config.get("pre_upload") or {}
        script_rel = pre.get("script")
        if not script_rel:
            return True

        script_path = self.project_root / script_rel
        if not script_path.exists():
            print(f"✗ Pre-upload script not found: {script_rel}")
            return pre.get("allow_fail", False)

        cmd = [sys.executable, str(script_path), *pre.get("args", [])]
        print(f"⏳ Running pre-upload script: {' '.join(cmd)}")
        try:
            subprocess.run(cmd, check=True, cwd=str(self.project_root))
            print("✓ Pre-upload script completed successfully")
            return True
        except subprocess.CalledProcessError as e:
            print(f"✗ Pre-upload script failed: {e}")
            if pre.get("allow_fail", False):
                print("→ Continuing despite pre-upload failure (allow_fail=True)")
                return True
            return False

    def _print_dry_run_info(
        self, metadata_file: Path, file_paths: list[Path], config: dict[str, Any], dataset_name: str
    ) -> None:
        print("-- DRY RUN -- no Kaggle API calls will be made")
        print(f"Would create dataset metadata at: {metadata_file}")
        print(f"Would upload files: {[p.name for p in file_paths]}")
        create_if_missing = config.get("create_if_missing", False) or self.config.get(
            "upload", {}
        ).get("create_if_missing", False)
        if create_if_missing:
            print(
                f"Would attempt to create dataset if missing: create_if_missing={create_if_missing}"
            )
        print(f"✓ Dry run completed for: {dataset_name}")

    def _prepare_upload_folder(
        self,
        tmpdir_path: Path,
        file_paths: list[Path],
        image_path: Path | None,
        metadata: dict,
        config: dict,
    ) -> Path:
        """Copy files (and optional image) into `tmpdir_path`, update metadata resources, write metadata file, and return its path."""
        # Copy files
        for file_path in file_paths:
            dest = tmpdir_path / file_path.name
            dest.write_bytes(file_path.read_bytes())

        # Copy image if present
        if image_path:
            img_dest = tmpdir_path / image_path.name
            img_dest.write_bytes(image_path.read_bytes())
            # Ensure image is represented in resources (if not already)
            existing_paths = [r.get("path") for r in metadata.get("resources", [])]
            if image_path.name not in existing_paths:
                metadata.setdefault("resources", []).append(
                    {
                        "path": image_path.name,
                        "description": config.get("subtitle", "Thumbnail image"),
                    }
                )

        # Write dataset metadata
        metadata_file = tmpdir_path / "dataset-metadata.json"
        with open(metadata_file, "w") as f:
            json.dump(metadata, f, indent=2)

        return metadata_file

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

    def _upload_to_kaggle(
        self, tmpdir_path: Path, kaggle_slug: str, config: dict[str, Any], dataset_name: str
    ) -> None:
        """Handle Kaggle API upload (version creation or dataset creation)."""
        try:
            self._run_with_filtered_stderr(
                lambda: self.api.dataset_create_version(
                    folder=str(tmpdir_path),
                    version_notes=f"Auto-update: {datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')} UTC",
                    quiet=self.config.get("upload", {}).get("quiet", False),
                    delete_old_versions=True,
                )
            )
            print(f"✓ Successfully uploaded: {dataset_name}")
        except Exception as e:
            self._handle_upload_error(e, tmpdir_path, kaggle_slug, config, dataset_name)

    def _handle_upload_error(
        self,
        error: Exception,
        tmpdir_path: Path,
        kaggle_slug: str,
        config: dict[str, Any],
    ) -> None:
        """Handle upload error with fallback to dataset creation if configured."""
        msg = str(error).lower()
        create_if_missing = config.get("create_if_missing", False) or self.config.get(
            "upload", {}
        ).get("create_if_missing", False)

        if create_if_missing and ("not found" in msg or "404" in msg or "forbidden" in msg):
            print("⚠️  Dataset version creation failed — attempting to create dataset...")
            try:
                is_public = self.config.get("upload", {}).get("is_public", True)
                if not (self.confirm_yes or self.dry_run):
                    print(
                        "✗ Dataset creation required but not confirmed. Re-run with `--yes` to allow creation."
                    )
                    return

                self._run_with_filtered_stderr(
                    lambda: self.api.dataset_create_new(
                        folder=str(tmpdir_path),
                        public=is_public,
                        quiet=self.config.get("upload", {}).get("quiet", False),
                    )
                )
                print(f"✓ Dataset created: {kaggle_slug}")
            except Exception as e2:
                print(f"✗ Failed to create dataset: {e2}")
                print(f"✗ Original error: {error}")
        else:
            print(f"✗ Upload failed: {error}")

    def _upload_single_dataset(self, dataset_name: str, config: dict[str, Any]) -> None:
        """Upload a single dataset to Kaggle."""
        print(f"\n{'=' * 80}")
        print(f"Uploading: {dataset_name}")
        print(f"{'=' * 80}")

        kaggle_slug = config.get("kaggle_slug")
        files = config.get("files", [])

        # Run pre-upload script if configured (to generate/update files)
        if not self._run_pre_upload_script(config):
            print("✗ Aborting upload due to pre-upload script failure")
            return

        # Validate files and print summary
        file_paths = self._collect_file_paths(files)
        if not file_paths:
            print("✗ No valid files found for upload")
            return

        # Create metadata
        metadata = self._create_metadata(dataset_name, config)

        # Resolve image path (project-root relative) and prepare to include it in the upload
        image_rel = config.get("image")
        image_path = None
        if image_rel:
            candidate = self.project_root / image_rel
            if candidate.exists():
                image_path = candidate
                # set image field in metadata to the basename so Kaggle reads it from the resources
                metadata["image"] = image_path.name

        # Upload to Kaggle
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                tmpdir_path = Path(tmpdir)

                # Prepare upload folder and write metadata
                metadata_file = self._prepare_upload_folder(
                    tmpdir_path, file_paths, image_path, metadata, config
                )

                print(f"\n📤 Uploading to Kaggle ({kaggle_slug})...")

                # Dry-run: show what would happen
                if self.dry_run:
                    self._print_dry_run_info(metadata_file, file_paths, config, dataset_name)
                    return

                # Real upload
                self._upload_to_kaggle(tmpdir_path, kaggle_slug, config, dataset_name)

        except Exception as e:
            print(f"✗ Upload failed: {e}")
            return

    def _get_owner(self, config: dict[str, Any]) -> str:
        """Determine dataset owner with multiple fallbacks."""
        # 1. dataset-specific `kaggle_owner`
        owner = config.get("kaggle_owner")
        if owner:
            return owner

        # 2. full `kaggle_dataset` (extract user from 'user/slug')
        kaggle_dataset_full = config.get("kaggle_dataset")
        if kaggle_dataset_full and "/" in kaggle_dataset_full:
            return kaggle_dataset_full.split("/")[0]

        # 3. global upload owner (upload.owner)
        owner = self.config.get("upload", {}).get("owner")
        if owner:
            return owner

        # 4. env var KAGGLE_USERNAME
        owner = os.environ.get("KAGGLE_USERNAME")
        if owner:
            return owner

        # 5. username field from ~/.kaggle/kaggle.json
        try:
            kaggle_file = Path.home() / ".kaggle" / "kaggle.json"
            if kaggle_file.exists():
                with open(kaggle_file) as kf:
                    data = json.load(kf)
                    owner = data.get("username") or data.get("user")
                    if owner:
                        return owner
        except Exception:
            pass

        print(
            "✗ Could not determine Kaggle owner — set `kaggle_owner` in dataset config or export KAGGLE_USERNAME or add upload.owner"
        )
        return ""

    def _build_resource_schema(self, config: dict[str, Any]) -> dict | None:
        """Build schema for resource fields."""
        columns = config.get("columns", [])
        if not columns:
            return None

        fields = []
        for idx, col in enumerate(columns):
            field = {
                "order": idx,
                "name": col.get("name", ""),
                "type": col.get("type", "string"),
                "description": col.get("description", ""),
            }
            fields.append(field)

        return {"fields": fields}

    def _build_resources(self, files: list[str], dataset_name: str, config: dict[str, Any]) -> list:
        """Build resources list with file descriptions and schema."""
        resources = []
        schema = self._build_resource_schema(config)

        for full_path in files:
            fname = Path(full_path).name
            file_desc = (
                config.get("file_info", {}).get(full_path, {}).get("description")
                or config.get("file_info", {}).get(fname, {}).get("description")
                or f"{dataset_name} - {fname}"
            )

            resource = {
                "path": fname,
                "description": file_desc,
            }

            if schema:
                resource["schema"] = schema

            resources.append(resource)

        return resources

    def _create_metadata(self, dataset_name: str, config: dict[str, Any]) -> dict[str, Any]:
        """Create Kaggle dataset metadata."""
        files = config.get("files", [])
        owner = self._get_owner(config)

        # Map license strings to Kaggle format
        license_str = config.get("license", "CC0-1.0")
        license_map = {
            "MIT": "CC0-1.0",
            "CC0": "CC0-1.0",
        }
        license_name = license_map.get(license_str, license_str)

        # Build resources with file descriptions and schema
        resources = self._build_resources(files, dataset_name, config)

        metadata = {
            "title": config.get("title", dataset_name),
            "id": f"{owner}/{config.get('kaggle_slug')}",
            "licenses": [{"name": license_name}],
        }

        # Add optional fields only if they exist
        subtitle = config.get("subtitle")
        if subtitle:
            metadata["subtitle"] = subtitle

        description = config.get("description")
        if description:
            metadata["description"] = description

        keywords = config.get("keywords", [])
        if keywords:
            metadata["keywords"] = keywords

        if resources:
            metadata["resources"] = resources

        # Add update frequency if specified
        update_freq = config.get("updateFrequency")
        if update_freq:
            metadata["updateFrequency"] = update_freq

        return metadata


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

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do a dry run without calling the Kaggle API",
    )

    parser.add_argument(
        "--yes",
        action="store_true",
        help="Automatically confirm dataset creation (non-interactive)",
    )

    args = parser.parse_args()

    uploader = KaggleUploader(config_path=args.config, dry_run=args.dry_run, confirm_yes=args.yes)

    if args.list:
        uploader.list_datasets()
    else:
        uploader.upload_dataset(dataset_name=args.dataset)


if __name__ == "__main__":
    main()
