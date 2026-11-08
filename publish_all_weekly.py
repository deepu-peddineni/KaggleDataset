#!/usr/bin/env python3
"""Build and publish every enabled Kaggle dataset with one command.

This is the single entry point for the weekly schedule. It runs each dataset's
configured pre-upload pipeline once, then uploads every selected dataset to
Kaggle without rebuilding anything (``SKIP_PRE_UPLOAD=1``).

    python publish_all_weekly.py --yes
    python publish_all_weekly.py --yes --dataset energy_trading,crude_oil_brent
    python publish_all_weekly.py --dry-run
    python publish_all_weekly.py --skip-build --yes

The exit code is 0 only when every selected dataset was built and published, so
cron and GitHub Actions can alert on partial failures.
"""

from __future__ import annotations

import argparse
import os
import shlex
import subprocess
import sys
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from kaggle_uploader import KaggleUploader


REPO_ROOT = Path(__file__).resolve().parent
DEFAULT_CONFIG_PATH = REPO_ROOT / "kaggle_config.yaml"

Runner = Callable[..., subprocess.CompletedProcess]


@dataclass
class DatasetResult:
    """Outcome of building and uploading one dataset."""

    name: str
    built: bool = False
    build_skipped: bool = False
    build_seconds: float = 0.0
    upload_seconds: float = 0.0
    uploaded: bool = False
    error: str | None = None

    @property
    def ok(self) -> bool:
        """Whether the dataset finished without an error."""
        return self.error is None


@dataclass
class PublishSummary:
    """Aggregated results for one run."""

    results: list[DatasetResult] = field(default_factory=list)

    @property
    def failed(self) -> list[DatasetResult]:
        """Datasets that did not finish successfully."""
        return [result for result in self.results if not result.ok]


def load_config(config_path: Path) -> dict[str, Any]:
    """Load the Kaggle upload configuration file."""
    if not config_path.exists():
        raise SystemExit(f"Config file not found: {config_path}")
    with config_path.open(encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def select_datasets(
    config: dict[str, Any], names: Sequence[str] | None = None
) -> dict[str, dict[str, Any]]:
    """Return the explicitly named datasets, or every enabled dataset."""
    datasets: dict[str, dict[str, Any]] = config.get("datasets") or {}

    if names:
        missing = [name for name in names if name not in datasets]
        if missing:
            raise SystemExit(f"Unknown dataset(s): {', '.join(sorted(missing))}")
        return {name: datasets[name] for name in names}

    enabled = {
        name: dataset_config
        for name, dataset_config in datasets.items()
        if dataset_config.get("enabled", False)
    }
    if not enabled:
        raise SystemExit("No enabled datasets found in the config file.")
    return enabled


def build_dataset(
    name: str,
    config: dict[str, Any],
    *,
    runner: Runner | None = None,
) -> DatasetResult:
    """Run a dataset's pre-upload pipeline once."""
    result = DatasetResult(name=name)
    pre_upload = config.get("pre_upload") or {}
    script_rel = pre_upload.get("script")

    if not script_rel:
        result.built = True
        return result

    script = REPO_ROOT / script_rel
    if not script.exists():
        result.error = f"pre-upload script not found: {script_rel}"
        return result

    command = ["uv", "run", str(script)]
    command.extend(str(arg) for arg in pre_upload.get("args", []))
    print(f"  $ {shlex.join(command)}")

    started = time.monotonic()
    run = runner or subprocess.run
    completed = run(command, cwd=REPO_ROOT, check=False)
    result.build_seconds = time.monotonic() - started

    if completed.returncode != 0:
        result.error = f"build failed with exit code {completed.returncode}"
        return result

    result.built = True
    return result


def _parse_dataset_names(values: Sequence[str] | None) -> list[str] | None:
    """Flatten comma-separated and repeated ``--dataset`` values, keeping order."""
    if not values:
        return None

    names: list[str] = []
    for value in values:
        for part in value.split(","):
            name = part.strip()
            if name and name not in names:
                names.append(name)
    return names or None


def _is_allowed_to_fail(config: dict[str, Any]) -> bool:
    return bool((config.get("pre_upload") or {}).get("allow_fail", False))


def _publish_one(
    name: str,
    config: dict[str, Any],
    *,
    uploader: KaggleUploader | None,
    skip_build: bool,
    skip_upload: bool,
) -> DatasetResult:
    """Build and upload a single dataset, recording any failure."""
    print("-" * 78)
    print(f"Dataset: {name}")
    print("-" * 78)

    if skip_build:
        result = DatasetResult(name=name, built=True, build_skipped=True)
        print("  · build skipped (--skip-build)")
    else:
        print("  · building data")
        result = build_dataset(name, config)

    if not result.ok and _is_allowed_to_fail(config):
        print(f"  ! build failed but allow_fail=true: {result.error}")
        result.error = None

    if not result.ok:
        print(f"  ✗ {result.error}; skipping upload")
        return result

    if skip_upload or uploader is None:
        print("  · upload skipped")
        return result

    print("  · uploading to Kaggle")
    started = time.monotonic()
    try:
        published = uploader.upload_dataset(dataset_name=name)
    except SystemExit as exc:
        result.error = f"upload exited with status {exc.code}"
        return result
    result.upload_seconds = time.monotonic() - started
    result.uploaded = bool(published.get(name, False))
    if not result.uploaded:
        result.error = "upload failed"
    return result


def publish_all_datasets(
    *,
    config_path: Path | str = DEFAULT_CONFIG_PATH,
    datasets: Sequence[str] | None = None,
    skip_build: bool = False,
    skip_upload: bool = False,
    dry_run: bool = False,
    confirm_yes: bool = False,
) -> int:
    """Build and publish every selected dataset. Returns a process exit code."""
    config_path = Path(config_path)
    config = load_config(config_path)
    selected = select_datasets(config, datasets)

    print("=" * 78)
    print("Kaggle dataset update")
    print("=" * 78)
    print(f"Config:   {config_path}")
    print(f"Datasets: {', '.join(selected)}")
    print(f"Build:    {'skipped' if skip_build else 'yes'}")
    upload_state = "skipped" if skip_upload else ("dry-run" if dry_run else "yes")
    print(f"Upload:   {upload_state}")
    print()

    uploader = None
    if not skip_upload:
        # Authenticate before the long build phase so bad credentials fail fast.
        uploader = KaggleUploader(
            config_path=str(config_path),
            dry_run=dry_run,
            confirm_yes=confirm_yes,
            project_root=REPO_ROOT,
        )

    # The pipelines already ran above, so stop the uploader rebuilding them.
    previous_skip = os.environ.get("SKIP_PRE_UPLOAD")
    os.environ["SKIP_PRE_UPLOAD"] = "1"
    try:
        results = [
            _publish_one(
                name,
                dataset_config,
                uploader=uploader,
                skip_build=skip_build,
                skip_upload=skip_upload,
            )
            for name, dataset_config in selected.items()
        ]
    finally:
        if previous_skip is None:
            os.environ.pop("SKIP_PRE_UPLOAD", None)
        else:
            os.environ["SKIP_PRE_UPLOAD"] = previous_skip

    summary = PublishSummary(results=results)
    _print_summary(summary)
    return 0 if not summary.failed else 1


def _print_summary(summary: PublishSummary) -> None:
    print()
    print("=" * 78)
    print("Summary")
    print("=" * 78)
    for result in summary.results:
        status = "✓" if result.ok else "✗"
        details: list[str] = []
        if result.build_skipped:
            details.append("build skipped")
        elif result.built:
            details.append(f"build {result.build_seconds:.1f}s")
        if result.uploaded:
            details.append(f"upload {result.upload_seconds:.1f}s")
        if result.error:
            details.append(result.error)
        print(f"  {status} {result.name:40s} {' · '.join(details)}")

    print()
    if summary.failed:
        print(f"{len(summary.failed)} of {len(summary.results)} dataset(s) failed.")
    else:
        print(f"All {len(summary.results)} dataset(s) updated successfully.")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Build and publish all enabled Kaggle datasets.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python publish_all_weekly.py --yes
  python publish_all_weekly.py --yes --dataset energy_trading,crude_oil_brent
  python publish_all_weekly.py --skip-build --yes
  python publish_all_weekly.py --dry-run
        """,
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help="Path to the Kaggle configuration file.",
    )
    parser.add_argument(
        "--dataset",
        action="append",
        default=None,
        help="Dataset name(s) to update. Repeat or comma-separate. Defaults to all enabled.",
    )
    parser.add_argument(
        "--skip-build",
        action="store_true",
        help="Upload the existing files without running the data pipelines.",
    )
    parser.add_argument(
        "--skip-upload",
        action="store_true",
        help="Build the data without uploading to Kaggle.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate files without calling the Kaggle API.",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Allow dataset creation without an interactive prompt.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the weekly update."""
    args = parse_args(argv)
    return publish_all_datasets(
        config_path=args.config,
        datasets=_parse_dataset_names(args.dataset),
        skip_build=args.skip_build,
        skip_upload=args.skip_upload,
        dry_run=args.dry_run,
        confirm_yes=args.yes,
    )


if __name__ == "__main__":
    sys.exit(main())
