#!/usr/bin/env python3
"""
Weekly Energy Trading dataset publisher.

Designed to be driven by cron, systemd timers, or GitHub Actions:

    python EnergyTrading/publish_weekly.py
    python EnergyTrading/publish_weekly.py --dry-run
    python EnergyTrading/publish_weekly.py --full

The script runs the data pipeline, then calls the repository's Kaggle uploader
for the ``energy_trading`` dataset. It exits non-zero if either step fails, so
cron/CI can alert on it.
"""

from __future__ import annotations

import argparse
import os
import shlex
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path


MODULE_DIR = Path(__file__).resolve().parent
REPO_ROOT = MODULE_DIR.parent
PIPELINE = MODULE_DIR / "energy_trading_pipeline.py"
UPLOADER = REPO_ROOT / "kaggle_uploader.py"


def _run(command: list[str]) -> None:
    """Run a subprocess, echoing the command first."""
    print(f"\n$ {shlex.join(command)}")
    subprocess.run(command, cwd=REPO_ROOT, check=True)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Run the Energy Trading pipeline and publish it to Kaggle.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Cron example (every Monday at 03:17 UTC):
  17 3 * * 1 cd /path/to/KaggleDataset && \\
    .venv/bin/python EnergyTrading/publish_weekly.py >> energy_trading.log 2>&1

Examples:
  python EnergyTrading/publish_weekly.py --dry-run
  python EnergyTrading/publish_weekly.py --full
  python EnergyTrading/publish_weekly.py --skip-pipeline
        """,
    )
    parser.add_argument(
        "--dataset",
        default="energy_trading",
        help="Dataset key in kaggle_config.yaml (default: energy_trading).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Build the data but do not call the Kaggle uploader.",
    )
    parser.add_argument(
        "--skip-pipeline",
        action="store_true",
        help="Skip the download step and upload the existing files.",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Rebuild the full history instead of doing an incremental update.",
    )
    parser.add_argument(
        "--print-cron",
        action="store_true",
        help="Print a ready-to-use crontab line and exit.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run the weekly pipeline and publish step."""
    args = parse_args(argv)

    if args.print_cron:
        print(
            "17 3 * * 1 "
            f"cd {shlex.quote(str(REPO_ROOT))} && "
            f"{shlex.quote(sys.executable)} EnergyTrading/publish_weekly.py "
            ">> energy_trading.log 2>&1"
        )
        return 0

    started = datetime.now(UTC)
    print("=" * 78)
    print("Energy Trading - weekly publish")
    print("=" * 78)
    print(f"Started (UTC): {started:%Y-%m-%d %H:%M:%S}")
    print(f"Repository:    {REPO_ROOT}")

    if not args.skip_pipeline:
        pipeline_command = [sys.executable, str(PIPELINE)]
        if args.full:
            pipeline_command.append("--full")
        _run(pipeline_command)

    if args.dry_run:
        print("\nDry run: skipping Kaggle upload.")
        print(
            "Would run: "
            + shlex.join(
                [
                    sys.executable,
                    str(UPLOADER),
                    "--dataset",
                    args.dataset,
                    "--yes",
                ]
            )
        )
        return 0

    # The pipeline already ran above, so the uploader must not run it again.
    os.environ["SKIP_PRE_UPLOAD"] = "1"
    _run(
        [
            sys.executable,
            str(UPLOADER),
            "--dataset",
            args.dataset,
            "--yes",
        ]
    )

    elapsed = (datetime.now(UTC) - started).total_seconds()
    print("\n" + "=" * 78)
    print(f"Published '{args.dataset}' in {elapsed:.1f}s")
    print("=" * 78)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
