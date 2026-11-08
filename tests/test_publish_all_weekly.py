"""Tests for the all-datasets weekly publisher."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import ClassVar

import pytest
import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

import publish_all_weekly as publisher  # noqa: E402


EXISTING_SCRIPT = "CrudeOil/crude_oil_brent.py"


def _completed(returncode: int = 0) -> subprocess.CompletedProcess:
    return subprocess.CompletedProcess(args=["uv"], returncode=returncode)


def _built_only(name: str, _config: dict) -> publisher.DatasetResult:
    return publisher.DatasetResult(name=name, built=True)


def _failed_build(name: str, _config: dict) -> publisher.DatasetResult:
    return publisher.DatasetResult(name=name, error="build failed with exit code 1")


def _write_config(tmp_path: Path, datasets: dict) -> Path:
    path = tmp_path / "kaggle_config.yaml"
    path.write_text(yaml.safe_dump({"datasets": datasets}), encoding="utf-8")
    return path


@pytest.fixture
def config() -> dict:
    return {
        "datasets": {
            "enabled_one": {
                "enabled": True,
                "pre_upload": {"script": EXISTING_SCRIPT},
            },
            "enabled_two": {
                "enabled": True,
                "pre_upload": {"script": EXISTING_SCRIPT},
            },
            "disabled_one": {
                "enabled": False,
                "pre_upload": {"script": EXISTING_SCRIPT},
            },
        }
    }


class FakeUploader:
    """Stand-in for KaggleUploader that records calls."""

    instances: ClassVar[list[FakeUploader]] = []
    outcome: ClassVar[dict[str, bool]] = {}

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.calls: list[str] = []
        self.skip_pre_upload_seen: list[str | None] = []
        FakeUploader.instances.append(self)

    def upload_dataset(self, dataset_name: str | None = None) -> dict[str, bool]:
        assert dataset_name is not None
        self.calls.append(dataset_name)
        self.skip_pre_upload_seen.append(os.environ.get("SKIP_PRE_UPLOAD"))
        return {dataset_name: FakeUploader.outcome.get(dataset_name, True)}


@pytest.fixture(autouse=True)
def _reset_fake_uploader():
    FakeUploader.instances.clear()
    FakeUploader.outcome = {}
    yield
    FakeUploader.instances.clear()
    FakeUploader.outcome = {}


class TestSelection:
    def test_defaults_to_enabled_datasets(self, config):
        selected = publisher.select_datasets(config)
        assert list(selected) == ["enabled_one", "enabled_two"]

    def test_explicit_names_include_disabled(self, config):
        selected = publisher.select_datasets(config, ["disabled_one"])
        assert list(selected) == ["disabled_one"]

    def test_unknown_name_exits(self, config):
        with pytest.raises(SystemExit, match="Unknown dataset"):
            publisher.select_datasets(config, ["nope"])

    def test_no_enabled_datasets_exits(self):
        with pytest.raises(SystemExit, match="No enabled"):
            publisher.select_datasets({"datasets": {"a": {"enabled": False}}})

    def test_parse_dataset_names_flattens_and_deduplicates(self):
        assert publisher._parse_dataset_names(["a,b", "c", "a"]) == ["a", "b", "c"]

    def test_parse_dataset_names_without_values(self):
        assert publisher._parse_dataset_names(None) is None
        assert publisher._parse_dataset_names([]) is None


class TestBuild:
    def test_runs_the_configured_script_with_args(self):
        calls = []

        def fake_run(command, **kwargs):
            calls.append((command, kwargs))
            return _completed(0)

        result = publisher.build_dataset(
            "one",
            {"pre_upload": {"script": EXISTING_SCRIPT, "args": ["--full", 3]}},
            runner=fake_run,
        )

        assert result.built is True
        assert result.error is None
        command, kwargs = calls[0]
        assert command == ["uv", "run", str(REPO_ROOT / EXISTING_SCRIPT), "--full", "3"]
        assert kwargs == {"cwd": REPO_ROOT, "check": False}

    def test_missing_script_is_reported(self):
        result = publisher.build_dataset("one", {"pre_upload": {"script": "missing.py"}})
        assert result.built is False
        assert "not found" in result.error

    def test_failed_build_records_exit_code(self):
        result = publisher.build_dataset(
            "one",
            {"pre_upload": {"script": EXISTING_SCRIPT}},
            runner=lambda *_args, **_kwargs: _completed(7),
        )
        assert result.built is False
        assert "exit code 7" in result.error

    def test_dataset_without_pipeline_counts_as_built(self):
        result = publisher.build_dataset("one", {})
        assert result.built is True
        assert result.ok is True


class TestPublishAll:
    def _run(self, tmp_path, datasets, monkeypatch, **kwargs):
        config_path = _write_config(tmp_path, datasets)
        monkeypatch.setattr(publisher, "KaggleUploader", FakeUploader)
        return publisher.publish_all_datasets(config_path=config_path, **kwargs)

    def test_builds_then_uploads_each_enabled_dataset(self, tmp_path, monkeypatch):
        built: list[str] = []

        def fake_build(name, _config):
            built.append(name)
            return publisher.DatasetResult(name=name, built=True)

        monkeypatch.setattr(publisher, "build_dataset", fake_build)

        code = self._run(
            tmp_path,
            {
                "a": {"enabled": True, "pre_upload": {"script": EXISTING_SCRIPT}},
                "b": {"enabled": True, "pre_upload": {"script": EXISTING_SCRIPT}},
            },
            monkeypatch,
            confirm_yes=True,
        )

        assert code == 0
        assert built == ["a", "b"]
        assert FakeUploader.instances[0].calls == ["a", "b"]

    def test_skip_build_does_not_run_pipelines(self, tmp_path, monkeypatch):
        def fail_build(*_args, **_kwargs):
            raise AssertionError("build should not run")

        monkeypatch.setattr(publisher, "build_dataset", fail_build)

        code = self._run(
            tmp_path,
            {"a": {"enabled": True, "pre_upload": {"script": EXISTING_SCRIPT}}},
            monkeypatch,
            skip_build=True,
            confirm_yes=True,
        )

        assert code == 0
        assert FakeUploader.instances[0].calls == ["a"]

    def test_skips_upload_when_build_fails(self, tmp_path, monkeypatch):
        monkeypatch.setattr(publisher, "build_dataset", _failed_build)

        code = self._run(
            tmp_path,
            {"a": {"enabled": True, "pre_upload": {"script": EXISTING_SCRIPT}}},
            monkeypatch,
            confirm_yes=True,
        )

        assert code == 1
        assert FakeUploader.instances[0].calls == []

    def test_allow_fail_build_still_uploads(self, tmp_path, monkeypatch):
        monkeypatch.setattr(publisher, "build_dataset", _failed_build)

        code = self._run(
            tmp_path,
            {
                "a": {
                    "enabled": True,
                    "pre_upload": {"script": EXISTING_SCRIPT, "allow_fail": True},
                }
            },
            monkeypatch,
            confirm_yes=True,
        )

        assert code == 0
        assert FakeUploader.instances[0].calls == ["a"]

    def test_upload_failure_returns_non_zero(self, tmp_path, monkeypatch):
        monkeypatch.setattr(publisher, "build_dataset", _built_only)
        monkeypatch.setattr(FakeUploader, "outcome", {"a": False})

        code = self._run(
            tmp_path,
            {"a": {"enabled": True, "pre_upload": {"script": EXISTING_SCRIPT}}},
            monkeypatch,
            confirm_yes=True,
        )

        assert code == 1

    def test_skip_upload_does_not_construct_uploader(self, tmp_path, monkeypatch):
        monkeypatch.setattr(publisher, "build_dataset", _built_only)

        code = self._run(
            tmp_path,
            {"a": {"enabled": True, "pre_upload": {"script": EXISTING_SCRIPT}}},
            monkeypatch,
            skip_upload=True,
        )

        assert code == 0
        assert FakeUploader.instances == []

    def test_skip_pre_upload_is_set_during_uploads_and_restored(self, tmp_path, monkeypatch):
        monkeypatch.setattr(publisher, "build_dataset", _built_only)
        monkeypatch.delenv("SKIP_PRE_UPLOAD", raising=False)

        self._run(
            tmp_path,
            {"a": {"enabled": True, "pre_upload": {"script": EXISTING_SCRIPT}}},
            monkeypatch,
            confirm_yes=True,
        )

        assert FakeUploader.instances[0].skip_pre_upload_seen == ["1"]
        assert os.environ.get("SKIP_PRE_UPLOAD") is None

    def test_restores_previous_skip_pre_upload_value(self, tmp_path, monkeypatch):
        monkeypatch.setattr(publisher, "build_dataset", _built_only)
        monkeypatch.setenv("SKIP_PRE_UPLOAD", "0")

        self._run(
            tmp_path,
            {"a": {"enabled": True, "pre_upload": {"script": EXISTING_SCRIPT}}},
            monkeypatch,
            confirm_yes=True,
        )

        assert os.environ["SKIP_PRE_UPLOAD"] == "0"

    def test_passes_project_root_and_yes_to_uploader(self, tmp_path, monkeypatch):
        monkeypatch.setattr(publisher, "build_dataset", _built_only)

        self._run(
            tmp_path,
            {"a": {"enabled": True, "pre_upload": {"script": EXISTING_SCRIPT}}},
            monkeypatch,
            confirm_yes=True,
        )

        kwargs = FakeUploader.instances[0].kwargs
        assert kwargs["confirm_yes"] is True
        assert kwargs["project_root"] == REPO_ROOT
        assert kwargs["dry_run"] is False


class TestCli:
    def test_parses_repeated_and_comma_separated_datasets(self):
        args = publisher.parse_args(["--dataset", "a,b", "--dataset", "c", "--yes"])
        assert publisher._parse_dataset_names(args.dataset) == ["a", "b", "c"]
        assert args.yes is True

    def test_main_returns_publisher_exit_code(self, monkeypatch):
        monkeypatch.setattr(publisher, "publish_all_datasets", lambda **_kwargs: 3)
        assert publisher.main(["--yes"]) == 3
