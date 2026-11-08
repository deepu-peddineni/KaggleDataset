"""Tests for the license-clean Energy Trading pipeline."""

from __future__ import annotations

import json
import sys
from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl
import pytest
from requests import Response


MODULE_DIR = Path(__file__).resolve().parents[1] / "EnergyTrading"
sys.path.insert(0, str(MODULE_DIR))

import energy_trading_pipeline as pipeline  # noqa: E402


RETRIEVED_AT = datetime(2026, 11, 8, 12, 0, tzinfo=UTC)


def _eia_frame(rows: list[dict]) -> pl.DataFrame:
    """Build an EIA frame with the pipeline's full schema."""
    base = [
        {
            "TimestampUTC": datetime(2026, 11, 1, tzinfo=UTC),
            "Date": date(2026, 11, 1),
            "Frequency": "daily",
            "Category": "Crude Oil",
            "Instrument": "WTI Crude Oil Spot",
            "InstrumentID": "RWTC",
            "Market": "Cushing, OK",
            "Price": 70.0,
            "Unit": "USD/barrel",
            "Currency": "USD",
            "Source": "U.S. Energy Information Administration (EIA)",
            "License": pipeline.EIA_LICENSE_NAME,
            "LicenseURL": pipeline.EIA_LICENSE_URL,
            "SourceURL": pipeline.EIA_BASE_URL,
            "RetrievedAtUTC": RETRIEVED_AT,
            **row,
        }
        for row in rows
    ]
    return pipeline._finalise_frame(pl.DataFrame(base), schema=pipeline.EIA_SCHEMA)


def _power_frame(rows: list[dict]) -> pl.DataFrame:
    """Build a power frame with the pipeline's full schema."""
    base = [
        {
            "TimestampUTC": datetime(2026, 11, 1, tzinfo=UTC),
            "Date": date(2026, 11, 1),
            "Frequency": "hourly",
            "Category": "Electricity",
            "Instrument": "Day-Ahead Electricity",
            "InstrumentID": "DE-LU",
            "Market": "DE-LU",
            "Price": 100.0,
            "Unit": "EUR/MWh",
            "Currency": "EUR",
            "Source": "Energy-Charts (Fraunhofer ISE) / Bundesnetzagentur | SMARD.de",
            "License": pipeline.POWER_LICENSE_NAME,
            "LicenseURL": pipeline.POWER_LICENSE_URL,
            "SourceURL": pipeline.POWER_SOURCE_URL,
            "RetrievedAtUTC": RETRIEVED_AT,
            **row,
        }
        for row in rows
    ]
    return pipeline._finalise_frame(pl.DataFrame(base), schema=pipeline.POWER_SCHEMA)


class TestLicenseValidation:
    def test_accepts_cc_by_40(self):
        pipeline.ensure_redistributable("CC BY 4.0", "test")

    def test_accepts_cc_by_url(self):
        pipeline.ensure_redistributable("https://creativecommons.org/licenses/by/4.0/", "test")

    @pytest.mark.parametrize(
        "license_info",
        [
            "",
            "CC0-1.0",
            "CC BY-SA 4.0",
            "All rights reserved",
            "Proprietary",
        ],
    )
    def test_rejects_non_cc_by(self, license_info):
        with pytest.raises(pipeline.LicenseError):
            pipeline.ensure_redistributable(license_info, "test")


class TestRetryAfter:
    @staticmethod
    def _response(headers: dict[str, str]) -> Response:
        response = Response()
        response.headers.update(headers)
        return response

    def test_parses_seconds(self):
        response = self._response({"Retry-After": "12"})
        assert pipeline._retry_after_seconds(response) == 12.0

    def test_caps_long_waits(self):
        response = self._response({"Retry-After": "999"})
        assert pipeline._retry_after_seconds(response) == 120.0

    def test_ignores_invalid_values(self):
        assert pipeline._retry_after_seconds(self._response({"Retry-After": "soon"})) is None
        assert pipeline._retry_after_seconds(self._response({})) is None


class TestFrequencyDerivation:
    def test_labels_each_row_with_the_interval_it_covers(self):
        frame = _power_frame(
            [
                {"TimestampUTC": datetime(2025, 9, 30, 21, 0, tzinfo=UTC)},
                {"TimestampUTC": datetime(2025, 9, 30, 22, 0, tzinfo=UTC)},
                {"TimestampUTC": datetime(2025, 9, 30, 22, 15, tzinfo=UTC)},
                {"TimestampUTC": datetime(2025, 9, 30, 22, 30, tzinfo=UTC)},
            ]
        )

        assert frame["Frequency"].to_list() == ["hourly", "15min", "15min", "15min"]

    def test_ignores_irregular_gaps_when_labelling(self):
        frame = _power_frame(
            [
                {"TimestampUTC": datetime(2025, 9, 30, 0, 0, tzinfo=UTC)},
                {"TimestampUTC": datetime(2025, 9, 30, 0, 15, tzinfo=UTC)},
                {"TimestampUTC": datetime(2025, 9, 30, 2, 0, tzinfo=UTC)},
                {"TimestampUTC": datetime(2025, 9, 30, 2, 15, tzinfo=UTC)},
            ]
        )

        assert frame["Frequency"].to_list() == ["15min", "15min", "15min", "15min"]

    def test_does_not_leak_between_zones(self):
        frame = _power_frame(
            [
                {"TimestampUTC": datetime(2025, 9, 30, 0, 0, tzinfo=UTC)},
                {"TimestampUTC": datetime(2025, 9, 30, 1, 0, tzinfo=UTC)},
                {
                    "TimestampUTC": datetime(2025, 9, 30, 0, 0, tzinfo=UTC),
                    "InstrumentID": "FR",
                },
                {
                    "TimestampUTC": datetime(2025, 9, 30, 0, 15, tzinfo=UTC),
                    "InstrumentID": "FR",
                },
            ]
        )

        per_zone = dict(
            zip(
                frame["InstrumentID"].to_list(),
                frame["Frequency"].to_list(),
                strict=True,
            )
        )
        assert per_zone == {"DE-LU": "hourly", "FR": "15min"}

    def test_single_row_is_unknown(self):
        frame = _power_frame([{"TimestampUTC": datetime(2025, 9, 30, 0, 0, tzinfo=UTC)}])

        assert frame["Frequency"].to_list() == ["unknown"]

    @pytest.mark.parametrize(
        ("seconds", "expected"),
        [
            (900, "15min"),
            (1_800, "30min"),
            (3_600, "hourly"),
            (86_400, "daily"),
            (1_234, "1234s"),
            (None, "unknown"),
        ],
    )
    def test_resolution_label(self, seconds, expected):
        assert pipeline._resolution_label(seconds) == expected


class TestMergeFrames:
    def test_keeps_latest_revision_per_key(self):
        existing = _eia_frame([{"Price": 70.0}])
        newer = _eia_frame([{"Price": 71.5, "RetrievedAtUTC": datetime(2026, 11, 9, tzinfo=UTC)}])

        merged = pipeline.merge_frames(
            existing,
            newer,
            keys=["Date", "InstrumentID"],
            schema=pipeline.EIA_SCHEMA,
        )

        assert merged.height == 1
        assert merged["Price"].to_list() == [71.5]
        assert merged["RetrievedAtUTC"].to_list() == [datetime(2026, 11, 9, tzinfo=UTC)]

    def test_preserves_distinct_keys(self):
        existing = _eia_frame([{"Price": 70.0}])
        other = _eia_frame([{"InstrumentID": "RBRTE", "Price": 74.0}])

        merged = pipeline.merge_frames(
            existing,
            other,
            keys=["Date", "InstrumentID"],
            schema=pipeline.EIA_SCHEMA,
        )

        assert merged.height == 2
        assert set(merged["InstrumentID"].to_list()) == {"RWTC", "RBRTE"}

    def test_empty_new_frame_keeps_existing(self):
        existing = _eia_frame([{"Price": 70.0}])
        merged = pipeline.merge_frames(
            existing,
            pipeline._empty_frame(pipeline.EIA_SCHEMA),
            keys=["Date", "InstrumentID"],
            schema=pipeline.EIA_SCHEMA,
        )

        assert merged.height == 1

    def test_empty_everything_returns_schema(self):
        merged = pipeline.merge_frames(
            None,
            pipeline._empty_frame(pipeline.EIA_SCHEMA),
            keys=["Date", "InstrumentID"],
            schema=pipeline.EIA_SCHEMA,
        )

        assert merged.is_empty()
        assert merged.schema == pipeline.EIA_SCHEMA


class TestBuildCombined:
    def test_combines_sources_with_stable_schema(self):
        eia = _eia_frame([{"Price": 70.0}])
        power = _power_frame([{"Price": 100.0}])

        combined = pipeline.build_combined(eia, power)

        assert combined.height == 2
        assert combined.columns == pipeline.COMBINED_COLUMNS
        assert combined.schema == pipeline.COMBINED_SCHEMA
        assert set(combined["Category"].to_list()) == {"Crude Oil", "Electricity"}

    def test_empty_inputs_return_empty_schema(self):
        combined = pipeline.build_combined(None, None)

        assert combined.is_empty()
        assert combined.columns == pipeline.COMBINED_COLUMNS


class TestCalendarColumns:
    def test_derived_from_timestamp(self):
        frame = _eia_frame(
            [
                {
                    "TimestampUTC": datetime(2026, 3, 4, 5, 6, tzinfo=UTC),
                    "Date": date(2026, 3, 4),
                }
            ]
        )

        row = frame.row(0, named=True)
        assert row["Year"] == 2026
        assert row["Month"] == 3
        assert row["Day"] == 4
        assert row["Hour"] == 5
        assert row["Minute"] == 6


class TestRequestThrottle:
    def test_default_delay_respects_public_rate_limit(self):
        # /price allows 2 requests/minute, i.e. one every 30 seconds.
        assert pipeline.POWER_REQUEST_DELAY_SECONDS >= 31.0

    def _make(self, min_interval: float = 10.0):
        now = [0.0]
        sleeps: list[float] = []

        def clock() -> float:
            return now[0]

        def sleep(seconds: float) -> None:
            sleeps.append(seconds)
            now[0] += seconds

        throttle = pipeline.RequestThrottle(min_interval, clock=clock, sleep=sleep)
        return throttle, now, sleeps

    def test_spaces_consecutive_requests(self):
        throttle, now, sleeps = self._make(10.0)

        throttle.wait()
        now[0] += 2.0
        throttle.wait()

        assert sleeps == [8.0]

    def test_no_wait_when_interval_already_elapsed(self):
        throttle, now, sleeps = self._make(10.0)

        throttle.wait()
        now[0] += 30.0
        throttle.wait()

        assert sleeps == []

    def test_adopts_retry_after(self):
        throttle, _, _ = self._make(2.0)

        throttle.observe_retry_after(12.0)

        assert throttle.min_interval == 12.5

    def test_doubles_without_retry_after(self):
        throttle, _, _ = self._make(2.0)

        throttle.observe_retry_after(None)

        assert throttle.min_interval == 4.0


class TestPowerFetching:
    def test_rejects_non_cc_by_payload(self, monkeypatch):
        payload = {
            "license_info": "All rights reserved",
            "unix_seconds": [0, 3_600],
            "price": [10.0, 20.0],
            "unit": "EUR/MWh",
        }

        def fake_request(*_args, **_kwargs):
            return payload

        monkeypatch.setattr(pipeline, "request_json", fake_request)

        with pytest.raises(pipeline.LicenseError):
            pipeline.fetch_power_zone(
                "DE-LU",
                start=date(2026, 11, 1),
                end=date(2026, 11, 2),
                retrieved_at=RETRIEVED_AT,
            )

    def test_one_request_per_zone_and_checkpoint_callback(self, monkeypatch):
        calls: list[date] = []

        def fake_fetch(zone, *, start, **_kwargs):
            calls.append(start)
            return _power_frame([{"InstrumentID": zone}])

        checkpointed: list[pl.DataFrame] = []
        monkeypatch.setattr(pipeline, "fetch_power_zone", fake_fetch)

        frame = pipeline.fetch_power(
            zones=["DE-LU", "FR", "NL"],
            start=date(2026, 11, 1),
            end=date(2026, 11, 8),
            retrieved_at=RETRIEVED_AT,
            request_delay=0.0,
            zone_callback=checkpointed.append,
        )

        assert calls == [date(2026, 11, 1)] * 3
        assert len(checkpointed) == 3
        assert frame["InstrumentID"].n_unique() == 3


class TestTrackerRoundTrip:
    def test_write_and_read_tracker(self, tmp_path):
        frame = _eia_frame([{"Price": 70.0}, {"InstrumentID": "RBRTE", "Price": 74.0}])
        path = tmp_path / "eia_spot_prices.csv"

        pipeline.write_tracker(path, frame)
        loaded = pipeline.read_tracker(path, pipeline.EIA_SCHEMA)

        assert loaded is not None
        assert loaded.height == 2
        assert loaded.schema == pipeline.EIA_SCHEMA
        assert loaded.sort("InstrumentID")["Price"].to_list() == [74.0, 70.0]

    def test_missing_tracker_returns_none(self, tmp_path):
        assert pipeline.read_tracker(tmp_path / "missing.csv", pipeline.EIA_SCHEMA) is None


class TestResolveStart:
    def test_uses_history_when_no_tracker(self, tmp_path):
        start = pipeline._resolve_start(
            tracker_path=tmp_path / "missing.csv",
            schema=pipeline.EIA_SCHEMA,
            history_start=date(1990, 1, 1),
            requested_start=None,
            full=False,
            overlap_days=14,
        )

        assert start == date(1990, 1, 1)

    def test_incremental_overlap(self, tmp_path):
        frame = _eia_frame([{"Date": date(2026, 11, 1)}])
        path = tmp_path / "eia_spot_prices.csv"
        pipeline.write_tracker(path, frame)

        start = pipeline._resolve_start(
            tracker_path=path,
            schema=pipeline.EIA_SCHEMA,
            history_start=date(1990, 1, 1),
            requested_start=None,
            full=False,
            overlap_days=14,
        )

        assert start == date(2026, 10, 18)

    def test_requested_start_wins(self, tmp_path):
        start = pipeline._resolve_start(
            tracker_path=tmp_path / "missing.csv",
            schema=pipeline.EIA_SCHEMA,
            history_start=date(1990, 1, 1),
            requested_start=date(2026, 1, 1),
            full=True,
            overlap_days=14,
        )

        assert start == date(2026, 1, 1)


class TestSourceManifest:
    def test_manifest_records_licenses_and_exclusions(self, tmp_path):
        pipeline.write_source_manifest(
            tmp_path,
            retrieved_at=RETRIEVED_AT,
            counts={"combined": 10, "eia_spot_prices": 6, "power_day_ahead_prices": 4},
            zones=pipeline.DEFAULT_POWER_ZONES,
        )

        manifest_path = tmp_path / "metadata" / "sources.json"
        assert manifest_path.exists()
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

        assert manifest["dataset"] == "energy_trading_prices"
        assert manifest["record_counts"]["combined"] == 10
        sources = {source["name"]: source for source in manifest["sources"]}
        assert any("Energy Information Administration" in name for name in sources)
        assert any("SMARD" in name for name in sources)
        assert all(source["redistribution_allowed"] for source in sources.values())
        assert {item["name"] for item in manifest["excluded_sources"]} >= {
            "FRED (Federal Reserve Bank of St. Louis)",
            "AEMO (Australian Energy Market Operator)",
        }

    def test_default_zones_match_audit(self):
        assert pipeline.DEFAULT_POWER_ZONES == [
            "DE-LU",
            "FR",
            "NL",
            "BE",
            "AT",
            "CH",
            "CZ",
            "DK1",
            "DK2",
            "HU",
            "IT-North",
            "NO2",
            "PL",
            "SE4",
            "SI",
        ]
        assert "DE-AT-LU" not in pipeline.DEFAULT_POWER_ZONES
