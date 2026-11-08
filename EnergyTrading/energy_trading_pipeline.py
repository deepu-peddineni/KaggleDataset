# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "polars>=1.35.2",
#     "python-dotenv>=1.2.2",
#     "requests>=2.32.5",
# ]
# ///
"""
Energy Trading Prices - download, normalise, and export.

This pipeline builds a license-clean energy trading dataset from two sources:

1. U.S. Energy Information Administration (EIA) Open Data API
   - Public domain (U.S. Government work).
   - Daily spot prices for crude oil, natural gas, and refined products.

2. Energy-Charts API (Fraunhofer ISE), backed by Bundesnetzagentur | SMARD.de
   - CC BY 4.0 for the bidding zones explicitly listed in the Energy-Charts
     OpenAPI specification.
   - Day-ahead electricity prices for selected European bidding zones.

The pipeline intentionally does NOT ingest sources whose licenses do not allow
redistribution (for example FRED, ENTSO-E day-ahead prices, AEMO, or exchange
data from EPEX/EEX/ICE/CME). See DATA_LICENSES.md for the full audit.

Usage:
    python EnergyTrading/energy_trading_pipeline.py
    python EnergyTrading/energy_trading_pipeline.py --sources power --start 2026-10-01
    python EnergyTrading/energy_trading_pipeline.py --full
"""

from __future__ import annotations

import argparse
import json
import os
import random
import time
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl
import requests
from dotenv import load_dotenv


MODULE_DIR = Path(__file__).resolve().parent
REPO_ROOT = MODULE_DIR.parent

EIA_BASE_URL = "https://api.eia.gov/v2"
EIA_LICENSE_NAME = "Public Domain (U.S. Government)"
EIA_LICENSE_URL = "https://www.eia.gov/about/copyrights_reuse.php"
EIA_SOURCE_URL = "https://www.eia.gov/opendata/"
EIA_ATTRIBUTION = "Source: U.S. Energy Information Administration (EIA)."

POWER_API_URL = "https://api.energy-charts.info/price"
POWER_OPENAPI_URL = "https://api.energy-charts.info/openapi.json"
POWER_LICENSE_NAME = "CC BY 4.0"
POWER_LICENSE_URL = "https://creativecommons.org/licenses/by/4.0/"
POWER_SOURCE_URL = "https://api.energy-charts.info/"
POWER_ATTRIBUTION = (
    "Day-ahead price data: Bundesnetzagentur | SMARD.de, retrieved via the "
    "Energy-Charts API (Fraunhofer ISE), licensed under CC BY 4.0."
)

EIA_HISTORY_START = date(1990, 1, 1)
POWER_HISTORY_START = date(2019, 1, 1)
INCREMENTAL_OVERLAP_DAYS = 14
EIA_PAGE_SIZE = 5_000

# The public Energy-Charts /price endpoint allows 2 requests per minute with a
# burst of 2 (see https://api.energy-charts.info/openapi.json). 32 seconds leaves
# a small margin over the 30-second refill interval so requests never trip the
# token bucket through timing jitter.
POWER_REQUEST_DELAY_SECONDS = 32.0
POWER_RATE_LIMIT_WARNING_THRESHOLD = 30.0

# EIA series IDs. Route defaults to the petroleum spot-price endpoint.
EIA_SERIES: dict[str, dict[str, str]] = {
    "RWTC": {
        "category": "Crude Oil",
        "instrument": "WTI Crude Oil Spot",
        "market": "Cushing, OK",
        "unit": "USD/barrel",
    },
    "RBRTE": {
        "category": "Crude Oil",
        "instrument": "Brent Crude Oil Spot",
        "market": "Europe",
        "unit": "USD/barrel",
    },
    "RNGWHHD": {
        "category": "Natural Gas",
        "instrument": "Henry Hub Natural Gas Spot",
        "market": "Henry Hub, LA",
        "unit": "USD/MMBtu",
        "route": "natural-gas/pri/fut",
    },
    "EER_EPMRU_PF4_RGC_DPG": {
        "category": "Refined Products",
        "instrument": "Conventional Regular Gasoline Spot",
        "market": "U.S. Gulf Coast",
        "unit": "USD/gallon",
    },
    "EER_EPMRU_PF4_Y35NY_DPG": {
        "category": "Refined Products",
        "instrument": "Conventional Regular Gasoline Spot",
        "market": "New York Harbor",
        "unit": "USD/gallon",
    },
    "EER_EPMRR_PF4_Y05LA_DPG": {
        "category": "Refined Products",
        "instrument": "Reformulated RBOB Regular Gasoline Spot",
        "market": "Los Angeles",
        "unit": "USD/gallon",
    },
    "EER_EPD2DXL0_PF4_RGC_DPG": {
        "category": "Refined Products",
        "instrument": "Ultra-Low Sulfur No. 2 Diesel Spot",
        "market": "U.S. Gulf Coast",
        "unit": "USD/gallon",
    },
    "EER_EPD2DC_PF4_Y05LA_DPG": {
        "category": "Refined Products",
        "instrument": "CARB Diesel Spot",
        "market": "Los Angeles",
        "unit": "USD/gallon",
    },
    "EER_EPD2F_PF4_Y35NY_DPG": {
        "category": "Refined Products",
        "instrument": "No. 2 Heating Oil Spot",
        "market": "New York Harbor",
        "unit": "USD/gallon",
    },
    "EER_EPJK_PF4_RGC_DPG": {
        "category": "Refined Products",
        "instrument": "Kerosene-Type Jet Fuel Spot",
        "market": "U.S. Gulf Coast",
        "unit": "USD/gallon",
    },
    "EER_EPLLPA_PF4_Y44MB_DPG": {
        "category": "Refined Products",
        "instrument": "Propane Spot",
        "market": "Mont Belvieu, TX",
        "unit": "USD/gallon",
    },
}

# Energy-Charts zones that its OpenAPI specification explicitly labels CC BY 4.0.
# DE-AT-LU is deliberately omitted because it overlaps DE-LU and AT.
DEFAULT_POWER_ZONES = [
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

EIA_SCHEMA: dict[str, pl.DataType] = {
    "TimestampUTC": pl.Datetime(time_unit="ms", time_zone="UTC"),
    "Date": pl.Date,
    "Frequency": pl.Utf8,
    "Category": pl.Utf8,
    "Instrument": pl.Utf8,
    "InstrumentID": pl.Utf8,
    "Market": pl.Utf8,
    "Price": pl.Float64,
    "Unit": pl.Utf8,
    "Currency": pl.Utf8,
    "Source": pl.Utf8,
    "License": pl.Utf8,
    "LicenseURL": pl.Utf8,
    "SourceURL": pl.Utf8,
    "RetrievedAtUTC": pl.Datetime(time_unit="ms", time_zone="UTC"),
    "Year": pl.Int32,
    "Month": pl.Int8,
    "Day": pl.Int8,
    "Hour": pl.Int8,
    "Minute": pl.Int8,
}

POWER_SCHEMA: dict[str, pl.DataType] = {
    **EIA_SCHEMA,
}

COMBINED_COLUMNS = [
    "TimestampUTC",
    "Date",
    "Frequency",
    "Category",
    "Instrument",
    "InstrumentID",
    "Market",
    "Price",
    "Unit",
    "Currency",
    "Source",
    "License",
    "LicenseURL",
    "SourceURL",
    "RetrievedAtUTC",
    "Year",
    "Month",
    "Day",
    "Hour",
    "Minute",
]

COMBINED_SCHEMA: dict[str, pl.DataType] = {name: EIA_SCHEMA[name] for name in COMBINED_COLUMNS}


class DownloadError(RuntimeError):
    """Raised when a remote source cannot be downloaded after retries."""


class LicenseError(RuntimeError):
    """Raised when a source does not grant redistribution rights."""


class RequestThrottle:
    """Space out requests and adapt to server rate limits.

    The public Energy-Charts ``/price`` endpoint allows 2 requests per minute
    with a burst of 2, and answers bursts with HTTP 429 plus a ``Retry-After``
    header. The pipeline therefore spaces requests by ``POWER_REQUEST_DELAY_SECONDS``
    and permanently slows down further whenever a 429 still occurs, rather than
    generating noisy retry storms.
    """

    def __init__(
        self,
        min_interval: float,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.min_interval = max(min_interval, 0.0)
        self._clock = clock
        self._sleep = sleep
        self._last_request: float | None = None

    def wait(self) -> None:
        """Block until the next request is allowed, then mark it as sent."""
        if self._last_request is not None:
            remaining = self.min_interval - (self._clock() - self._last_request)
            if remaining > 0:
                self._sleep(remaining)
        self._last_request = self._clock()

    def observe_retry_after(self, seconds: float | None) -> None:
        """Slow down permanently after a 429 (or a server error)."""
        if seconds is None:
            self.min_interval = min(max(self.min_interval * 2, 1.0), 120.0)
        else:
            self.min_interval = min(max(self.min_interval, seconds + 0.5), 120.0)


def _utc_now() -> datetime:
    """Return the current time as a timezone-aware UTC datetime."""
    return datetime.now(UTC)


def _iso_utc(value: datetime) -> str:
    """Format a datetime as an ISO-8601 UTC string with a trailing Z."""
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _to_float(value: Any) -> float | None:
    """Convert an API value to float, returning None for blank/missing values."""
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        if stripped in {"", ".", "NA", "N/A", "null"}:
            return None
        value = stripped
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalise_unit(unit: str | None) -> str:
    """Normalise unit strings so they are stable across exports."""
    if not unit:
        return "unknown"
    return unit.replace(" / ", "/").strip()


def _normalise_currency(unit: str) -> str:
    """Infer the currency from a unit string."""
    return "EUR" if "EUR" in unit.upper() else "USD"


def _retry_after_seconds(response: requests.Response) -> float | None:
    """Parse a Retry-After header, capped so a bad value cannot stall the run."""
    value = response.headers.get("Retry-After")
    if not value:
        return None
    try:
        seconds = float(value)
    except ValueError:
        return None
    return min(max(seconds, 0.0), 120.0)


def request_json(
    url: str,
    params: dict[str, Any] | None = None,
    *,
    allow_404: bool = False,
    retries: int = 8,
    timeout: int = 60,
    throttle: RequestThrottle | None = None,
) -> dict[str, Any] | None:
    """GET a JSON endpoint with throttling and backoff for 429/5xx responses.

    When the server sends a Retry-After header, its value is preferred over the
    exponential backoff. This matters for the Energy-Charts API, which applies a
    per-window rate limit and otherwise returns repeated 429 responses.
    """
    delay = 1.5
    last_error: Exception | None = None

    for attempt in range(1, retries + 1):
        retry_after: float | None = None
        if throttle is not None:
            throttle.wait()
        try:
            response = requests.get(url, params=params, timeout=timeout)
            if response.status_code == 404 and allow_404:
                return None
            if response.status_code == 429 or response.status_code >= 500:
                retry_after = _retry_after_seconds(response)
                if throttle is not None:
                    throttle.observe_retry_after(retry_after)
                raise requests.HTTPError(
                    f"retryable HTTP {response.status_code}", response=response
                )
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            if attempt >= retries:
                break
            if retry_after is not None:
                wait = retry_after + random.uniform(0, 0.75)
            else:
                wait = delay * (2 ** (attempt - 1)) + random.uniform(0, 0.75)
            print(f"  ! request failed ({exc}); retrying in {wait:.1f}s")
            time.sleep(wait)

    raise DownloadError(f"GET {url} failed after {retries} attempts: {last_error}")


def ensure_redistributable(license_info: str, source_name: str) -> None:
    """Abort if a source does not explicitly grant CC BY 4.0 redistribution."""
    normalised = license_info.lower().replace("-", " ")
    has_cc_by = "cc by 4.0" in normalised
    has_cc_url = "creativecommons.org/licenses/by/4.0" in normalised
    if not (has_cc_by or has_cc_url):
        raise LicenseError(
            f"{source_name} reported license {license_info!r}, which is not "
            "CC BY 4.0. Refusing to publish this data."
        )


def _empty_frame(schema: dict[str, pl.DataType]) -> pl.DataFrame:
    """Create an empty DataFrame with an explicit schema."""
    return pl.DataFrame(schema=schema)


def _finalise_frame(
    frame: pl.DataFrame,
    *,
    schema: dict[str, pl.DataType],
) -> pl.DataFrame:
    """Cast a frame to a stable schema and derive calendar columns."""
    if frame.is_empty():
        return _empty_frame(schema)

    frame = frame.with_columns(
        [
            pl.col("TimestampUTC").cast(pl.Datetime(time_unit="ms", time_zone="UTC")),
            pl.col("Date").cast(pl.Date),
            pl.col("Price").cast(pl.Float64),
            pl.col("RetrievedAtUTC").cast(pl.Datetime(time_unit="ms", time_zone="UTC")),
        ]
    )
    frame = frame.with_columns(
        [
            pl.col("TimestampUTC").dt.year().cast(pl.Int32).alias("Year"),
            pl.col("TimestampUTC").dt.month().cast(pl.Int8).alias("Month"),
            pl.col("TimestampUTC").dt.day().cast(pl.Int8).alias("Day"),
            pl.col("TimestampUTC").dt.hour().cast(pl.Int8).alias("Hour"),
            pl.col("TimestampUTC").dt.minute().cast(pl.Int8).alias("Minute"),
        ]
    )
    for name, dtype in schema.items():
        if name not in frame.columns:
            frame = frame.with_columns(pl.lit(None, dtype=dtype).alias(name))
    if schema is POWER_SCHEMA:
        frame = _derive_power_frequency(frame)
    return frame.select(list(schema))


def fetch_eia_series(
    series_id: str,
    *,
    api_key: str,
    start: date,
    end: date,
    retrieved_at: datetime,
) -> pl.DataFrame:
    """Download one EIA series and return a normalised DataFrame."""
    meta = EIA_SERIES[series_id]
    route = meta.get("route", "petroleum/pri/spt")
    url = f"{EIA_BASE_URL}/{route}/data/"
    records: list[dict[str, Any]] = []
    offset = 0

    while True:
        payload = request_json(
            url,
            {
                "api_key": api_key,
                "data[]": "value",
                "frequency": "daily",
                "facets[series][]": series_id,
                "start": start.isoformat(),
                "end": end.isoformat(),
                "sort[0][column]": "period",
                "sort[0][direction]": "asc",
                "offset": offset,
                "length": EIA_PAGE_SIZE,
            },
        )
        if payload is None:
            break

        batch = payload.get("response", {}).get("data", [])
        for item in batch:
            price = _to_float(item.get("value"))
            period = item.get("period")
            if price is None or not period:
                continue
            try:
                row_date = date.fromisoformat(str(period)[:10])
            except ValueError:
                continue
            records.append(
                {
                    "TimestampUTC": datetime(
                        row_date.year, row_date.month, row_date.day, tzinfo=UTC
                    ),
                    "Date": row_date,
                    "Frequency": "daily",
                    "Category": meta["category"],
                    "Instrument": meta["instrument"],
                    "InstrumentID": series_id,
                    "Market": meta["market"],
                    "Price": price,
                    "Unit": meta["unit"],
                    "Currency": "USD",
                    "Source": "U.S. Energy Information Administration (EIA)",
                    "License": EIA_LICENSE_NAME,
                    "LicenseURL": EIA_LICENSE_URL,
                    "SourceURL": f"{EIA_BASE_URL}/{route}",
                    "RetrievedAtUTC": retrieved_at,
                }
            )

        if len(batch) < EIA_PAGE_SIZE:
            break
        offset += EIA_PAGE_SIZE

    if not records:
        print(f"  - EIA {series_id}: no rows in {start}..{end}")
        return _empty_frame(EIA_SCHEMA)

    frame = pl.DataFrame(records)
    return _finalise_frame(frame, schema=EIA_SCHEMA)


def fetch_eia(
    *,
    api_key: str,
    start: date,
    end: date,
    retrieved_at: datetime,
) -> pl.DataFrame:
    """Download every configured EIA series."""
    frames: list[pl.DataFrame] = []
    for series_id in EIA_SERIES:
        print(f"  · EIA {series_id}")
        frames.append(
            fetch_eia_series(
                series_id,
                api_key=api_key,
                start=start,
                end=end,
                retrieved_at=retrieved_at,
            )
        )
        time.sleep(0.25)
    non_empty = [frame for frame in frames if not frame.is_empty()]
    if not non_empty:
        return _empty_frame(EIA_SCHEMA)
    return pl.concat(non_empty, how="diagonal_relaxed")


_RESOLUTION_LABELS: dict[int, str] = {
    900: "15min",
    1_800: "30min",
    3_600: "hourly",
    86_400: "daily",
}


def _resolution_label(seconds: int | None) -> str:
    """Map a sampling interval in seconds to a human-readable label."""
    if seconds is None:
        return "unknown"
    return _RESOLUTION_LABELS.get(int(seconds), f"{int(seconds)}s")


def _derive_power_frequency(frame: pl.DataFrame) -> pl.DataFrame:
    """Label every power row with the interval it actually covers.

    A zone can change resolution over its history: European day-ahead prices
    moved from hourly to 15-minute settlement during 2025. Labelling the whole
    zone with one modal interval would therefore be wrong for roughly 40% of
    rows. Instead, each row is labelled from the gap to the next observation in
    the same zone. Irregular gaps caused by missing data are skipped in favour
    of the surrounding regular interval.
    """
    if frame.is_empty():
        return frame

    ordered = frame.sort(["InstrumentID", "TimestampUTC"])
    delta = (
        pl.col("TimestampUTC").shift(-1).over("InstrumentID") - pl.col("TimestampUTC")
    ).dt.total_seconds()
    regular = (
        pl.when(delta.is_in(list(_RESOLUTION_LABELS)))
        .then(delta)
        .otherwise(None)
        .backward_fill()
        .forward_fill()
        .over("InstrumentID")
    )
    frequency = (
        pl.when(regular.is_null())
        .then(pl.lit("unknown"))
        .when(regular == 900)
        .then(pl.lit("15min"))
        .when(regular == 1_800)
        .then(pl.lit("30min"))
        .when(regular == 3_600)
        .then(pl.lit("hourly"))
        .when(regular == 86_400)
        .then(pl.lit("daily"))
        .otherwise(pl.format("{}s", regular.cast(pl.Int64)))
    )
    return ordered.with_columns(frequency.alias("Frequency"))


def fetch_power_zone(
    zone: str,
    *,
    start: date,
    end: date,
    retrieved_at: datetime,
    throttle: RequestThrottle | None = None,
) -> pl.DataFrame:
    """Download one bidding zone's day-ahead prices for a date range."""
    payload = request_json(
        POWER_API_URL,
        {"bzn": zone, "start": start.isoformat(), "end": end.isoformat()},
        allow_404=True,
        timeout=180,
        throttle=throttle,
    )
    if payload is None:
        print(f"  - {zone}: no data in {start}..{end}")
        return _empty_frame(POWER_SCHEMA)

    license_info = str(payload.get("license_info", ""))
    ensure_redistributable(license_info, f"Energy-Charts zone {zone}")

    unix_seconds = payload.get("unix_seconds") or []
    prices = payload.get("price") or []
    if not unix_seconds or not prices:
        print(f"  - {zone}: empty payload in {start}..{end}")
        return _empty_frame(POWER_SCHEMA)

    unit = _normalise_unit(payload.get("unit", "EUR/MWh"))

    frame = pl.DataFrame(
        {
            "unix_seconds": pl.Series(unix_seconds, dtype=pl.Int64),
            "Price": pl.Series(prices, dtype=pl.Float64, strict=False),
        }
    )
    frame = frame.with_columns(
        (pl.col("unix_seconds") * 1_000)
        .cast(pl.Datetime(time_unit="ms", time_zone="UTC"))
        .alias("TimestampUTC")
    ).drop_nulls("Price")

    if frame.is_empty():
        return _empty_frame(POWER_SCHEMA)

    frame = frame.with_columns(
        [
            pl.col("TimestampUTC").dt.date().alias("Date"),
            pl.lit("Electricity").alias("Category"),
            pl.lit("Day-Ahead Electricity").alias("Instrument"),
            pl.lit(zone).alias("InstrumentID"),
            pl.lit(zone).alias("Market"),
            pl.lit(unit).alias("Unit"),
            pl.lit(_normalise_currency(unit)).alias("Currency"),
            pl.lit("Energy-Charts (Fraunhofer ISE) / Bundesnetzagentur | SMARD.de").alias("Source"),
            pl.lit(POWER_LICENSE_NAME).alias("License"),
            pl.lit(POWER_LICENSE_URL).alias("LicenseURL"),
            pl.lit(POWER_SOURCE_URL).alias("SourceURL"),
            pl.lit(retrieved_at).alias("RetrievedAtUTC"),
        ]
    )
    return _finalise_frame(frame, schema=POWER_SCHEMA)


def fetch_power(
    *,
    zones: list[str],
    start: date,
    end: date,
    retrieved_at: datetime,
    request_delay: float,
    zone_callback: Callable[[pl.DataFrame], None] | None = None,
) -> pl.DataFrame:
    """Download day-ahead prices for all requested bidding zones.

    Each zone is requested in a single call. The API accepts multi-year ranges,
    so one request per zone is both faster and far less likely to trip the rate
    limiter than splitting the range by calendar year.

    ``zone_callback`` is invoked after every zone so the caller can checkpoint
    progress; a failure part-way through does not discard completed zones.
    """
    throttle = RequestThrottle(request_delay)
    frames: list[pl.DataFrame] = []
    for zone in zones:
        print(f"  · Energy-Charts {zone} {start}..{end}")
        frame = fetch_power_zone(
            zone,
            start=start,
            end=end,
            retrieved_at=retrieved_at,
            throttle=throttle,
        )
        frames.append(frame)
        if zone_callback is not None:
            zone_callback(frame)
    non_empty = [frame for frame in frames if not frame.is_empty()]
    if not non_empty:
        return _empty_frame(POWER_SCHEMA)
    return pl.concat(non_empty, how="diagonal_relaxed")


def _csv_ready(frame: pl.DataFrame) -> pl.DataFrame:
    """Convert typed columns to stable strings for CSV output."""
    return frame.with_columns(
        [
            pl.col("TimestampUTC").dt.strftime("%Y-%m-%dT%H:%M:%SZ").alias("TimestampUTC"),
            pl.col("RetrievedAtUTC").dt.strftime("%Y-%m-%dT%H:%M:%SZ").alias("RetrievedAtUTC"),
            pl.col("Date").cast(pl.Utf8).alias("Date"),
        ]
    )


def read_tracker(path: Path, schema: dict[str, pl.DataType]) -> pl.DataFrame | None:
    """Read an existing tracker CSV, returning None when absent or unreadable."""
    if not path.exists() or path.stat().st_size == 0:
        return None
    try:
        frame = pl.read_csv(path)
    except Exception as exc:  # pragma: no cover - defensive
        print(f"  ! could not read {path}: {exc}; rebuilding")
        return None

    if frame.is_empty():
        return None

    try:
        frame = frame.with_columns(
            [
                pl.col("TimestampUTC")
                .cast(pl.Utf8)
                .str.to_datetime(format="%Y-%m-%dT%H:%M:%SZ", time_zone="UTC", strict=False)
                .alias("TimestampUTC"),
                pl.col("RetrievedAtUTC")
                .cast(pl.Utf8)
                .str.to_datetime(format="%Y-%m-%dT%H:%M:%SZ", time_zone="UTC", strict=False)
                .alias("RetrievedAtUTC"),
                pl.col("Date")
                .cast(pl.Utf8)
                .str.to_date(format="%Y-%m-%d", strict=False)
                .alias("Date"),
            ]
        )
    except Exception as exc:  # pragma: no cover - defensive
        print(f"  ! could not parse {path}: {exc}; rebuilding")
        return None

    return _finalise_frame(frame, schema=schema)


def write_tracker(path: Path, frame: pl.DataFrame) -> None:
    """Persist the full tracker file used for incremental updates."""
    path.parent.mkdir(parents=True, exist_ok=True)
    _csv_ready(frame).write_csv(path)
    print(f"  ✓ tracker saved: {path} ({frame.height:,} rows)")


def merge_frames(
    existing: pl.DataFrame | None,
    new: pl.DataFrame,
    *,
    keys: list[str],
    schema: dict[str, pl.DataType],
) -> pl.DataFrame:
    """Merge new rows into existing rows, keeping the latest revision per key."""
    if existing is None or existing.is_empty():
        merged = new
    elif new.is_empty():
        merged = existing
    else:
        merged = pl.concat([existing, new], how="diagonal_relaxed")

    if merged.is_empty():
        return _empty_frame(schema)

    merged = merged.sort([*keys, "RetrievedAtUTC"], nulls_last=True)
    merged = merged.unique(subset=keys, keep="last", maintain_order=True)
    return _finalise_frame(merged.sort(keys), schema=schema)


def export_frame(frame: pl.DataFrame, stem: str, output_dir: Path) -> None:
    """Export a frame to CSV, JSON, and Parquet."""
    csv_dir = output_dir / "csv"
    json_dir = output_dir / "json"
    parquet_dir = output_dir / "parquet"
    for directory in (csv_dir, json_dir, parquet_dir):
        directory.mkdir(parents=True, exist_ok=True)

    csv_file = csv_dir / f"{stem}.csv"
    json_file = json_dir / f"{stem}.json"
    parquet_file = parquet_dir / f"{stem}.parquet"

    _csv_ready(frame).write_csv(csv_file)
    frame.write_json(json_file)
    with json_file.open("a", encoding="utf-8") as handle:
        handle.write("\n")
    frame.write_parquet(parquet_file, compression="zstd")

    print(
        f"  ✓ {stem}: {frame.height:,} rows "
        f"(csv {csv_file.stat().st_size / 1024:,.1f} KB, "
        f"parquet {parquet_file.stat().st_size / 1024:,.1f} KB)"
    )


def build_combined(
    eia_frame: pl.DataFrame | None,
    power_frame: pl.DataFrame | None,
) -> pl.DataFrame:
    """Combine all sources into one tidy long-format table."""
    frames = [
        frame for frame in (eia_frame, power_frame) if frame is not None and not frame.is_empty()
    ]
    if not frames:
        return _empty_frame(COMBINED_SCHEMA)
    combined = pl.concat(frames, how="diagonal_relaxed")
    return _finalise_frame(combined, schema=COMBINED_SCHEMA).sort(
        ["TimestampUTC", "Category", "InstrumentID"]
    )


def write_source_manifest(
    output_dir: Path,
    *,
    retrieved_at: datetime,
    counts: dict[str, int],
    zones: list[str],
) -> None:
    """Write a machine-readable license and provenance manifest."""
    metadata_dir = output_dir / "metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)

    manifest = {
        "generated_at_utc": _iso_utc(retrieved_at),
        "dataset": "energy_trading_prices",
        "record_counts": counts,
        "sources": [
            {
                "name": "U.S. Energy Information Administration (EIA)",
                "data": "Daily spot prices for crude oil, natural gas, and refined products",
                "api": EIA_SOURCE_URL,
                "license": EIA_LICENSE_NAME,
                "license_url": EIA_LICENSE_URL,
                "redistribution_allowed": True,
                "attribution": EIA_ATTRIBUTION,
                "changes": (
                    "Series renamed to descriptive instruments, dates normalised to "
                    "ISO-8601, and data reshaped to a tidy long table. Price values "
                    "are unchanged."
                ),
                "series": sorted(EIA_SERIES),
            },
            {
                "name": "Energy-Charts (Fraunhofer ISE) / Bundesnetzagentur | SMARD.de",
                "data": "European day-ahead electricity prices",
                "api": POWER_SOURCE_URL,
                "openapi": POWER_OPENAPI_URL,
                "license": POWER_LICENSE_NAME,
                "license_url": POWER_LICENSE_URL,
                "redistribution_allowed": True,
                "attribution": POWER_ATTRIBUTION,
                "changes": (
                    "Timestamps converted to UTC, prices reshaped to a tidy long "
                    "table, and the original resolution recorded. Price values are "
                    "unchanged."
                ),
                "bidding_zones": zones,
            },
        ],
        "excluded_sources": [
            {
                "name": "FRED (Federal Reserve Bank of St. Louis)",
                "reason": (
                    "FRED terms prohibit archiving, redistribution, and commercial "
                    "reuse of its content. Use the underlying EIA series instead."
                ),
                "url": "https://fred.stlouisfed.org/legal/",
            },
            {
                "name": "ENTSO-E Transparency Platform day-ahead prices",
                "reason": (
                    "Only the data items on ENTSO-E's published free-reuse list are "
                    "CC BY 4.0. Day-ahead prices (Article 12.1.D) are not on that list."
                ),
                "url": "https://transparencyplatform.zendesk.com/hc/en-us/articles/40921911218961",
            },
            {
                "name": "AEMO (Australian Energy Market Operator)",
                "reason": "Copyrighted material; redistribution requires permission.",
                "url": "https://www.aemo.com.au/privacy-and-legal-notices/copyright-permissions",
            },
            {
                "name": "EPEX SPOT / EEX / Nord Pool / ICE / CME",
                "reason": (
                    "Exchange and futures data are commercial products. A paid "
                    "data licence is required before redistribution."
                ),
                "url": "https://www.eex.com/en/",
            },
        ],
    }

    manifest_file = metadata_dir / "sources.json"
    manifest_file.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(f"  ✓ license manifest saved: {manifest_file}")


def _resolve_start(
    *,
    tracker_path: Path,
    schema: dict[str, pl.DataType],
    history_start: date,
    requested_start: date | None,
    full: bool,
    overlap_days: int,
) -> date:
    """Choose the download window based on tracker state and CLI flags."""
    if requested_start is not None:
        return requested_start
    if full:
        return history_start

    existing = read_tracker(tracker_path, schema)
    if existing is None or existing.is_empty():
        return history_start

    last_date = existing["Date"].max()
    if not isinstance(last_date, date):
        return history_start
    return max(history_start, last_date - timedelta(days=overlap_days))


def _get_eia_api_key() -> str | None:
    """Read the EIA API key from the environment."""
    return os.getenv("EIA_API_KEY") or os.getenv("API_KEY")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Build the license-clean Energy Trading Prices dataset.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python EnergyTrading/energy_trading_pipeline.py
  python EnergyTrading/energy_trading_pipeline.py --sources power --start 2026-10-01
  python EnergyTrading/energy_trading_pipeline.py --full
        """,
    )
    parser.add_argument(
        "--sources",
        default="eia,power",
        help="Comma-separated sources to run: eia, power (default: eia,power)",
    )
    parser.add_argument(
        "--start",
        type=date.fromisoformat,
        default=None,
        help="Override the start date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--end",
        type=date.fromisoformat,
        default=None,
        help="Override the end date (YYYY-MM-DD). Defaults to today (UTC).",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Rebuild from the full default history instead of incrementally.",
    )
    parser.add_argument(
        "--zones",
        default=",".join(DEFAULT_POWER_ZONES),
        help="Comma-separated Energy-Charts bidding zones (default: CC BY 4.0 set).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=MODULE_DIR,
        help="Directory for exports and tracker files (default: EnergyTrading/).",
    )
    parser.add_argument(
        "--request-delay",
        type=float,
        default=POWER_REQUEST_DELAY_SECONDS,
        help=(
            "Minimum seconds between Energy-Charts requests "
            f"(default: {POWER_REQUEST_DELAY_SECONDS:.0f}). "
            "The public /price endpoint allows 2 requests/minute."
        ),
    )
    parser.add_argument(
        "--allow-partial",
        action="store_true",
        help="Continue when a requested source is unavailable (e.g. missing EIA key).",
    )
    return parser.parse_args(argv)


def _warn_if_delay_is_too_fast(request_delay: float) -> None:
    """Warn when a custom delay exceeds the public /price rate limit."""
    if request_delay < POWER_RATE_LIMIT_WARNING_THRESHOLD:
        print(
            "! Warning: the public Energy-Charts /price endpoint allows "
            "2 requests/minute; delays below 30s are likely to return HTTP 429."
        )
        print()


def main(argv: list[str] | None = None) -> int:
    """Run the pipeline."""
    args = parse_args(argv)
    load_dotenv(REPO_ROOT / ".env")
    load_dotenv(MODULE_DIR / ".env")

    sources = [source.strip().lower() for source in args.sources.split(",") if source.strip()]
    valid_sources = {"eia", "power"}
    unknown = set(sources) - valid_sources
    if unknown:
        raise SystemExit(f"Unknown source(s): {', '.join(sorted(unknown))}")

    output_dir: Path = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    end = args.end or _utc_now().date()
    if end > _utc_now().date():
        end = _utc_now().date()

    retrieved_at = _utc_now()
    zones = [zone.strip() for zone in args.zones.split(",") if zone.strip()]

    print("=" * 78)
    print("Energy Trading Prices - pipeline")
    print("=" * 78)
    print(f"Timestamp (UTC): {_iso_utc(retrieved_at)}")
    print(f"Sources:         {', '.join(sources)}")
    print(f"Output:          {output_dir}")
    print()

    eia_tracker = output_dir / "eia_spot_prices.csv"
    power_tracker = output_dir / "power_day_ahead_prices.csv"

    eia_frame: pl.DataFrame | None = None
    power_frame: pl.DataFrame | None = None

    if "eia" in sources:
        api_key = _get_eia_api_key()
        if not api_key:
            message = (
                "EIA API key not found. Set EIA_API_KEY (or API_KEY) in the "
                "environment or .env file. Register free at "
                "https://www.eia.gov/opendata/register.php"
            )
            if args.allow_partial:
                print(f"! {message}")
                print("! Skipping EIA source (--allow-partial).")
            else:
                raise SystemExit(message)
        else:
            start = _resolve_start(
                tracker_path=eia_tracker,
                schema=EIA_SCHEMA,
                history_start=EIA_HISTORY_START,
                requested_start=args.start,
                full=args.full,
                overlap_days=INCREMENTAL_OVERLAP_DAYS,
            )
            print(f"EIA: downloading {start} .. {end}")
            new_eia = fetch_eia(
                api_key=api_key,
                start=start,
                end=end,
                retrieved_at=retrieved_at,
            )
            existing_eia = read_tracker(eia_tracker, EIA_SCHEMA)
            eia_frame = merge_frames(
                existing_eia,
                new_eia,
                keys=["Date", "InstrumentID"],
                schema=EIA_SCHEMA,
            )
            write_tracker(eia_tracker, eia_frame)
            export_frame(eia_frame, "eia_spot_prices", output_dir)
            print()

    if "power" in sources:
        _warn_if_delay_is_too_fast(args.request_delay)
        start = _resolve_start(
            tracker_path=power_tracker,
            schema=POWER_SCHEMA,
            history_start=POWER_HISTORY_START,
            requested_start=args.start,
            full=args.full,
            overlap_days=INCREMENTAL_OVERLAP_DAYS,
        )
        print(f"Power: downloading {start} .. {end} for {len(zones)} zones")
        existing_power = read_tracker(power_tracker, POWER_SCHEMA)

        def _checkpoint_power(frame: pl.DataFrame) -> None:
            nonlocal existing_power
            existing_power = merge_frames(
                existing_power,
                frame,
                keys=["TimestampUTC", "InstrumentID"],
                schema=POWER_SCHEMA,
            )
            write_tracker(power_tracker, existing_power)

        new_power = fetch_power(
            zones=zones,
            start=start,
            end=end,
            retrieved_at=retrieved_at,
            request_delay=args.request_delay,
            zone_callback=_checkpoint_power,
        )
        power_frame = merge_frames(
            existing_power,
            new_power,
            keys=["TimestampUTC", "InstrumentID"],
            schema=POWER_SCHEMA,
        )
        write_tracker(power_tracker, power_frame)
        export_frame(power_frame, "power_day_ahead_prices", output_dir)
        print()

    if eia_frame is None and eia_tracker.exists():
        eia_frame = read_tracker(eia_tracker, EIA_SCHEMA)
    if power_frame is None and power_tracker.exists():
        power_frame = read_tracker(power_tracker, POWER_SCHEMA)

    combined = build_combined(eia_frame, power_frame)
    export_frame(combined, "energy_trading_prices", output_dir)
    print()

    counts = {
        "combined": combined.height,
        "eia_spot_prices": eia_frame.height if eia_frame is not None else 0,
        "power_day_ahead_prices": power_frame.height if power_frame is not None else 0,
    }
    write_source_manifest(
        output_dir,
        retrieved_at=retrieved_at,
        counts=counts,
        zones=zones if "power" in sources else [],
    )

    print()
    print("=" * 78)
    print("Pipeline completed successfully")
    print(f"  combined rows:        {counts['combined']:,}")
    print(f"  EIA rows:             {counts['eia_spot_prices']:,}")
    print(f"  power rows:           {counts['power_day_ahead_prices']:,}")
    print("=" * 78)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
