# Energy Trading Prices

A license-clean energy-trading dataset with a weekly build-and-publish schedule.

It combines two sources whose licenses permit redistribution:

1. **U.S. Energy Information Administration (EIA)** — public-domain daily spot
   prices for crude oil, natural gas, and refined products.
2. **Energy-Charts API (Fraunhofer ISE) / Bundesnetzagentur | SMARD.de** —
   European day-ahead electricity prices for the 15 bidding zones that
   Energy-Charts explicitly labels **CC BY 4.0**.

Sources that do **not** allow redistribution (FRED, ENTSO-E day-ahead prices,
AEMO, EPEX/EEX/Nord Pool/ICE/CME) are deliberately excluded. See
[`DATA_LICENSES.md`](DATA_LICENSES.md) and
[`../ENERGY_DATA_LICENSE_AUDIT.md`](../ENERGY_DATA_LICENSE_AUDIT.md).

## Files

| Path | Purpose |
|---|---|
| `energy_trading_pipeline.py` | Download, normalise, merge, and export the data |
| `publish_weekly.py` | Run the pipeline and publish to Kaggle |
| `DATA_LICENSES.md` | Attribution, license terms, and change notice |
| `eia_spot_prices.csv` | Incremental tracker for EIA data (created on first run) |
| `power_day_ahead_prices.csv` | Incremental tracker for power data (created on first run) |
| `csv/`, `json/`, `parquet/` | Published exports |
| `metadata/sources.json` | Machine-readable provenance and license manifest |

Main output columns: `TimestampUTC`, `Date`, `Frequency`, `Category`,
`Instrument`, `InstrumentID`, `Market`, `Price`, `Unit`, `Currency`, `Source`,
`License`, `LicenseURL`, `SourceURL`, `RetrievedAtUTC`, `Year`, `Month`, `Day`,
`Hour`, `Minute`. `Frequency` is the interval each row covers (`hourly`,
`15min`, etc.); power zones can change resolution over their history, so it is
computed per row.

## Setup

```bash
uv sync --all-groups
```

Add an EIA API key (free: https://www.eia.gov/opendata/register.php) to the
repository `.env`:

```dotenv
EIA_API_KEY=your-key-here
```

`API_KEY` is also accepted for backward compatibility.

## Usage

Incremental update (recommended for the weekly schedule):

```bash
uv run python EnergyTrading/energy_trading_pipeline.py
```

Full history rebuild:

```bash
uv run python EnergyTrading/energy_trading_pipeline.py --full
```

Power prices only, for a fixed window:

```bash
uv run python EnergyTrading/energy_trading_pipeline.py \
  --sources power --start 2026-10-01 --end 2026-11-01
```

Build and publish in one step:

```bash
uv run python EnergyTrading/publish_weekly.py
uv run python EnergyTrading/publish_weekly.py --dry-run
```

Print a crontab line:

```bash
uv run python EnergyTrading/publish_weekly.py --print-cron
```

## Rate limiting

Energy-Charts publishes its limits in the
[OpenAPI document](https://api.energy-charts.info/openapi.json): the public
`/price` endpoint allows **2 requests per minute with a burst of 2**, and limits
are lowered dynamically under server load. The pipeline is built to stay under
that:

- **One request per bidding zone**, covering the whole date range. The API
  accepts multi-year ranges, so a full 2019→today rebuild is 15 requests rather
  than ~120 per-year requests.
- **Proactive spacing.** Requests wait at least `--request-delay` seconds apart
  (default `32.0`, just over the 30-second refill interval for 2 requests/minute).
- **Retry-After aware.** If the API still answers `429` (for example because
  load lowered the limit), the pipeline waits for the advertised `Retry-After`
  instead of guessing, and permanently slows down for the rest of the run.
- **Per-zone checkpoints.** The power tracker is written after each zone, so a
  failure part-way through keeps completed work instead of discarding it.

You normally don't need to tune anything. If you are on a shared IP that is
already rate-limited, raise the delay:

```bash
uv run python EnergyTrading/energy_trading_pipeline.py --full --request-delay 60
```

Delays below 30 seconds trigger a warning because they exceed the public limit.

## Weekly schedule

The repository-wide weekly job
[`.github/workflows/kaggle_upload.yml`](../.github/workflows/kaggle_upload.yml)
runs every **Monday at 03:17 UTC** and updates every enabled dataset, including
this one, via `publish_all_weekly.py`:

1. `uv sync --all-groups`
2. Build each dataset once (`energy_trading_pipeline.py` here)
3. Verify `metadata/sources.json` marks every source as redistributable
4. Upload each dataset without rebuilding (`SKIP_PRE_UPLOAD=1`)

[`energy_trading_weekly.yml`](../.github/workflows/energy_trading_weekly.yml)
is kept for manual energy-only rebuilds (`workflow_dispatch`).

Required repository secrets:

- `EIA_API_KEY` (or `API_KEY`)
- `KAGGLE_USERNAME`
- `KAGGLE_KEY`

Manual runs accept `full` (rebuild all history) and `sources` inputs.

For a local cron job:

```cron
17 3 * * 1 cd /path/to/KaggleDataset && .venv/bin/python EnergyTrading/publish_weekly.py >> energy_trading.log 2>&1
```

## License safety

- The dataset is published under **CC BY 4.0**, the most restrictive upstream
  license.
- Every Energy-Charts response is checked with `ensure_redistributable()`. If the
  API stops reporting CC BY 4.0 for a zone, the pipeline raises `LicenseError`
  and refuses to publish.
- `metadata/sources.json` records the license, license URL, attribution, and
  change notice for each source, plus the excluded sources and why.
- The CI workflow re-checks the manifest before uploading.

## Tests

```bash
uv run pytest tests/test_energy_trading.py
uv run ruff check EnergyTrading tests
```
