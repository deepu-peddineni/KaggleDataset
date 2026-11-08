# Kaggle Datasets - Commodity Price Data

A comprehensive Python project for downloading, processing, and exporting commodity price data from FRED (Federal Reserve Economic Data) in multiple formats (CSV, JSON, Parquet).

## 📊 Datasets

### Crude Oil Brent Prices

**Description**: Daily crude oil Brent prices in USD per barrel from the Federal Reserve Economic Data (FRED) API.

**Data Source**: [FRED Series DCOILBRENTEU](https://fred.stlouisfed.org/series/DCOILBRENTEU)

**Time Range**: 2020-11-17 to Present (continuously updated)

**Total Records**: 1,265+ price observations

**Price Range**: $42.54 - $133.18 per barrel

**Update Frequency**: Weekly (automated via cron job)

**Columns**:
- `Date` (date): Trading date
- `Price` (float): Daily closing price in USD/barrel
- `Year` (int): Year extracted from date
- `Month` (int): Month extracted from date (1-12)
- `Day` (int): Day extracted from date (1-31)

### Henry Hub Natural Gas Spot Prices

**Description**: Daily Henry Hub natural gas spot prices in USD per million BTU from the U.S. Energy Information Administration (EIA).

**Data Source**: [EIA Henry Hub Natural Gas Spot Price - Daily](https://www.eia.gov/dnav/ng/hist_xls/RNGWHHDd.xls)

**Time Range**: 1997-01-07 to Present (continuously updated)

**Total Records**: 7,252+ price observations

**Price Range**: $1.05 - $23.86 per Million BTU

**Update Frequency**: Weekly (automated, Mondays at 03:17 UTC)

**Columns**:
- `Date` (date): Trading date
- `Price` (float): Daily spot price in USD/Million BTU
- `Year` (int): Year extracted from date
- `Month` (int): Month extracted from date (1-12)
- `Day` (int): Day extracted from date (1-31)

### Energy Trading Prices

**Description**: License-clean energy trading dataset combining U.S. spot prices
with European day-ahead electricity prices in one tidy long table.

**Data Sources**:
- U.S. Energy Information Administration (EIA) Open Data API — public domain
- Bundesnetzagentur | SMARD.de via the Energy-Charts API (Fraunhofer ISE) —
  CC BY 4.0

**License**: CC BY 4.0 (most restrictive upstream license). See
[`ENERGY_DATA_LICENSE_AUDIT.md`](ENERGY_DATA_LICENSE_AUDIT.md) and
[`EnergyTrading/DATA_LICENSES.md`](EnergyTrading/DATA_LICENSES.md).

**Time Range**: 1990-01-01 to Present (EIA); 2019-01-01 to Present (power)

**Update Frequency**: Weekly (automated, Mondays at 03:17 UTC)

**Files**: Combined table plus `eia_spot_prices` and
`power_day_ahead_prices` extracts in CSV, JSON, and Parquet, a
`metadata/sources.json` license manifest, and `DATA_LICENSES.md`.

**Main Columns**: `TimestampUTC`, `Date`, `Frequency`, `Category`,
`Instrument`, `InstrumentID`, `Market`, `Price`, `Unit`, `Currency`, `Source`,
`License`, `LicenseURL`, `SourceURL`, `RetrievedAtUTC`, `Year`, `Month`, `Day`,
`Hour`, `Minute`.

Deliberately excluded for licensing reasons: FRED, ENTSO-E day-ahead prices,
AEMO, and EPEX/EEX/Nord Pool/ICE/CME exchange data.

## 🚀 Quick Start

### Prerequisites

- **Python 3.13+** (required)
- **[uv](https://docs.astral.sh/uv/)** - Fast Python package installer and resolver (recommended)
  - Install: `curl -LsSf https://astral.sh/uv/install.sh | sh` (macOS/Linux)
  - Or: `powershell -Command "irm https://astral.sh/uv/install.ps1 | iex"` (Windows)

### Installation with uv (Recommended)

```bash
# Clone repository and navigate to directory
git clone https://github.com/deepu-peddineni/KaggleDataset.git
cd KaggleDataset

# Create and activate virtual environment with uv
uv venv

# Activate the environment
source .venv/bin/activate  # macOS/Linux
# or
.venv\Scripts\activate  # Windows

# Install dependencies using uv (fast, deterministic)
uv sync
```

### Installation with pip (Alternative)

```bash
# Clone repository and navigate to directory
git clone https://github.com/deepu-peddineni/KaggleDataset.git
cd KaggleDataset

# Create and activate virtual environment
python3.13 -m venv .venv
source .venv/bin/activate  # macOS/Linux

# Install dependencies
pip install -e .
```

### Package Management with uv

#### Add Dependencies
```bash
# Add a new production dependency
uv add polars>=1.35.0

# Add a development-only dependency
uv add --dev pytest

# Add to specific group
uv add --group dev pytest-cov
```

#### Update Dependencies
```bash
# Update all dependencies to latest compatible versions
uv sync --upgrade

# Update a specific package
uv pip install --upgrade polars
```

#### View Dependency Tree
```bash
# Show all installed packages and their versions
uv pip list

# Show dependency details
uv pip show polars
```

#### Lock File Management
```bash
# Generate/update uv.lock (committed to git)
uv lock

# Install from lock file (deterministic, reproducible builds)
uv sync --frozen  # Error if lock file needs update
```

### Running the Data Pipeline

```bash
# Download latest Crude Oil data and export to all formats
python CrudeOil/crude_oil_brent.py

# Download latest Henry Hub Natural Gas data and export to all formats
python HenryHub/henry_hub_downloader.py
```

**Crude Oil Output**:
```
================================================================================
Crude Oil Brent Data Downloader & Processor
================================================================================
Timestamp: 2025-11-24 20:50:49

Downloading Crude Oil Brent data from FRED...
✓ Data downloaded successfully (Status: 200)
✓ CSV parsed successfully - 1265 rows

Sample Data (First 10 rows)
[Data table showing oldest prices]

✓ Existing file loaded - 1265 rows
✓ No new records to add - data is up to date
✓ Combined data - 1265 total rows
✓ Data tracker saved to ./CrudeOil/crude_oil_brent.csv

Exporting Data
✓ CSV exported to ./CrudeOil/csv/crude_oil_brent.csv
✓ JSON exported to ./CrudeOil/json/crude_oil_brent.json
✓ Parquet exported to ./CrudeOil/parquet/crude_oil_brent.parquet

Process completed successfully!
```

**Henry Hub Output**:
```
================================================================================
Henry Hub Natural Gas Spot Price Downloader & Processor
================================================================================
Timestamp: 2025-11-24 21:03:02

Downloading Henry Hub Natural Gas data from EIA...
✓ Data downloaded successfully (Status: 200)
✓ Excel parsed successfully - 7252 rows

Sample Data (First 10 rows)
[Data table showing prices from 1997]

✓ Creating new dataset
✓ Data tracker saved to ./HenryHub/henry_hub_natural_gas.csv

Exporting Data
✓ CSV exported to ./HenryHub/csv/henry_hub_natural_gas.csv
✓ JSON exported to ./HenryHub/json/henry_hub_natural_gas.json
✓ Parquet exported to ./HenryHub/parquet/henry_hub_natural_gas.parquet

Process completed successfully!
```

## 📁 Project Structure

```
KaggleDataset/
├── pyproject.toml                  # Project metadata & uv config
├── uv.lock                         # Lock file (deterministic dependencies)
├── .pre-commit-config.yaml         # Git hooks for code quality
├── setup.cfg                       # Additional tool configuration
├── .gitignore                      # Git exclusion patterns
├── README.md                       # This file
│
├── CrudeOil/
│   ├── crude_oil_brent.py          # Main data downloader & processor script
│   ├── crude_oil_brent.csv         # Data tracker (for duplicate detection)
│   ├── csv/
│   │   └── crude_oil_brent.csv     # CSV export (1,265 rows, 34 KB)
│   ├── json/
│   │   └── crude_oil_brent.json    # JSON export (1,265 rows, 85 KB)
│   └── parquet/
│       └── crude_oil_brent.parquet # Parquet export (1,265 rows, 9.1 KB)
│
├── HenryHub/
│   ├── henry_hub_downloader.py     # Natural Gas data downloader & processor
│   ├── henry_hub_natural_gas.csv   # Data tracker (for duplicate detection)
│   ├── csv/
│   │   └── henry_hub_natural_gas.csv     # CSV export (7,252 rows, 95 KB)
│   ├── json/
│   │   └── henry_hub_natural_gas.json    # JSON export (7,252 rows, 285 KB)
│   └── parquet/
│       └── henry_hub_natural_gas.parquet # Parquet export (7,252 rows, 28 KB)
│
├── EnergyTrading/
│   ├── energy_trading_pipeline.py  # EIA + Energy-Charts downloader/exporter
│   ├── publish_weekly.py           # Weekly build + Kaggle publish
│   ├── DATA_LICENSES.md            # Attribution and license terms
│   ├── README.md                   # Module usage notes
│   ├── eia_spot_prices.csv         # EIA incremental tracker
│   ├── power_day_ahead_prices.csv  # Power incremental tracker
│   ├── csv/                        # CSV exports
│   ├── json/                       # JSON exports
│   ├── parquet/                    # Parquet exports
│   └── metadata/
│       └── sources.json            # Machine-readable license manifest
│
├── publish_all_weekly.py           # Build + publish every enabled dataset
├── kaggle_uploader.py              # Kaggle upload engine
├── kaggle_config.yaml              # Dataset definitions and metadata
│
└── .venv/                          # Virtual environment (uv venv)
```

## 🔧 Project Configuration with uv

### pyproject.toml Overview

```toml
[project]
name = "kaggledataset"
version = "0.1.0"
requires-python = ">=3.13"
dependencies = [
    "polars>=1.35.2",      # DataFrame library
    "pandas>=2.3.3",       # Data manipulation
    "requests>=2.32.5",    # HTTP requests
    "duckdb>=1.4.2",       # SQL queries on data
    "pyarrow>=22.0.0",     # Parquet support
    "xlrd>=2.0.2",         # XLS file reading
    "openpyxl>=3.1.5",     # XLSX support
    "fastexcel>=0.18.0",   # Fast Excel parsing
    "ruff>=0.14.6",        # Formatter & linter
    "mypy>=1.18.2",        # Type checker
]

[project.optional-dependencies]
dev = [
    "pre-commit>=3.5.0",   # Git hooks
    "pytest>=7.4.3",       # Testing framework
    "pytest-cov>=4.1.0",   # Coverage reports
    "bandit>=1.7.5",       # Security analyzer
]
```

### uv.lock - Dependency Lock File

The `uv.lock` file ensures reproducible builds:

```bash
# Generated automatically by uv
# Pinned versions prevent surprises in CI/CD and production
uv sync --frozen  # Uses locked versions, fails if out of sync
```

Benefits:
- ✓ Deterministic builds across machines
- ✓ Exact version reproduction
- ✓ Faster dependency resolution
- ✓ Security: no version surprises

## 🛠️ Common uv Workflows

### Development Workflow

```bash
# Clone and setup with uv
git clone https://github.com/deepu-peddineni/KaggleDataset.git
cd KaggleDataset

# Create virtual environment and install all dependencies
uv venv
source .venv/bin/activate
uv sync

# Install development dependencies too
uv sync --all-groups
```

### Add New Dependency

```bash
# Add production dependency
uv add requests-retry

# Add development dependency
uv add --group dev pytest-xdist

# Add optional dependency group
uv add --optional test pytest

# Update lock file (done automatically)
git add uv.lock pyproject.toml
git commit -m "chore: add new dependencies"
```

### Upgrade Dependencies

```bash
# Upgrade all to latest compatible versions
uv lock --upgrade

# Upgrade specific package to latest
uv lock --upgrade-package polars

# Check for outdated packages
uv pip list --outdated
```

### Run Scripts with uv

```bash
# Run Python script in project environment
uv run python CrudeOil/crude_oil_brent.py

# Run with specific Python version
uv run --python 3.13 HenryHub/henry_hub_downloader.py

# Run tests
uv run pytest tests/

# Run formatter
uv run ruff format .

# Run linter
uv run ruff check . --fix
```

### Docker with uv

```dockerfile
FROM python:3.13-slim
WORKDIR /app

# Install uv
RUN pip install uv

# Copy project
COPY pyproject.toml uv.lock ./

# Install dependencies
RUN uv sync --frozen --no-dev

# Run application
CMD ["uv", "run", "python", "CrudeOil/crude_oil_brent.py"]
```

### CI/CD with uv

```yaml
# .github/workflows/ci.yml
name: CI
on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v3
      - uses: actions/setup-python@v5
        with:
          python-version: "3.13"
      - run: uv sync --all-groups
      - run: uv run pytest
      - run: uv run ruff format --check .
      - run: uv run ruff check .
```

## 📊 uv vs pip - Comparison

| Feature | pip | uv |
|---------|-----|-----|
| **Speed** | Slow (serial resolution) | 🚀 **10-100x faster** |
| **Lock File** | ❌ No native support | ✓ `uv.lock` built-in |
| **Deterministic Builds** | ❌ Requires pip-tools | ✓ Native |
| **Dependency Resolution** | ❌ Backtracking only | ✓ Full SAT solver |
| **Installation** | `pip install` | `uv add` |
| **Update All** | Manual process | `uv lock --upgrade` |
| **Memory Usage** | High | **Low** |
| **Virtual Environments** | `venv` module | `uv venv` (integrated) |
| **Reproducibility** | Difficult | **Easy with uv.lock** |
| **Developer Experience** | Basic | **Rich output, progress bars** |

### Quick Command Comparison

```bash
# Creating environment
pip: python -m venv .venv
uv:  uv venv

# Installing dependencies
pip: pip install -r requirements.txt
uv:  uv sync

# Adding package
pip: pip install package && pip freeze > requirements.txt
uv:  uv add package  # Auto-updates uv.lock

# Upgrading all
pip: pip install --upgrade pip && pip install -r requirements.txt --upgrade
uv:  uv lock --upgrade

# Running scripts
pip: python script.py
uv:  uv run python script.py
```

## 📖 Usage Examples

### Python - Reading Data with Polars

```python
import polars as pl

# Read CSV
df_csv = pl.read_csv("CrudeOil/csv/crude_oil_brent.csv")
print(df_csv.head())
print(f"Total records: {len(df_csv)}")

# Read Parquet (fastest, compressed)
df_parquet = pl.read_parquet("CrudeOil/parquet/crude_oil_brent.parquet")

# Read JSON
df_json = pl.read_json("CrudeOil/json/crude_oil_brent.json")

# Filter and analyze
recent = df_csv.filter(pl.col("Year") == 2025)
avg_price = recent.select(pl.col("Price").mean()).item()
print(f"Average 2025 price: ${avg_price:.2f}")

# Group by month and calculate statistics
monthly_stats = df_csv.group_by("Month").agg(
    pl.col("Price").mean().alias("avg_price"),
    pl.col("Price").min().alias("min_price"),
    pl.col("Price").max().alias("max_price"),
    pl.col("Price").count().alias("observations")
)
print(monthly_stats)
```

### Python - Pandas Alternative

```python
import pandas as pd

# Read CSV
df = pd.read_csv("CrudeOil/csv/crude_oil_brent.csv")

# Convert Date column to datetime
df['Date'] = pd.to_datetime(df['Date'])

# Filter 2024 data
df_2024 = df[df['Year'] == 2024]
print(f"2024 average price: ${df_2024['Price'].mean():.2f}")

# Time series plot
df.set_index('Date')['Price'].plot(figsize=(12, 6))
plt.title('Brent Crude Oil Prices')
plt.ylabel('Price (USD/barrel)')
plt.show()
```

### SQL Query - DuckDB

```python
import duckdb

# Query Parquet file directly
result = duckdb.query("""
    SELECT
        Year,
        Month,
        COUNT(*) as trading_days,
        AVG(Price) as avg_price,
        MIN(Price) as min_price,
        MAX(Price) as max_price
    FROM 'CrudeOil/parquet/crude_oil_brent.parquet'
    WHERE Year >= 2024
    GROUP BY Year, Month
    ORDER BY Year, Month
""").to_df()

print(result)
```

### Command Line - DuckDB

```bash
# Query directly from command line
duckdb << EOF
SELECT Date, Price, Year, Month
FROM 'CrudeOil/parquet/crude_oil_brent.parquet'
WHERE Price > 100
ORDER BY Price DESC
LIMIT 10;
EOF
```

## 🔄 Data Pipeline Features

### Intelligent Duplicate Detection
- Maintains tracker file (`crude_oil_brent.csv` in root)
- Only adds new records on subsequent runs
- Compares dates to avoid duplicates
- Efficient incremental updates

### Multi-Format Export
- **CSV**: Universal format, human-readable, easy to share
- **JSON**: Preserves date types, API-friendly
- **Parquet**: Compressed columnar format, optimal for analytics

### Type Preservation
```
CSV:     Date as ISO string (YYYY-MM-DD), Price/Year/Month/Day as text
JSON:    Date as ISO string, numeric fields preserve types
Parquet: Full type preservation (Date type, Float64, Int32, Int8)
```

### Data Quality
- ✓ No duplicate records
- ✓ Proper type casting (dates, integers, floats)
- ✓ Complete year/month/day extraction
- ✓ Chronologically sorted (newest first)

## 🛠️ Code Quality & Security

### Pre-commit Hooks
Automated checks on every git commit:

```bash
# Manual pre-commit run
pre-commit run --all-files
```

**Checks performed**:
- ✓ File size limits (max 1000 KB)
- ✓ YAML/JSON/TOML syntax validation
- ✓ Python syntax checking
- ✓ Secret detection (credentials, API keys)
- ✓ Code security analysis (bandit)
- ✓ Python formatting (ruff format)
- ✓ Code linting (ruff check)
- ✓ Trailing whitespace removal
- ✓ Line ending normalization

### Python Tools Configuration

**Ruff** (Formatter & Linter):
```toml
# pyproject.toml
line-length = 100
target-version = "py312"
```

**MyPy** (Type Checker):
```bash
mypy CrudeOil/crude_oil_brent.py
```

## 📅 Automation

### Update every dataset in one command

`publish_all_weekly.py` builds every enabled dataset in `kaggle_config.yaml`
once, then uploads each one to Kaggle without rebuilding:

```bash
uv run python publish_all_weekly.py --yes
```

Useful variants:

```bash
# Only some datasets
uv run python publish_all_weekly.py --yes --dataset energy_trading,crude_oil_brent

# Upload the files already on disk, without downloading fresh data
uv run python publish_all_weekly.py --skip-build --yes

# Validate files without calling the Kaggle API
uv run python publish_all_weekly.py --dry-run

# Build only, do not upload
uv run python publish_all_weekly.py --skip-upload
```

It exits non-zero if any dataset fails, so cron and CI can alert. The weekly
GitHub Actions workflow [`kaggle_upload.yml`](.github/workflows/kaggle_upload.yml)
runs this command every Monday at 03:17 UTC.

### Weekly Cron Job Setup

```bash
# Edit crontab
crontab -e

# Add weekly update (Mondays at 3:17 AM)
17 3 * * 1 cd /path/to/KaggleDataset && uv run python publish_all_weekly.py --yes >> cron.log 2>&1
```

### Manual Update

```bash
uv run python publish_all_weekly.py --yes --dataset crude_oil_brent
```

## 📊 Data Statistics

**Crude Oil Dataset** (as of 2025-11-24):
- Total records: 1,265
- Date range: 2020-11-17 to 2025-11-17
- Price range: $42.54 - $133.18/barrel
- Average price: $76.42/barrel
- Highest price: $133.18 (2022-03-08)
- Lowest price: $42.54 (2020-11-17)

**Henry Hub Natural Gas Dataset** (as of 2025-11-24):
- Total records: 7,252
- Date range: 1997-01-07 to 2025-11-17
- Price range: $1.05 - $23.86/Million BTU
- Average price: $5.42/Million BTU
- Highest price: $23.86 (2022-03-07)
- Lowest price: $1.05 (2020-04-20)

**File Sizes**:
- Crude Oil CSV: 34 KB | JSON: 85 KB | Parquet: 9.1 KB
- Henry Hub CSV: 95 KB | JSON: 285 KB | Parquet: 28 KB

## 🔐 Environment Variables

No API key required for FRED public data. All requests are unauthenticated GET requests.

## ⚡ uv Best Practices

### Project Setup Checklist

```bash
# ✓ Initialize with pyproject.toml
uv venv
source .venv/bin/activate
uv sync

# ✓ Commit lock file to git
git add uv.lock
git commit -m "chore: add uv.lock for reproducible builds"

# ✓ Use specific version constraints
uv add "polars>=1.35,<2.0"  # More specific than >=1.35

# ✓ Separate dev dependencies
uv add --group dev pytest

# ✓ Pin Python version in pyproject.toml
# requires-python = ">=3.13"
```

### Dependency Management Tips

```bash
# Check for security vulnerabilities
uv pip audit

# See dependency tree
uv tree

# Verify reproducibility
uv sync --frozen --all-groups

# Keep lock file up-to-date
git diff uv.lock  # Review changes before committing
```

### Team Collaboration

```bash
# When pulling changes with new dependencies
uv sync

# When adding dependencies
uv add package-name
git add uv.lock pyproject.toml
git push

# CI should use --frozen for reproducibility
uv sync --frozen --all-groups
```

### Performance Tips

```bash
# Use uv.lock for production deployments
# Skip development dependencies
uv sync --frozen

# Cache dependencies in Docker
# Place COPY pyproject.toml uv.lock before COPY .
RUN uv sync --frozen

# Use --no-binary for specific packages if needed
uv add --no-binary package-name
```

## 🐛 Troubleshooting

### Date Type Mismatch Error
```
Error: type Date is incompatible with expected type String
```
**Solution**: The script automatically handles this by casting loaded CSV dates to Date type for comparison.

### Missing Newline Error
```
Failed: end-of-file-fixer
Fixing CrudeOil/json/crude_oil_brent.json
```
**Solution**: The script automatically appends newlines to JSON files after export.

### Pre-commit Hook Failures
```bash
# Fix all auto-fixable issues
pre-commit run --all-files --fix

# Run specific hook
pre-commit run ruff --all-files
```

### uv Installation Issues

```bash
# If uv command not found, reinstall
curl -LsSf https://astral.sh/uv/install.sh | sh

# Update uv to latest version
uv self update

# Clear uv cache if corrupted
uv cache clean
```

## 📝 Script Internals

### Download Process
1. GET request to FRED API with 35+ query parameters
2. Parse CSV response with Polars
3. Rename columns and extract date components
4. Return DataFrame with 1,265 records

### Deduplication Process
1. Load existing tracker file
2. Compare dates between existing and new data
3. Filter only missing dates
4. Merge and sort chronologically

### Export Process
1. Convert Date column to string for CSV
2. Write CSV (34 KB)
3. Write JSON with newline (85 KB)
4. Write Parquet with compression (9.1 KB)

## 🤝 Contributing

When adding new datasets:

1. Create new directory under project root
2. Implement processor script following existing pattern
3. Update `.pre-commit-config.yaml` if needed
4. Add documentation to README.md
5. Test with `pre-commit run --all-files`

## 📄 License

This project uses public data from FRED (Federal Reserve Economic Data).

## 🔗 Resources

- [FRED API Documentation](https://fred.stlouisfed.org/docs/api/)
- [DCOILBRENTEU Series](https://fred.stlouisfed.org/series/DCOILBRENTEU)
- [Polars Documentation](https://docs.pola-rs.com/)
- [Parquet Format](https://parquet.apache.org/)
- [Pre-commit Framework](https://pre-commit.com/)

## 📞 Support

For issues or questions:
1. Check the Troubleshooting section above
2. Review script output carefully
3. Verify FRED API is accessible
4. Check git status and pre-commit logs
