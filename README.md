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

**Update Frequency**: Daily (automated via cron job)

**Columns**:
- `Date` (date): Trading date
- `Price` (float): Daily spot price in USD/Million BTU
- `Year` (int): Year extracted from date
- `Month` (int): Month extracted from date (1-12)
- `Day` (int): Day extracted from date (1-31)

## 🚀 Quick Start

### Installation

```bash
# Clone repository and navigate to directory
cd KaggleDataset

# Create and activate virtual environment
python3.13 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
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
├── .pre-commit-config.yaml         # Git hooks for code quality
├── pyproject.toml                  # Project metadata & tool config
├── setup.cfg                       # Additional tool configuration
├── .gitignore                      # Git exclusion patterns
└── README.md                       # This file
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

### Weekly Cron Job Setup

```bash
# Edit crontab
crontab -e

# Add weekly update (Sundays at 2 AM)
0 2 * * 0 cd /path/to/KaggleDataset && source .venv/bin/activate && python CrudeOil/crude_oil_brent.py >> cron.log 2>&1
```

### Manual Update

```bash
source .venv/bin/activate
python CrudeOil/crude_oil_brent.py
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
