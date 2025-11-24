# Crude Oil Brent Data Downloader

This script downloads Crude Oil Brent daily price data from the FRED (Federal Reserve Economic Data) API and automatically manages historical records while exporting to multiple formats.

## Features

✅ **Automated Data Download** - Downloads latest Crude Oil Brent prices from FRED using GET request
✅ **Duplicate Detection** - Automatically identifies and adds only new records on subsequent runs
✅ **Multiple Export Formats** - Exports to CSV, JSON, and Parquet for flexibility
✅ **Organized Folder Structure** - Separates exports by format in dedicated subdirectories
✅ **Type Safety** - Maintains proper data types (Date, Float64, Int32, Int8)
✅ **Date Components** - Automatically extracts Year, Month, and Day columns
✅ **Weekly Updates** - Can be scheduled via cron to run weekly and update Kaggle datasets

## Directory Structure

```
CrudeOil/
├── crude_oil_brent.py          # Main script
├── crude_oil_brent.csv         # Data tracker (for duplicate detection)
├── csv/
│   └── crude_oil_brent.csv     # CSV export (1265 rows)
├── json/
│   └── crude_oil_brent.json    # JSON export (1265 rows)
└── parquet/
    └── crude_oil_brent.parquet # Parquet export (1265 rows)
```

## Data Schema

| Column | Type | Description |
|--------|------|-------------|
| **Date** | Date | Trading date (YYYY-MM-DD) |
| **Price** | Float64 | Crude Oil Brent price in USD per barrel |
| **Year** | Int32 | Extracted year from date |
| **Month** | Int8 | Extracted month (1-12) |
| **Day** | Int8 | Extracted day (1-31) |

## Export Formats

### CSV Format
- **File**: `csv/crude_oil_brent.csv`
- **Date Type**: String (ISO format: YYYY-MM-DD)
- **Use Case**: Excel, databases, universal compatibility
- **Size**: ~33 KB

### JSON Format
- **File**: `json/crude_oil_brent.json`
- **Date Type**: String (ISO format)
- **Use Case**: APIs, web applications, data interchange
- **Size**: ~83 KB

### Parquet Format
- **File**: `parquet/crude_oil_brent.parquet`
- **Date Type**: Date (native type)
- **Use Case**: Big data analysis, optimal storage efficiency
- **Size**: ~9 KB (most compressed)

## Sample Data

```
Date,Price,Year,Month,Day
2025-11-17,63.16,2025,11,17
2025-11-14,63.45,2025,11,14
2025-11-13,62.14,2025,11,13
2025-11-12,61.88,2025,11,12
```

## Usage

### Run Manually
```bash
cd /Users/lakshmikanthpeddineni/Developer/Dev/Python/KaggleDataset/CrudeOil
python crude_oil_brent.py
```

### Run Weekly (Cron Job)
Add to your crontab to run every week:
```bash
# Every Monday at 9:00 AM
0 9 * * 1 cd /Users/lakshmikanthpeddineni/Developer/Dev/Python/KaggleDataset/CrudeOil && python crude_oil_brent.py
```

Or use a more frequent schedule:
```bash
# Every day at 5:00 PM
0 17 * * * cd /Users/lakshmikanthpeddineni/Developer/Dev/Python/KaggleDataset/CrudeOil && python crude_oil_brent.py
```

## How It Works

1. **Download**: Fetches latest data from FRED API
2. **Parse**: Converts raw CSV to Polars DataFrame with proper types
3. **Track**: Loads existing data from `crude_oil_brent.csv` (data tracker)
4. **Detect**: Compares dates to identify missing records
5. **Merge**: Adds only new records to prevent duplicates
6. **Export**: Saves to CSV, JSON, and Parquet formats simultaneously
7. **Update Tracker**: Saves combined dataset for next run

## Data Source

- **Source**: Federal Reserve Economic Data (FRED)
- **API Endpoint**: https://fred.stlouisfed.org/graph/fredgraph.csv
- **Series ID**: DCOILBRENTEU (Crude Oil Prices: Brent - Europe)
- **Frequency**: Daily
- **Historical Coverage**: 1987-05-20 onwards

## Uploading to Kaggle

After each weekly update, you can upload the files to Kaggle:

```bash
# Install Kaggle CLI if not already done
pip install kaggle

# Upload dataset
kaggle datasets version -p /path/to/dataset/ --message "Weekly update with latest crude oil prices"
```

## Dependencies

- `polars` - High-performance data manipulation
- `requests` - HTTP requests for API calls
- Python 3.7+

## Requirements Met ✅

- [x] Download data using GET request
- [x] Check for new/updated records
- [x] Add only missing records (no duplicates)
- [x] Export to CSV, JSON, Parquet
- [x] Organize exports in separate folders
- [x] Maintain proper data types (Date, integers)
- [x] Ready for weekly Kaggle updates
- [x] Automatic duplicate detection

## Notes

- The `crude_oil_brent.csv` in the root directory is used for duplicate tracking only
- For Kaggle uploads, include the `csv/`, `json/`, and `parquet/` folders
- First run downloads all historical data (~1265 records)
- Subsequent runs add only new records (typically 1-5 records per week)
- All files are sorted by date (newest first)
