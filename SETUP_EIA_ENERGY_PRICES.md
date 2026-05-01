# EIA Energy Prices Dataset - Complete Setup Summary

## Overview
✅ Successfully created comprehensive EIA energy prices downloader with **all available products** and **regional breakdown** data.

## Data Coverage

### Products (9 Total)
1. **WTI Crude Oil** (EPCWTI) - USD/barrel
2. **UK Brent Crude Oil** (EPCBRENT) - USD/barrel
3. **Reformulated Regular Gasoline** (EPMRR) - USD/gallon
4. **Conventional Regular Gasoline** (EPMRU) - USD/gallon
5. **No 2 Diesel Low Sulfur** (EPD2DXL0) - USD/gallon
6. **Carb Diesel** (EPD2DC) - USD/gallon
7. **No 2 Fuel Oil / Heating Oil** (EPD2F) - USD/gallon
8. **Kerosene-Type Jet Fuel** (EPJK) - USD/gallon
9. **Propane (Consumer Grade)** (EPLLPA) - USD/gallon

### Regional Breakdown (11 Series)
- **Crude Oil (2)**
  - WTI Crude Oil (RWTC)
  - Brent Crude Oil (RBRTE)

- **Gasoline - Gulf Coast** (EPMRU_RGC)
- **Gasoline - NY** (EPMRU_NY)
- **Gasoline - LA** (EPMRR_LA)

- **Diesel - Gulf Coast** (EPD2DXL0_RGC)
- **Diesel - NY** (EPD2DXL0_NY)
- **Diesel - LA** (EPD2DC_LA)

- **Heating Oil - NY** (EPD2F_NY)
- **Jet Fuel - Gulf Coast** (EPJK_RGC)
- **Propane - Midwest** (EPLLPA_MB)

## Data Structure

### CSV/JSON/Parquet Columns
```
Date           - Trading date (YYYY-MM-DD)
Product        - Energy commodity name
ProductID      - EIA product identifier
Series         - Location-specific series name
SeriesID       - EIA series identifier code
Price          - Daily spot price (varies by commodity unit)
Year           - Year extracted from date
Month          - Month (1-12)
Day            - Day (1-31)
```

### Current Dataset Stats
- **Total Records:** 5,001 rows (with header)
- **Date Range:** 2024-06-24 to 2026-04-20
- **File Formats:** CSV (532 KB), JSON, Parquet
- **Export Location:** `Gasoline/` directory

## Data Files Created
```
Gasoline/
├── csv/
│   ├── eia_energy_prices.csv (Main dataset)
│   └── wti_crude_oil.csv (Legacy/Cache)
├── json/
│   ├── eia_energy_prices.json
│   └── wti_crude_oil.json
├── parquet/
│   ├── eia_energy_prices.parquet
│   └── wti_crude_oil.parquet
└── eia_downloader.py (Downloader script)
```

## Kaggle Configuration
- **Dataset Name:** EIA Energy Prices - All Commodities
- **Kaggle URL:** lakshmi2305/wti-crude-oil-prices
- **Status:** Enabled and ready for upload
- **Pre-upload Script:** `Gasoline/eia_downloader.py`
- **Metadata:** Complete with all product descriptions and EIA API attribution

## API Parameters Used
```
Base URL: https://api.eia.gov/v2/petroleum/pri/spt/data
Method: GET
Frequency: daily
Data Fields: value
Facets:
  - product: All 9 products
  - series: All 11 regional series
Sort: period descending
Length: 5000 (max per request)
API Version: v2
```

## Usage Instructions

### Download/Update Data
```bash
python Gasoline/eia_downloader.py
```

### Upload to Kaggle
```bash
# Upload all datasets
python kaggle_uploader.py

# Upload only EIA dataset
python kaggle_uploader.py --dataset wti_crude_oil

# List all datasets
python kaggle_uploader.py --list
```

## Key Features

✅ **All EIA Products** - Fetches all available petroleum spot prices
✅ **Regional Breakdown** - Location-specific pricing (Gulf Coast, NY, LA, Midwest)
✅ **Automatic Deduplication** - Prevents duplicate records on updates
✅ **Multiple Formats** - CSV, JSON, and Parquet for different use cases
✅ **EIA Attribution** - Full metadata crediting EIA as data source
✅ **Daily Updates** - Automated weekly uploads to Kaggle
✅ **Error Handling** - Comprehensive error messages and logging

## API Documentation
- **EIA Open Data:** https://www.eia.gov/opendata/
- **API Docs:** https://www.eia.gov/opendata/documentation.php
- **Register for Key:** https://www.eia.gov/opendata/register.php

## Next Steps
1. Run downloader: `python Gasoline/eia_downloader.py`
2. Verify data: Check CSV in `Gasoline/csv/`
3. Upload to Kaggle: `python kaggle_uploader.py --dataset wti_crude_oil`

---
*Last Updated: April 27, 2026*
