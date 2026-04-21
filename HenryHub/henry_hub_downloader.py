"""
Henry Hub Natural Gas Spot Price Downloader & Processor

Downloads daily natural gas prices from FRED (Federal Reserve Economic Data)
which mirrors EIA data in CSV format - avoiding the xlrd/xlsx compatibility issue.

Data Source: FRED DHHNGSP (mirrors EIA Henry Hub Natural Gas Spot Price)
Series: Henry Hub Natural Gas Spot Price - Daily
Unit: USD per Million BTU
"""

import io
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import requests


BASE_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DHHNGSP"


def download_henry_hub_data():
    """Download Henry Hub Natural Gas data from FRED (CSV format)"""
    try:
        print("Downloading Henry Hub Natural Gas data from FRED...")
        response = requests.get(BASE_URL, timeout=30)
        response.raise_for_status()
        print(f"✓ Data downloaded successfully (Status: {response.status_code})")
        return response.text
    except requests.RequestException as e:
        print(f"✗ Error downloading data: {e}")
        return None


def parse_csv_data(csv_content):
    """Parse CSV content and extract Henry Hub data"""
    try:
        df = pl.read_csv(io.StringIO(csv_content))

        df = df.rename({"DHHNGSP": "Price", "observation_date": "Date"})

        df = df.with_columns(
            [
                pl.col("Date").str.to_date().alias("Date"),
                pl.col("Price").cast(pl.Float64).alias("Price"),
            ]
        )

        df = df.with_columns(
            [
                pl.col("Date").dt.year().cast(pl.Int32).alias("Year"),
                pl.col("Date").dt.month().cast(pl.Int8).alias("Month"),
                pl.col("Date").dt.day().cast(pl.Int8).alias("Day"),
            ]
        )

        df = df.drop_nulls()

        df = df.select(["Date", "Price", "Year", "Month", "Day"])

        print(f"✓ CSV parsed successfully - {len(df)} rows")
        return df
    except Exception as e:
        print(f"✗ Error parsing CSV: {e}")
        return None


def append_to_csv(csv_file_path, new_df):
    """Append new data to existing CSV file, keeping only new records"""
    csv_path = Path(csv_file_path)

    csv_path.parent.mkdir(parents=True, exist_ok=True)

    if csv_path.exists() and csv_path.stat().st_size > 0:
        try:
            existing_df = pl.read_csv(csv_path)
            existing_df = existing_df.with_columns(pl.col("Date").str.to_date())
            print(f"✓ Existing file loaded - {len(existing_df)} rows")

            existing_dates = set(existing_df["Date"].to_list())
            new_dates = set(new_df["Date"].to_list())

            missing_dates = new_dates - existing_dates

            if missing_dates:
                new_records = new_df.filter(pl.col("Date").is_in(list(missing_dates)))
                print(f"✓ Found {len(new_records)} new records to add")

                combined_df = pl.concat([existing_df, new_records])
                combined_df = combined_df.sort("Date", descending=True)
            else:
                print("✓ No new records to add - data is up to date")
                combined_df = existing_df

            print(f"✓ Combined data - {len(combined_df)} total rows")
        except Exception as e:
            print(f"✗ Error reading existing file: {e}")
            print("  → Creating new dataset with downloaded data")
            combined_df = new_df.sort("Date", descending=True)
    else:
        print("✓ Creating new dataset")
        combined_df = new_df.sort("Date", descending=True)

    try:
        combined_df.write_csv(csv_path)
        print(f"✓ Data tracker saved to {csv_path}")
    except Exception as e:
        print(f"✗ Error saving tracker file: {e}")

    return combined_df


def export_data(df, output_dir):
    """Export data to CSV, JSON, and Parquet formats"""
    output_path = Path(output_dir)

    csv_dir = output_path / "csv"
    json_dir = output_path / "json"
    parquet_dir = output_path / "parquet"

    csv_dir.mkdir(parents=True, exist_ok=True)
    json_dir.mkdir(parents=True, exist_ok=True)
    parquet_dir.mkdir(parents=True, exist_ok=True)

    csv_file = csv_dir / "henry_hub_natural_gas.csv"
    json_file = json_dir / "henry_hub_natural_gas.json"
    parquet_file = parquet_dir / "henry_hub_natural_gas.parquet"

    try:
        df_csv = df.with_columns(pl.col("Date").cast(pl.Utf8))
        df_csv.write_csv(csv_file)
        print(f"✓ CSV exported to {csv_file}")

        df.write_json(json_file)
        with open(json_file, "a") as f:
            f.write("\n")
        print(f"✓ JSON exported to {json_file}")

        df.write_parquet(parquet_file)
        print(f"✓ Parquet exported to {parquet_file}")

        print("\n✓ Folder Structure:")
        print("  └── HenryHub/")
        print("      ├── csv/")
        print(f"      │   └── henry_hub_natural_gas.csv ({len(df)} rows)")
        print("      ├── json/")
        print(f"      │   └── henry_hub_natural_gas.json ({len(df)} rows)")
        print("      └── parquet/")
        print(f"          └── henry_hub_natural_gas.parquet ({len(df)} rows)")

        return True
    except Exception as e:
        print(f"✗ Error exporting data: {e}")
        return False


def display_sample_data(df, n=10):
    """Display sample data from the DataFrame"""
    print("\n" + "=" * 80)
    print(f"Sample Data (First {n} rows)")
    print("=" * 80)
    sample_df = df.sort("Date", descending=False).head(n)
    print(sample_df)
    print("\n" + "=" * 80)
    print("Data Summary")
    print("=" * 80)
    print(f"Total Rows: {len(df)}")
    print(f"Columns: {df.columns}")
    print(f"Date Range: {df['Date'].min()} to {df['Date'].max()}")
    print(f"Price Range: ${df['Price'].min():.2f} - ${df['Price'].max():.2f} per Million BTU")
    print(f"Data Types:\n{df.schema}")
    print("=" * 80 + "\n")


def main():
    """Main function to orchestrate the download and append process"""
    print("=" * 80)
    print("Henry Hub Natural Gas Spot Price Downloader & Processor")
    print("=" * 80)
    print(f"Timestamp: {datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    csv_content = download_henry_hub_data()
    if not csv_content:
        print("Exiting due to download failure")
        return

    df = parse_csv_data(csv_content)
    if df is None or df.is_empty():
        print("Exiting due to parsing failure")
        return

    display_sample_data(df)

    script_dir = Path(__file__).parent
    csv_file = script_dir / "henry_hub_natural_gas.csv"

    combined_df = append_to_csv(csv_file, df)

    print("\n" + "=" * 80)
    print("Exporting Data")
    print("=" * 80)
    export_data(combined_df, script_dir)

    print()
    print("=" * 80)
    print("Process completed successfully!")
    print("=" * 80)


if __name__ == "__main__":
    main()
