import io
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import polars as pl
import requests


# EIA Excel URL for Crude Oil Brent
BASE_URL = "https://www.eia.gov/dnav/pet/hist_xls/RBRTEd.xls"


def download_crude_oil_data():
    """Download Crude Oil Brent data from EIA (Excel)"""
    try:
        print("Downloading Crude Oil Brent data from EIA...")
        response = requests.get(BASE_URL, timeout=30)
        response.raise_for_status()
        print(f"✓ Data downloaded successfully (Status: {response.status_code})")
        return response.content
    except requests.RequestException as e:
        print(f"✗ Error downloading data: {e}")
        return None


def parse_excel_data(excel_content):
    """Parse Excel content and extract Crude Oil Brent data"""
    try:
        # Read Excel file with pandas (safer for XLS format)
        df_pd = pd.read_excel(
            io.BytesIO(excel_content), sheet_name="Data 1", engine="xlrd", header=None
        )

        # Skip header rows, data usually starts at row index 2
        df_pd = df_pd.iloc[2:].reset_index(drop=True)

        # Select first two columns (Date and Price)
        df_pd = df_pd.iloc[:, [0, 1]]
        df_pd.columns = ["Date", "Price"]

        # Filter out any header-like rows
        df_pd = df_pd[df_pd["Date"] != "Date"].reset_index(drop=True)

        # Remove rows with NaN values
        df_pd = df_pd.dropna()

        # Convert datetime and numeric types
        df_pd["Date"] = pd.to_datetime(df_pd["Date"]).dt.strftime("%Y-%m-%d")
        df_pd["Price"] = pd.to_numeric(df_pd["Price"], errors="coerce")
        df_pd = df_pd.dropna()

        # Convert to Polars
        df = pl.from_pandas(df_pd)

        df = (
            df.with_columns(
                [
                    pl.col("Date").str.to_date().alias("Date"),
                    pl.col("Price").cast(pl.Float64).alias("Price"),
                ]
            )
            .with_columns(
                [
                    pl.col("Date").dt.year().cast(pl.Int32).alias("Year"),
                    pl.col("Date").dt.month().cast(pl.Int8).alias("Month"),
                    pl.col("Date").dt.day().cast(pl.Int8).alias("Day"),
                ]
            )
            .select(["Date", "Price", "Year", "Month", "Day"])
        )

        print(f"✓ Excel parsed successfully - {len(df)} rows")
        return df
    except Exception as e:
        print(f"✗ Error parsing Excel: {e}")
        return None


def append_to_csv(csv_file_path, new_df):
    """Append new data to existing CSV file, keeping only new records"""
    csv_path = Path(csv_file_path)

    # Create directory if it doesn't exist
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    # Check if file exists and has data
    if csv_path.exists() and csv_path.stat().st_size > 0:
        try:
            existing_df = pl.read_csv(csv_path)
            # Ensure Date column is cast to Date type for comparison
            existing_df = existing_df.with_columns(pl.col("Date").str.to_date())

            # Normalize column types to match new data (prevent type mismatch)
            if "Price" in existing_df.columns:
                existing_df = existing_df.with_columns(pl.col("Price").cast(pl.Float64))
            if "Year" in existing_df.columns:
                existing_df = existing_df.with_columns(pl.col("Year").cast(pl.Int32))
            if "Month" in existing_df.columns:
                existing_df = existing_df.with_columns(pl.col("Month").cast(pl.Int8))
            if "Day" in existing_df.columns:
                existing_df = existing_df.with_columns(pl.col("Day").cast(pl.Int8))

            print(f"✓ Existing file loaded - {len(existing_df)} rows")

            # Get existing dates
            existing_dates = set(existing_df["Date"].to_list())
            new_dates = set(new_df["Date"].to_list())

            # Find only new records
            missing_dates = new_dates - existing_dates

            if missing_dates:
                new_records = new_df.filter(pl.col("Date").is_in(list(missing_dates)))
                print(f"✓ Found {len(new_records)} new records to add")

                # Combine data
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

    # Save to tracker file for next run
    try:
        combined_df.write_csv(csv_path)
        print(f"✓ Data tracker saved to {csv_path}")
    except Exception as e:
        print(f"✗ Error saving tracker file: {e}")

    return combined_df


def export_data(df, output_dir):
    """Export DataFrame to CSV, JSON, and Parquet formats in respective folders"""
    output_path = Path(output_dir)

    # Create subdirectories for each format
    csv_dir = output_path / "csv"
    json_dir = output_path / "json"
    parquet_dir = output_path / "parquet"

    csv_dir.mkdir(parents=True, exist_ok=True)
    json_dir.mkdir(parents=True, exist_ok=True)
    parquet_dir.mkdir(parents=True, exist_ok=True)

    # Define file paths
    csv_file = csv_dir / "crude_oil_brent.csv"
    json_file = json_dir / "crude_oil_brent.json"
    parquet_file = parquet_dir / "crude_oil_brent.parquet"

    try:
        # Export to CSV (with proper type handling for CSV format)
        # CSV will have Date as string, but Parquet/JSON will preserve Date type
        df_csv = df.with_columns(pl.col("Date").cast(pl.Utf8))
        df_csv.write_csv(csv_file)
        print(f"✓ CSV exported to {csv_file}")

        # Export to JSON (preserves Date type)
        df.write_json(json_file)
        # Add newline to JSON file (pre-commit requires files to end with newline)
        with open(json_file, "a") as f:
            f.write("\n")
        print(f"✓ JSON exported to {json_file}")

        # Export to Parquet (preserves all types including Date)
        df.write_parquet(parquet_file)
        print(f"✓ Parquet exported to {parquet_file}")

        # Print folder structure
        print("\n✓ Folder Structure:")
        print("  └── CrudeOil/")
        print("      ├── csv/")
        print(f"      │   └── crude_oil_brent.csv ({len(df)} rows)")
        print("      ├── json/")
        print(f"      │   └── crude_oil_brent.json ({len(df)} rows)")
        print("      └── parquet/")
        print(f"          └── crude_oil_brent.parquet ({len(df)} rows)")

        return True
    except Exception as e:
        print(f"✗ Error exporting data: {e}")
        return False


def display_sample_data(df, n=10):
    """Display sample data from the DataFrame"""
    print("\n" + "=" * 80)
    print(f"Sample Data (First {n} rows)")
    print("=" * 80)
    # Sort by Date ascending to show oldest first
    sample_df = df.sort("Date", descending=False).head(n)
    print(sample_df)
    print("\n" + "=" * 80)
    print("Data Summary")
    print("=" * 80)
    print(f"Total Rows: {len(df)}")
    print(f"Columns: {df.columns}")
    print(f"Date Range: {df['Date'].min()} to {df['Date'].max()}")
    print(f"Price Range: ${df['Price'].min():.2f} - ${df['Price'].max():.2f} per barrel")
    print(f"Data Types:\n{df.schema}")
    print("=" * 80 + "\n")


def main():
    """Main function to orchestrate the download and append process"""
    print("=" * 80)
    print("Crude Oil Brent Data Downloader & Processor")
    print("=" * 80)
    print(f"Timestamp: {datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # Download data (Excel)
    excel_content = download_crude_oil_data()
    if not excel_content:
        print("Exiting due to download failure")
        return

    # Parse Excel and add date components
    df = parse_excel_data(excel_content)
    if df is None or df.is_empty():
        print("Exiting due to parsing failure")
        return

    # Display sample data
    display_sample_data(df)

    # Define CSV file path (same directory as this script)
    script_dir = Path(__file__).parent
    csv_file = script_dir / "crude_oil_brent.csv"

    # Append to existing data and get updated dataset
    combined_df = append_to_csv(csv_file, df)

    # Export to multiple formats
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
