# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "polars>=1.35.2",
#     "python-dotenv>=1.2.2",
#     "requests>=2.32.5",
# ]
# ///
"""
EIA Energy Prices Downloader & Processor

Downloads daily energy commodity spot prices from the EIA Open Data API.
Includes multiple products: WTI Crude Oil, Brent Crude, Gasoline, Diesel, Heating Oil, Jet Fuel, and Propane.

Data Source: EIA APIv2 - Petroleum Prices (Spot Prices)
API Route: /v2/petroleum/pri/spt
API Documentation: https://www.eia.gov/opendata/documentation.php

Products Available:
- EPCWTI: WTI Crude Oil
- EPCBRENT: UK Brent Crude Oil
- EPMRR: Reformulated Regular Gasoline
- EPMRU: Conventional Regular Gasoline
- EPD2DXL0: No 2 Diesel Low Sulfur
- EPD2DC: Carb Diesel
- EPD2F: No 2 Fuel Oil / Heating Oil
- EPJK: Kerosene-Type Jet Fuel
- EPLLPA: Propane (Consumer Grade)
"""

import json
import os
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import requests
from dotenv import load_dotenv

# Load API key from .env file
load_dotenv()
EIA_API_KEY = os.getenv("API_KEY")

if not EIA_API_KEY:
    raise ValueError("API_KEY not found in .env file. Please add your EIA API key.")

BASE_URL = "https://api.eia.gov/v2"

# Product mapping for easier reference
ENERGY_PRODUCTS = {
    "EPCWTI": "WTI Crude Oil",
    "EPCBRENT": "UK Brent Crude Oil",
    "EPMRR": "Reformulated Regular Gasoline",
    "EPMRU": "Conventional Regular Gasoline",
    "EPD2DXL0": "No 2 Diesel Low Sulfur",
    "EPD2DC": "Carb Diesel",
    "EPD2F": "No 2 Fuel Oil / Heating Oil",
    "EPJK": "Kerosene-Type Jet Fuel",
    "EPLLPA": "Propane (Consumer Grade)",
}

# Specific series for regional/location-based data
REGIONAL_SERIES = [
    "EER_EPD2DC_PF4_Y05LA_DPG",  # Carb Diesel (LA/Gulf Coast)
    "EER_EPD2DXL0_PF4_RGC_DPG",  # No 2 Diesel Low Sulfur (Gulf Coast)
    "EER_EPD2DXL0_PF4_Y35NY_DPG",  # No 2 Diesel Low Sulfur (NY)
    "EER_EPD2F_PF4_Y35NY_DPG",  # No 2 Fuel Oil (NY)
    "EER_EPJK_PF4_RGC_DPG",  # Kerosene-Type Jet Fuel (Gulf Coast)
    "EER_EPLLPA_PF4_Y44MB_DPG",  # Propane (Midwest)
    "EER_EPMRR_PF4_Y05LA_DPG",  # Reformulated Regular Gasoline (LA)
    "EER_EPMRU_PF4_RGC_DPG",  # Conventional Regular Gasoline (Gulf Coast)
    "EER_EPMRU_PF4_Y35NY_DPG",  # Conventional Regular Gasoline (NY)
    "RBRTE",  # Brent Crude Oil
    "RWTC",  # WTI Crude Oil
]


def get_available_products() -> dict:
    """
    Fetch all available products from EIA Spot Prices endpoint.

    Returns:
        Dictionary mapping product IDs to product names
    """
    try:
        url = f"{BASE_URL}/petroleum/pri/spt/facet/product?api_key={EIA_API_KEY}"
        print("Fetching available products from EIA API...")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()

        products = {}
        if "response" in data and "facets" in data["response"]:
            for facet in data["response"]["facets"]:
                product_id = facet.get("id")
                product_name = facet.get("name", product_id)
                products[product_id] = product_name

        print(f"✓ Found {len(products)} available products")
        return products
    except Exception as e:
        print(f"✗ Error fetching products: {e}")
        return ENERGY_PRODUCTS  # Fallback to hardcoded products


def get_regional_series_mapping() -> dict:
    """
    Get mapping of regional series IDs to more descriptive names.

    Returns:
        Dictionary mapping series IDs to descriptions
    """
    return {
        "EER_EPD2DC_PF4_Y05LA_DPG": "Carb Diesel (LA/Gulf Coast)",
        "EER_EPD2DXL0_PF4_RGC_DPG": "No 2 Diesel Low Sulfur (Gulf Coast)",
        "EER_EPD2DXL0_PF4_Y35NY_DPG": "No 2 Diesel Low Sulfur (NY)",
        "EER_EPD2F_PF4_Y35NY_DPG": "No 2 Fuel Oil (NY)",
        "EER_EPJK_PF4_RGC_DPG": "Kerosene-Type Jet Fuel (Gulf Coast)",
        "EER_EPLLPA_PF4_Y44MB_DPG": "Propane (Midwest)",
        "EER_EPMRR_PF4_Y05LA_DPG": "Reformulated Regular Gasoline (LA)",
        "EER_EPMRU_PF4_RGC_DPG": "Conventional Regular Gasoline (Gulf Coast)",
        "EER_EPMRU_PF4_Y35NY_DPG": "Conventional Regular Gasoline (NY)",
        "RBRTE": "Brent Crude Oil",
        "RWTC": "WTI Crude Oil",
    }


def download_eia_data(
    route: str,
    data_field: str,
    facets: dict | None = None,
    frequency: str = "daily",
) -> str | None:
    """
    Download data from EIA API in JSON format.

    Args:
        route: EIA API route (e.g., 'petroleum/pri/spt')
        data_field: Data column to retrieve (e.g., 'value')
        facets: Dictionary of facet filters (e.g., {'product': ['EPCWTI', 'EPCBRENT']})
        frequency: Data frequency - 'daily', 'monthly', 'yearly'

    Returns:
        JSON response as string or None if request fails
    """
    try:
        url = f"{BASE_URL}/{route}/data?api_key={EIA_API_KEY}&data[]={data_field}"

        # Add frequency if specified
        if frequency:
            url += f"&frequency={frequency}"

        # Add facets if provided
        if facets:
            for facet_name, facet_values in facets.items():
                for value in facet_values:
                    url += f"&facets[{facet_name}][]={value}"

        # Sort by period descending to get latest data first
        url += "&sort[0][column]=period&sort[0][direction]=desc"

        product_display = ""
        if facets and "product" in facets:
            product_ids = facets["product"]
            product_display = (
                f" ({len(product_ids)} products)"
                if len(product_ids) > 1
                else f" ({product_ids[0]})"
            )

        print(f"Downloading energy prices from EIA API{product_display}...")
        response = requests.get(url, timeout=60)
        response.raise_for_status()

        print(f"✓ Data downloaded successfully (Status: {response.status_code})")

        data = response.json()

        if "response" in data and "data" in data["response"]:
            total = data["response"].get("total", 0)
            print(f"✓ Retrieved {total} data points")

        return json.dumps(data)
    except requests.RequestException as e:
        print(f"✗ Error downloading data: {e}")
        return None


def parse_eia_json_data(
    json_content: str, product_names: dict | None = None, series_names: dict | None = None
) -> pl.DataFrame | None:
    """
    Parse EIA JSON response and extract price data with product and series information.

    Expected structure:
    {
      "response": {
        "data": [
          {"period": "2024-04-15", "value": "85.50", "product": "EPCWTI", "series": "RWTC", ...},
          ...
        ]
      }
    }
    """
    try:
        response = json.loads(json_content)

        if "response" not in response or "data" not in response["response"]:
            print("✗ Unexpected EIA response structure")
            return None

        data_list = response["response"]["data"]

        if not data_list:
            print("✗ No data points in response")
            return None

        # Extract relevant fields including product and series info
        records = []
        for item in data_list:
            product_id = item.get("product", "Unknown")
            series_id = item.get("series", "Unknown")
            product_name = (
                product_names.get(product_id, product_id) if product_names else product_id
            )
            series_name = series_names.get(series_id, series_id) if series_names else series_id

            record = {
                "Date": item.get("period"),
                "Price": item.get("value"),
                "Product": product_name,
                "ProductID": product_id,
                "Series": series_name,
                "SeriesID": series_id,
            }
            records.append(record)

        # Create DataFrame with Polars
        df_pd = pl.DataFrame(records)

        # Normalize data types
        df = (
            df_pd.with_columns(
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
            .drop_nulls()
            .select(
                [
                    "Date",
                    "Product",
                    "ProductID",
                    "Series",
                    "SeriesID",
                    "Price",
                    "Year",
                    "Month",
                    "Day",
                ]
            )
        )

        print(f"✓ JSON parsed successfully - {len(df)} rows")
        return df
    except Exception as e:
        print(f"✗ Error parsing JSON: {e}")
        return None


def append_to_csv(csv_file_path: str, new_df: pl.DataFrame) -> pl.DataFrame:
    """
    Append new data to existing CSV file, keeping only new records.
    Deduplicates by Date and Product combination.

    Args:
        csv_file_path: Path to CSV file
        new_df: New dataframe to append

    Returns:
        Combined dataframe
    """
    csv_path = Path(csv_file_path)
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    if csv_path.exists() and csv_path.stat().st_size > 0:
        try:
            existing_df = pl.read_csv(csv_path)
            existing_df = existing_df.with_columns(pl.col("Date").str.to_date())
            print(f"✓ Existing file loaded - {len(existing_df)} rows")

            # Create composite keys for deduplication (Date + Series for granular tracking)
            existing_keys = set(
                zip(existing_df["Date"].to_list(), existing_df["Series"].to_list(), strict=False)
            )
            new_keys = set(zip(new_df["Date"].to_list(), new_df["Series"].to_list(), strict=False))

            missing_keys = new_keys - existing_keys

            if missing_keys:
                missing_dates_series = [(d, s) for d, s in missing_keys]
                new_records = new_df.filter(
                    pl.struct(["Date", "Series"]).is_in([pl.lit(k) for k in missing_dates_series])
                )
                print(f"✓ Found {len(new_records)} new records to add")

                combined_df = pl.concat([existing_df, new_records])
                combined_df = combined_df.sort(["Series", "Date"], descending=[False, True])
            else:
                print("✓ No new records to add - data is up to date")
                combined_df = existing_df

            print(f"✓ Combined data - {len(combined_df)} total rows")
        except Exception as e:
            print(f"✗ Error reading existing file: {e}")
            print("  → Creating new dataset with downloaded data")
            combined_df = new_df.sort(["Series", "Date"], descending=[False, True])
    else:
        print("✓ Creating new dataset")
        combined_df = new_df.sort(["Series", "Date"], descending=[False, True])

    try:
        combined_df_str = combined_df.with_columns(pl.col("Date").cast(pl.Utf8))
        combined_df_str.write_csv(csv_path)
        print(f"✓ Data tracker saved to {csv_path}")
    except Exception as e:
        print(f"✗ Error saving tracker file: {e}")

    return combined_df


def export_data(df: pl.DataFrame, output_dir: str) -> bool:
    """
    Export data to CSV, JSON, and Parquet formats.

    Args:
        df: DataFrame to export
        output_dir: Output directory

    Returns:
        True if successful, False otherwise
    """
    output_path = Path(output_dir)

    csv_dir = output_path / "csv"
    json_dir = output_path / "json"
    parquet_dir = output_path / "parquet"

    csv_dir.mkdir(parents=True, exist_ok=True)
    json_dir.mkdir(parents=True, exist_ok=True)
    parquet_dir.mkdir(parents=True, exist_ok=True)

    csv_file = csv_dir / "eia_energy_prices.csv"
    json_file = json_dir / "eia_energy_prices.json"
    parquet_file = parquet_dir / "eia_energy_prices.parquet"

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

        # Count unique products and series
        unique_series = df["Series"].unique().len()

        print("\n✓ Folder Structure:")
        print("  └── Gasoline/")
        print("      ├── csv/")
        print(f"      │   └── eia_energy_prices.csv ({len(df)} rows, {unique_series} series)")
        print("      ├── json/")
        print(f"      │   └── eia_energy_prices.json ({len(df)} rows, {unique_series} series)")
        print("      └── parquet/")
        print(f"          └── eia_energy_prices.parquet ({len(df)} rows, {unique_series} series)")

        return True
    except Exception as e:
        print(f"✗ Error exporting data: {e}")
        return False


def display_sample_data(df: pl.DataFrame, n: int = 10) -> None:
    """Display sample data from the DataFrame."""
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
    unique_products = df["Product"].unique().len()
    unique_series = df["Series"].unique().len()
    print(f"Unique Products: {unique_products}")
    print(f"Unique Series/Locations: {unique_series}")
    print(f"Series: {', '.join(df['Series'].unique().to_list()[:5])}")
    if unique_series > 5:
        print(f"  ... and {unique_series - 5} more")
    print(f"Date Range: {df['Date'].min()} to {df['Date'].max()}")
    print(f"Price Range: ${df['Price'].min():.2f} - ${df['Price'].max():.2f}")
    print(f"Data Types:\n{df.schema}")
    print("=" * 80 + "\n")


def main():
    """Main function to orchestrate the download and append process."""
    print("=" * 80)
    print("EIA Energy Prices Downloader & Processor")
    print("=" * 80)
    print(f"Timestamp: {datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')}")
    print("Data Source: EIA Open Data API (https://www.eia.gov/opendata/)")
    print()

    # EIA API route for spot prices
    route = "petroleum/pri/spt"
    data_field = "value"

    # Step 1: Get product and series mappings
    print("Step 1: Setting up API parameters...")
    available_products = get_available_products()
    series_mapping = get_regional_series_mapping()
    print(f"✓ Products: {len(available_products)}")
    print(f"✓ Regional Series: {len(series_mapping)}")

    # Step 2: Download energy prices with regional breakdown
    print("\nStep 2: Downloading all energy prices from EIA API...")
    # Use both product facets and series facets for comprehensive regional data
    facets = {
        "product": list(available_products.keys()),
        "series": REGIONAL_SERIES,
    }

    json_content = download_eia_data(
        route=route,
        data_field=data_field,
        facets=facets,
        frequency="daily",
    )

    if not json_content:
        print("Exiting due to download failure")
        return

    # Step 3: Parse data
    print("\nStep 3: Parsing data...")
    df = parse_eia_json_data(
        json_content, product_names=available_products, series_names=series_mapping
    )
    if df is None or df.is_empty():
        print("Exiting due to parsing failure")
        return

    # Step 4: Display sample data
    print("\nStep 4: Displaying sample data...")
    display_sample_data(df)

    # Step 5: Append to existing data
    print("\nStep 5: Appending to existing data...")
    script_dir = Path(__file__).parent
    csv_file = script_dir / "eia_energy_prices.csv"

    combined_df = append_to_csv(str(csv_file), df)

    # Step 6: Export data
    print("\nStep 6: Exporting data...")
    print("=" * 80)
    print("Exporting Data")
    print("=" * 80)
    export_data(combined_df, str(script_dir))

    print()
    print("=" * 80)
    print("Process completed successfully!")
    print("=" * 80)


if __name__ == "__main__":
    main()
