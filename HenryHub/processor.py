import calendar
from datetime import date
from pathlib import Path

import polars as pl


# --- Input folder ---
input_folder = Path("HenryHub")

# --- Input files ---
input_files = {
    "daily": input_folder / "Henry_Hub_Natural_Gas_Spot_Daily.csv",
    "weekly": input_folder / "Henry_Hub_Natural_Gas_Spot_Weekly.csv",
    "monthly": input_folder / "Henry_Hub_Natural_Gas_Spot_Monthly.csv",
    "yearly": input_folder / "Henry_Hub_Natural_Gas_Spot_Annual.csv",
}

# --- Output formats ---
output_formats = ["csv", "parquet", "json"]


# --- Helper function to save in multiple formats ---
def save_in_formats(df: pl.DataFrame, base_name: str):
    for fmt in output_formats:
        folder = Path(fmt)
        folder.mkdir(exist_ok=True)
        file_out = folder / f"{base_name}.{fmt}"
        if fmt == "csv":
            df.write_csv(file_out)
        elif fmt == "parquet":
            df.write_parquet(file_out)
        elif fmt == "json":
            df.write_json(file_out)
    print(f"Saved {base_name} in {', '.join(output_formats)}")


# --- Process Daily ---
def process_daily(file_path):
    # Use schema_overrides instead of deprecated dtypes
    df = pl.read_csv(file_path, schema_overrides={"Day": pl.Utf8})

    df = df.rename(
        {
            "Day": "Date",
            "Henry Hub Natural Gas Spot Price Dollars per Million Btu": "Price",
        }
    )

    # Strip extra whitespace
    df = df.with_columns([pl.col("Date").str.strip_chars()])

    # Parse Date as actual pl.Date
    df = df.with_columns([pl.col("Date").str.strptime(pl.Date, format="%m/%d/%Y")])

    # Now safely extract Year, Month, Day
    return df.with_columns(
        [
            pl.col("Date").dt.year().alias("Year"),
            pl.col("Date").dt.month().alias("Month"),
            pl.col("Date").dt.day().alias("Day"),
        ]
    )


# --- Process Weekly ---
def process_weekly(file_path):
    # Force string type for 'Week of'
    df = pl.read_csv(file_path, schema_overrides={"Week of": pl.Utf8})

    # Rename columns correctly
    df = df.rename(
        {
            "Week of": "Week Start",
            "Henry Hub Natural Gas Spot Price Dollars per Million Btu": "Price",
        }
    )

    # Strip and parse into actual Date
    df = df.with_columns(
        [pl.col("Week Start").str.strip_chars().str.strptime(pl.Date, format="%m/%d/%Y")]
    )

    # Add Week End column
    return df.with_columns([(pl.col("Week Start") + pl.duration(days=6)).alias("Week End")])


# --- Process Monthly ---
def process_monthly(file_path):
    # Read CSV
    df = pl.read_csv(file_path, schema_overrides={"Month": pl.Utf8})

    df = df.rename(
        {
            "Month": "Month",
            "Henry Hub Natural Gas Spot Price Dollars per Million Btu": "Price",
        }
    )

    # Strip whitespace
    df = df.with_columns([pl.col("Month").str.strip_chars()])

    # Parse Month Start
    df = df.with_columns(
        [pl.col("Month").str.strptime(pl.Date, format="%b %Y").alias("Month Start")]
    )

    # Compute Month End using calendar.monthrange
    month_end = [
        date(d.year, d.month, calendar.monthrange(d.year, d.month)[1]) for d in df["Month Start"]
    ]
    return df.with_columns([pl.Series("Month End", month_end)])


# --- Process Yearly ---
def process_yearly(file_path):
    # Read CSV and force Year column as string
    df = pl.read_csv(file_path, schema_overrides={"Year": pl.Utf8})

    df = df.rename(
        {
            "Year": "Year",
            "Henry Hub Natural Gas Spot Price Dollars per Million Btu": "Price",
        }
    )

    # Strip whitespace and cast to integer
    df = df.with_columns([pl.col("Year").str.strip_chars().cast(pl.Int32)])

    # Compute Year Start and Year End using Python datetime
    year_start = [date(y, 1, 1) for y in df["Year"]]
    year_end = [date(y, 12, 31) for y in df["Year"]]

    return df.with_columns([pl.Series("Year Start", year_start), pl.Series("Year End", year_end)])


# --- Main Processing ---
for freq, file_path in input_files.items():
    if freq == "daily":
        df_clean = process_daily(file_path)
    elif freq == "weekly":
        df_clean = process_weekly(file_path)
    elif freq == "monthly":
        df_clean = process_monthly(file_path)
    elif freq == "yearly":
        df_clean = process_yearly(file_path)

    save_in_formats(df_clean, freq)
print("Processing complete.")
