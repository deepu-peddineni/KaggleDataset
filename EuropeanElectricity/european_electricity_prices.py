from datetime import UTC, datetime
from pathlib import Path

import polars as pl


def load_price_data(data_dir: Path) -> pl.DataFrame:
    csv_path = data_dir / "all_countries.csv"
    if not csv_path.exists():
        print(f"✗ File not found: {csv_path}")
        return None

    print(f"Loading price data from {csv_path}...")
    df = pl.read_csv(
        csv_path,
        schema_overrides={
            "Datetime (UTC)": pl.Utf8,
            "Datetime (Local)": pl.Utf8,
            "Price (EUR/MWhe)": pl.Float64,
        },
    )
    print(f"✓ Loaded {len(df):,} rows")
    return df


def process_price_data(df: pl.DataFrame) -> pl.DataFrame:
    df = (
        df.with_columns(
            pl.col("Datetime (UTC)").str.to_datetime().alias("Datetime (UTC)"),
            pl.col("Datetime (Local)").str.to_datetime().alias("Datetime (Local)"),
        )
        .with_columns(
            pl.col("Datetime (UTC)").dt.year().cast(pl.Int32).alias("Year"),
            pl.col("Datetime (UTC)").dt.month().cast(pl.Int8).alias("Month"),
            pl.col("Datetime (UTC)").dt.day().cast(pl.Int8).alias("Day"),
            pl.col("Datetime (UTC)").dt.hour().cast(pl.Int8).alias("Hour"),
        )
        .select(
            [
                "Country",
                "ISO3 Code",
                "Datetime (UTC)",
                "Datetime (Local)",
                "Price (EUR/MWhe)",
                "Year",
                "Month",
                "Day",
                "Hour",
            ]
        )
        .sort("Datetime (UTC)", "Country")
    )

    print(f"✓ Processed {len(df):,} rows — {df['Country'].n_unique()} countries")
    return df


def export_data(df: pl.DataFrame, output_dir: Path) -> bool:
    csv_dir = output_dir / "csv"
    json_dir = output_dir / "json"
    parquet_dir = output_dir / "parquet"

    for d in [csv_dir, json_dir, parquet_dir]:
        d.mkdir(parents=True, exist_ok=True)

    csv_file = csv_dir / "european_wholesale_electricity_prices.csv"
    json_file = json_dir / "european_wholesale_electricity_prices.json"
    parquet_file = parquet_dir / "european_wholesale_electricity_prices.parquet"

    try:
        df_csv = df.with_columns(
            pl.col("Datetime (UTC)").cast(pl.Utf8),
            pl.col("Datetime (Local)").cast(pl.Utf8),
        )
        df_csv.write_csv(csv_file)
        print(f"✓ CSV exported: {csv_file}")

        df.write_json(json_file)
        with open(json_file, "a") as f:
            f.write("\n")
        print(f"✓ JSON exported: {json_file}")

        df.write_parquet(parquet_file)
        print(f"✓ Parquet exported: {parquet_file}")

        return True
    except Exception as e:
        print(f"✗ Export error: {e}")
        return False


def display_summary(df: pl.DataFrame):
    print(f"\n{'=' * 80}")
    print("European Wholesale Electricity Prices — Summary")
    print(f"{'=' * 80}")
    print(f"Total Rows: {len(df):,}")
    print(f"Countries: {df['Country'].n_unique()}")
    print(f"Date Range: {df['Datetime (UTC)'].min()} to {df['Datetime (UTC)'].max()}")
    print(
        f"Price Range: {df['Price (EUR/MWhe)'].min():.2f} - {df['Price (EUR/MWhe)'].max():.2f} EUR/MWhe"
    )
    print(f"Columns: {df.columns}")
    print(f"Schema:\n{df.schema}")


def main():
    print(f"{'=' * 80}")
    print("European Wholesale Electricity Prices — Processor")
    print(f"{'=' * 80}")
    print(f"Timestamp: {datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')}\n")

    script_dir = Path(__file__).parent
    data_dir = script_dir / "european_wholesale_electricity_price_data_hourly"

    df = load_price_data(data_dir)
    if df is None:
        return

    df = process_price_data(df)
    display_summary(df)

    print(f"\n{'=' * 80}")
    print("Exporting Data")
    print(f"{'=' * 80}")
    export_data(df, script_dir)

    print(f"\n{'=' * 80}")
    print("Done!")
    print(f"{'=' * 80}")


if __name__ == "__main__":
    main()
