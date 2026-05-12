from datetime import UTC, datetime
from pathlib import Path

import polars as pl


def load_peak_demand(data_dir: Path) -> pl.DataFrame:
    path = data_dir / "peak_demand.csv"
    df = pl.read_csv(path)
    df = df.unpivot(
        index="Country",
        on=["2024", "2030", "2040"],
        variable_name="Year",
        value_name="Peak Demand (MW)",
    ).with_columns(pl.col("Year").cast(pl.Int32))
    print(f"  ✓ peak_demand: {len(df)} rows")
    return df.with_columns(pl.lit("peak_demand").alias("Category"))


def load_interconnectors(data_dir: Path) -> pl.DataFrame:
    interconn_dir = data_dir / "Interconnectors"
    parts = []
    schema = {"NTC_F": pl.Float64, "NTC_B": pl.Float64}
    for scenario, file_name in [
        ("Reference", "REF_NTC.csv"),
        ("Projects", "PROJ_NTC.csv"),
        ("Needs", "NEEDS_NTC.csv"),
    ]:
        path = interconn_dir / file_name
        df = pl.read_csv(path, schema_overrides=schema).with_columns(
            pl.lit(scenario).alias("Scenario")
        )
        parts.append(df)
    df = pl.concat(parts)
    print(f"  ✓ interconnectors: {len(df)} rows")
    return df.with_columns(pl.lit("interconnector").alias("Category"))


def load_import_potential(data_dir: Path) -> pl.DataFrame:
    imp_dir = data_dir / "Import potential"
    parts = []
    for year in [2030, 2040]:
        path = imp_dir / f"imp_pot_chart_{year}.csv"
        df = pl.read_csv(path)
        df = df.unpivot(
            index="Country",
            on=["2024", "Reference", "Projects", "Needs"],
            variable_name="Scenario",
            value_name="Import Potential (%)",
        ).with_columns(pl.lit(year).alias("Target Year"))
        parts.append(df)
    df = pl.concat(parts)
    print(f"  ✓ import_potential: {len(df)} rows")
    return df.with_columns(pl.lit("import_potential").alias("Category"))


def _normalize_cols(df: pl.DataFrame) -> pl.DataFrame:
    rename = {}
    for c in df.columns:
        if "RES-E" in c:
            rename[c] = c.replace(" ", "_")
        if "NET-P" in c:
            rename[c] = c.replace(" ", "_")
    if rename:
        df = df.rename(rename)
    return df


def _load_indicator_file(
    path: Path, agg_type: str, year: int, group_cols: list[str]
) -> pl.DataFrame:
    """Load a single country/flow indicator CSV, normalizing schemas for 2024-vs-2030/2040 files."""
    df = pl.read_csv(path)
    df = _normalize_cols(df)

    has_ref_cols = "RES-E_2024" in df.columns and "NET-P_2024" in df.columns

    time_col = next(c for c in df.columns if c.lower() in ("hour", "month"))
    id_vars = [*group_cols, time_col]

    if has_ref_cols:
        ref = df.select(
            *id_vars,
            pl.col("RES-E_2024").alias("RES-E"),
            pl.col("NET-P_2024").alias("NET-P"),
        ).with_columns(pl.lit("2024_reference").alias("Scenario"))

        proj = df.select(
            *id_vars,
            pl.col("RES-E"),
            pl.col("NET-P"),
        ).with_columns(pl.lit(str(year)).alias("Scenario"))

        df = pl.concat([ref, proj])
    else:
        df = df.with_columns(
            pl.lit(str(year)).alias("Scenario"),
            pl.col("RES-E"),
            pl.col("NET-P"),
        )

    return df.rename({time_col: "Time_Period"}).with_columns(
        pl.col("Time_Period").cast(pl.Utf8),
        pl.lit(agg_type).alias("Aggregation"),
        pl.lit(year).alias("Year"),
    )


def load_country_indicators(data_dir: Path) -> pl.DataFrame:
    ci_dir = data_dir / "Country indicators"
    parts = []
    for year in [2024, 2030, 2040]:
        for agg_type, file_name in [
            ("hourly", f"country_hourly_chart_{year}.csv"),
            ("monthly", f"country_monthly_chart_{year}.csv"),
        ]:
            path = ci_dir / file_name
            if not path.exists():
                continue
            df = _load_indicator_file(path, agg_type, year, group_cols=["Country"])
            parts.append(df)
    df = pl.concat(parts)
    print(f"  ✓ country_indicators: {len(df)} rows")
    return df.with_columns(pl.lit("country_indicator").alias("Category"))


def load_flow_indicators(data_dir: Path) -> pl.DataFrame:
    fi_dir = data_dir / "Flow indicators"
    parts = []
    for year in [2024, 2030, 2040]:
        for agg_type, file_name in [
            ("hourly", f"flows_hourly_chart_{year}.csv"),
            ("monthly", f"flows_monthly_chart_{year}.csv"),
        ]:
            path = fi_dir / file_name
            if not path.exists():
                continue
            df = pl.read_csv(path)
            df = _normalize_cols(df)
            is_monthly = agg_type == "monthly"
            df = (
                df.rename({"Country From": "Country_From", "Country To": "Country_To"})
                if not is_monthly
                else (
                    df.rename({"Country From": "Country_From", "Country To": "Country_To"})
                    if "Country From" in df.columns
                    else df
                )
            )
            time_col = next(c for c in df.columns if c.lower() in ("hour", "month"))
            df = df.with_columns(
                pl.lit(agg_type).alias("Aggregation"),
                pl.lit(year).alias("Year"),
                pl.col(time_col).cast(pl.Utf8).alias("Time_Period"),
            ).drop(time_col)
            parts.append(df)
    df = pl.concat(parts)
    print(f"  ✓ flow_indicators: {len(df)} rows")
    return df.with_columns(pl.lit("flow_indicator").alias("Category"))


def load_all_interconnection_data(data_dir: Path) -> dict[str, pl.DataFrame]:
    print("Loading interconnection data...")
    return {
        "peak_demand": load_peak_demand(data_dir),
        "interconnectors": load_interconnectors(data_dir),
        "import_potential": load_import_potential(data_dir),
        "country_indicators": load_country_indicators(data_dir),
        "flow_indicators": load_flow_indicators(data_dir),
    }


def export_datasets(datasets: dict[str, pl.DataFrame], output_dir: Path):
    csv_dir = output_dir / "csv"
    json_dir = output_dir / "json"
    parquet_dir = output_dir / "parquet"

    for d in [csv_dir, json_dir, parquet_dir]:
        d.mkdir(parents=True, exist_ok=True)

    all_ok = True
    for name, df in datasets.items():
        try:
            csv_path = csv_dir / f"{name}.csv"
            json_path = json_dir / f"{name}.json"
            parquet_path = parquet_dir / f"{name}.parquet"

            df.write_csv(csv_path)
            print(f"  ✓ {name}.csv")

            df.write_json(json_path)
            with open(json_path, "a") as f:
                f.write("\n")
            print(f"  ✓ {name}.json")

            df.write_parquet(parquet_path)
            print(f"  ✓ {name}.parquet")
        except Exception as e:
            print(f"  ✗ {name}: {e}")
            all_ok = False

    combined_dir = output_dir / "combined"
    combined_dir.mkdir(exist_ok=True)
    try:
        combined_dir_csv = combined_dir / "csv"
        combined_dir_json = combined_dir / "json"
        combined_dir_parquet = combined_dir / "parquet"
        for d in [combined_dir_csv, combined_dir_json, combined_dir_parquet]:
            d.mkdir(parents=True, exist_ok=True)

        for name, df in datasets.items():
            df.write_csv(combined_dir_csv / f"{name}.csv")

        combined_json = combined_dir_json / "all_interconnection_data.json"
        combined_parquet = combined_dir_parquet / "all_interconnection_data.parquet"
        all_data = pl.concat(list(datasets.values()), how="diagonal_relaxed")
        print(f"  ✓ Combined {len(all_data):,} rows across all categories")

        all_data.write_json(combined_json)
        with open(combined_json, "a") as f:
            f.write("\n")
        all_data.write_parquet(combined_parquet)

        csv_combined = combined_dir_csv / "all_interconnection_data.csv"
        all_data.write_csv(csv_combined)
        for f in [combined_json, combined_parquet, csv_combined]:
            size_mb = f.stat().st_size / (1024 * 1024)
            print(f"  ✓ {f.name} ({size_mb:.2f} MB)")
    except Exception as e:
        print(f"  ✗ Combined export: {e}")
        all_ok = False

    return all_ok


def display_summary(datasets: dict[str, pl.DataFrame]):
    print(f"\n{'=' * 80}")
    print("European Electricity Interconnection Data — Summary")
    print(f"{'=' * 80}")
    total = 0
    for name, df in datasets.items():
        print(f"  {name}: {len(df):,} rows, {len(df.columns)} cols")
        total += len(df)
    print(f"  Total: {total:,} rows")
    print(f"  Datasets: {list(datasets.keys())}")


def main():
    print(f"{'=' * 80}")
    print("European Electricity Interconnection Data — Processor")
    print(f"{'=' * 80}")
    print(f"Timestamp: {datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')}\n")

    script_dir = Path(__file__).parent
    data_dir = script_dir / "europe_interconnection_data"

    datasets = load_all_interconnection_data(data_dir)
    display_summary(datasets)

    print(f"\n{'=' * 80}")
    print("Exporting Data")
    print(f"{'=' * 80}")
    export_datasets(datasets, script_dir)

    print(f"\n{'=' * 80}")
    print("Done!")
    print(f"{'=' * 80}")


if __name__ == "__main__":
    main()
