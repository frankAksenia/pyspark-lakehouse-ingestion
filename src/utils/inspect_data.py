import pandas as pd
import pyarrow.dataset as ds
from pathlib import Path


def summarize_invalid_data(df, label: str):
    print(f"\nInvalid data summary for {label}:")

    null_counts = df.isna().sum()
    null_counts = null_counts[null_counts > 0].sort_values(ascending=False)
    if not null_counts.empty:
        print("Missing values by column:")
        print(null_counts)
    else:
        print("No missing values found.")

    empty_string_cols = [c for c in df.select_dtypes(include="object").columns]
    empty_string_counts = {
        col: int((df[col].astype(str).str.strip() == "").sum())
        for col in empty_string_cols
        if (df[col].astype(str).str.strip() == "").any()
    }
    if empty_string_counts:
        print("\nEmpty-string counts by column:")
        for col, count in empty_string_counts.items():
            print(f"  {col}: {count}")
    else:
        print("No empty-string values found in text columns.")

    duplicate_count = int(df.duplicated().sum())
    print(f"\nDuplicate rows: {duplicate_count}")


def inspect_raw_data():
    raw_file = Path("data/raw/yellow_tripdata_2024-01.parquet")

    if raw_file.exists():
        print("Inspecting RAW data...")
        print(f"File: {raw_file}")
        print(f"Size: {raw_file.stat().st_size / (1024*1024):.1f} MB")

        df = pd.read_parquet(raw_file)
        print(f"Shape: {df.shape[0]} rows × {df.shape[1]} columns")
        print(f"Columns: {list(df.columns)}")
        print("\n Sample data:")
        print(df.head())
        print("\n Data types:")
        print(df.dtypes)
        print("\n Basic statistics:")
        print(df.describe())
        summarize_invalid_data(df, "RAW")
    else:
        print("Raw data file not found")


def inspect_bronze_data():
    bronze_path = Path("lakehouse/bronze/trips")

    if bronze_path.exists():
        print("\n Inspecting BRONZE layer...")
        print(f"Path: {bronze_path}")

        # Read all parquet files in the bronze directory
        dataset = ds.dataset(str(bronze_path), format="parquet")
        table = dataset.to_table()
        df = table.to_pandas()

        print(f"Shape: {df.shape[0]} rows × {df.shape[1]} columns")
        print(f"Columns: {list(df.columns)}")
        print("\n Sample data:")
        print(df.head())
        print("\n Data types:")
        print(df.dtypes)
        summarize_invalid_data(df, "BRONZE")

    else:
        print("Bronze data not found")


if __name__ == "__main__":
    inspect_raw_data()
    inspect_bronze_data()
