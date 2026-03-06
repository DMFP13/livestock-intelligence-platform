import pandas as pd
from pathlib import Path


COLUMN_RENAME_MAP = {
    "ID": "record_id",
    "Cow ID": "animal_id",
    "Date": "date",
    "Ruminating(min)": "rumination_min",
    "Eating(min)": "eating_min",
    "Sitting(min)": "sitting_min",
    "Standing(min)": "standing_min",
    "Coughing(count)": "coughing_count",
    "Resting(min)": "resting_min",
    "Activity Rate": "activity_rate",
    "Mounting(count)": "mounting_count",
    "Sniffing": "sniffing",
    "Heat Detection(count)": "heat_detection_count",
    "SIT+STAND(min)": "sit_stand_min",
    "Data Collection Rate(%)": "data_collection_rate_pct",
}


def load_dataset(file_path):
    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    if file_path.suffix.lower() == ".csv":
        df = pd.read_csv(file_path)
    elif file_path.suffix.lower() in [".xlsx", ".xls"]:
        df = pd.read_excel(file_path)
    else:
        raise ValueError("Unsupported file type. Please use CSV or Excel.")

    return df


def clean_danone_dataset(df, source_name="uploaded_file"):
    df = df.copy()
    df = df.rename(columns=COLUMN_RENAME_MAP)

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

    df["source_file"] = source_name
    df = df.dropna(how="all")
    df = df.drop_duplicates()

    return df


def build_validation_report(raw_df, cleaned_df, source_name):
    expected_columns = list(COLUMN_RENAME_MAP.values()) + ["source_file"]
    present_columns = list(cleaned_df.columns)

    missing_expected_columns = [
        col for col in expected_columns if col not in present_columns
    ]

    report = {
        "source_file": source_name,
        "raw_row_count": len(raw_df),
        "cleaned_row_count": len(cleaned_df),
        "raw_columns": list(raw_df.columns),
        "cleaned_columns": present_columns,
        "missing_expected_columns": missing_expected_columns,
        "missing_values_after_cleaning": cleaned_df.isnull().sum().to_dict(),
        "unique_animals": cleaned_df["animal_id"].nunique() if "animal_id" in cleaned_df.columns else None,
        "unique_dates": cleaned_df["date"].nunique() if "date" in cleaned_df.columns else None,
    }

    return report


if __name__ == "__main__":
    input_path = "sample_data/Danone sensor dataset 2.csv"
    output_path = "sample_data/processed_danone_sensor_dataset_2.csv"

    try:
        raw_df = load_dataset(input_path)
        cleaned_df = clean_danone_dataset(raw_df, source_name=input_path)
        report = build_validation_report(raw_df, cleaned_df, source_name=input_path)

        cleaned_df.to_csv(output_path, index=False)

        print("\nVALIDATION REPORT")
        print("-" * 50)
        for key, value in report.items():
            print(f"{key}: {value}")

        print("\nProcessed file saved to:")
        print(output_path)

    except Exception as e:
        print(f"Error: {e}")
