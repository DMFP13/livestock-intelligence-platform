import pandas as pd


def load_processed_data(file_path):
    return pd.read_csv(file_path)


def build_herd_summary(df):
    summary = {
        "row_count": len(df),
        "unique_animals": df["animal_id"].nunique(),
        "unique_dates": df["date"].nunique(),
        "avg_rumination_min": round(df["rumination_min"].mean(), 2),
        "avg_activity_rate": round(df["activity_rate"].mean(), 2),
        "avg_standing_min": round(df["standing_min"].mean(), 2),
        "avg_eating_min": round(df["eating_min"].mean(), 2),
        "avg_resting_min": round(df["resting_min"].mean(), 2),
        "avg_data_collection_rate_pct": round(df["data_collection_rate_pct"].mean(), 2),
    }
    return summary


if __name__ == "__main__":
    input_path = "sample_data/processed_danone_sensor_dataset_2.csv"

    try:
        df = load_processed_data(input_path)
        summary = build_herd_summary(df)

        print("\nHERD SUMMARY")
        print("-" * 50)
        for key, value in summary.items():
            print(f"{key}: {value}")

    except Exception as e:
        print(f"Error: {e}")
