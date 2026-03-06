import pandas as pd


def load_processed_data(file_path):
    return pd.read_csv(file_path)


def build_cow_summary(df, animal_id):
    cow_df = df[df["animal_id"] == animal_id].copy()

    if cow_df.empty:
        return None

    summary = {
        "animal_id": animal_id,
        "row_count": len(cow_df),
        "unique_dates": cow_df["date"].nunique(),
        "avg_rumination_min": round(cow_df["rumination_min"].mean(), 2),
        "avg_activity_rate": round(cow_df["activity_rate"].mean(), 2),
        "avg_standing_min": round(cow_df["standing_min"].mean(), 2),
        "avg_eating_min": round(cow_df["eating_min"].mean(), 2),
        "avg_resting_min": round(cow_df["resting_min"].mean(), 2),
        "avg_mounting_count": round(cow_df["mounting_count"].mean(), 2),
        "avg_heat_detection_count": round(cow_df["heat_detection_count"].mean(), 2),
        "avg_data_collection_rate_pct": round(cow_df["data_collection_rate_pct"].mean(), 2),
    }

    return summary


if __name__ == "__main__":
    input_path = "sample_data/processed_danone_sensor_dataset_2.csv"
    animal_id = "COW-10000245"

    try:
        df = load_processed_data(input_path)
        summary = build_cow_summary(df, animal_id)

        if summary is None:
            print(f"No records found for animal_id: {animal_id}")
        else:
            print("\nCOW SUMMARY")
            print("-" * 50)
            for key, value in summary.items():
                print(f"{key}: {value}")

    except Exception as e:
        print(f"Error: {e}")
