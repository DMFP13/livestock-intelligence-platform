import pandas as pd


def load_processed_data(file_path):
    return pd.read_csv(file_path)


if __name__ == "__main__":
    input_path = "sample_data/processed_danone_sensor_dataset_2.csv"

    try:
        df = load_processed_data(input_path)

        animals = sorted(df["animal_id"].unique())

        print("\nANIMALS IN DATASET")
        print("-" * 50)

        for a in animals:
            print(a)

        print("\nTotal animals:", len(animals))

    except Exception as e:
        print(f"Error: {e}")
