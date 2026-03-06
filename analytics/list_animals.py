from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.cow_analysis import list_cows
from services.data_loader import load_processed_data


if __name__ == "__main__":
    input_path = "sample_data/processed_danone_sensor_dataset_2.csv"

    try:
        df = load_processed_data(input_path)
        animals = list_cows(df)

        print("\nANIMALS IN DATASET")
        print("-" * 50)

        for animal in animals:
            print(animal)

        print("\nTotal animals:", len(animals))

    except Exception as e:
        print(f"Error: {e}")
