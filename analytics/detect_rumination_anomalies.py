import argparse
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.anomaly_detection import detect_rumination_anomalies
from services.data_loader import load_processed_data


INPUT_PATH = "sample_data/processed_danone_sensor_dataset_2.csv"
OUTPUT_DIR = Path("outputs/anomalies")


def save_anomalies(cow_df, animal_id: str, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{animal_id}_rumination_anomalies.csv"
    cow_df.to_csv(output_path, index=False)
    return output_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Detect rumination anomalies for a single cow")
    parser.add_argument("animal_id", help="Cow ID to evaluate (e.g. COW-10000245)")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    animal_id = args.animal_id.strip()

    try:
        if not animal_id:
            raise ValueError("animal_id cannot be empty")

        dataframe = load_processed_data(INPUT_PATH)
        flagged_df, summary = detect_rumination_anomalies(dataframe, animal_id)

        if flagged_df is None or summary is None:
            raise ValueError(f"No records found for animal_id: {animal_id}")

        output_path = save_anomalies(flagged_df, animal_id, OUTPUT_DIR)

        print("\nRUMINATION ANOMALY SUMMARY")
        print("-" * 50)
        print(f"animal_id: {summary['animal_id']}")
        print(f"row_count: {summary['row_count']}")
        print(f"mean_rumination_min: {summary['mean_rumination_min']}")
        print(f"anomaly_count: {summary['anomaly_count']}")
        print(f"saved_to: {output_path}")

    except Exception as e:
        print(f"Error: {e}")
