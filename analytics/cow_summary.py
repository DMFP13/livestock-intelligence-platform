import argparse
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.cow_analysis import build_cow_summary
from services.data_loader import load_processed_data


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Show summary metrics for a single cow")
    parser.add_argument("animal_id", nargs="?", default="COW-10000245", help="Animal ID (e.g. COW-10000245)")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    input_path = "sample_data/processed_danone_sensor_dataset_2.csv"

    try:
        df = load_processed_data(input_path)
        summary = build_cow_summary(df, args.animal_id)

        if summary is None:
            print(f"No records found for animal_id: {args.animal_id}")
        else:
            print("\nCOW SUMMARY")
            print("-" * 50)
            for key, value in summary.items():
                print(f"{key}: {value}")

    except Exception as e:
        print(f"Error: {e}")
