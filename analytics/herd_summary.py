from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.data_loader import load_processed_data
from services.network_analysis import build_executive_overview, build_network_metric_summary


if __name__ == "__main__":
    input_path = "sample_data/processed_danone_sensor_dataset_2.csv"

    try:
        df = load_processed_data(input_path)
        overview = build_executive_overview(df)
        metric_summary = build_network_metric_summary(df)

        print("\nHERD SUMMARY")
        print("-" * 50)
        for key, value in overview.items():
            print(f"{key}: {value}")

        print("\nNETWORK METRICS")
        print(metric_summary.to_string(index=False))

    except Exception as e:
        print(f"Error: {e}")
