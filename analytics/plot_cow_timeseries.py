import argparse
from pathlib import Path
import sys

import matplotlib.pyplot as plt

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.cow_analysis import build_cow_timeseries
from services.data_loader import load_processed_data


INPUT_PATH = "sample_data/processed_danone_sensor_dataset_2.csv"
OUTPUT_DIR = Path("outputs/plots")
METRICS = ["rumination_min", "activity_rate", "standing_min", "eating_min"]


def plot_cow_timeseries(animal_df, animal_id: str, output_dir: Path) -> Path:
    if animal_df.empty:
        raise ValueError(f"No records found for animal_id: {animal_id}")

    fig, ax = plt.subplots(figsize=(12, 6))

    for metric in METRICS:
        if metric in animal_df.columns:
            ax.plot(animal_df["date"], animal_df[metric], marker="o", linewidth=1.5, markersize=3, label=metric)

    ax.set_title(f"Cow Timeseries: {animal_id}")
    ax.set_xlabel("Date")
    ax.set_ylabel("Value")
    ax.legend()
    ax.grid(True, alpha=0.3)

    fig.autofmt_xdate()

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{animal_id}_timeseries.png"
    fig.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close(fig)

    return output_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plot time-series metrics for a single cow")
    parser.add_argument("animal_id", help="Cow ID to plot (e.g. COW-10000245)")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    try:
        dataframe = load_processed_data(INPUT_PATH)
        cow_df = build_cow_timeseries(dataframe, args.animal_id, METRICS)
        saved_to = plot_cow_timeseries(cow_df, args.animal_id, OUTPUT_DIR)
        print(f"Saved plot to: {saved_to}")
    except Exception as e:
        print(f"Error: {e}")
