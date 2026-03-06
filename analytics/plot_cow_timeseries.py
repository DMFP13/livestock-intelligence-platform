import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


INPUT_PATH = "sample_data/processed_danone_sensor_dataset_2.csv"
OUTPUT_DIR = Path("outputs/plots")
METRICS = ["rumination_min", "activity_rate", "standing_min", "eating_min"]


def load_processed_data(file_path: str) -> pd.DataFrame:
    return pd.read_csv(file_path)


def plot_cow_timeseries(df: pd.DataFrame, animal_id: str, output_dir: Path) -> Path:
    cow_df = df[df["animal_id"] == animal_id].copy()

    if cow_df.empty:
        raise ValueError(f"No records found for animal_id: {animal_id}")

    cow_df["date"] = pd.to_datetime(cow_df["date"])
    cow_df = cow_df.sort_values("date")

    fig, ax = plt.subplots(figsize=(12, 6))

    for metric in METRICS:
        if metric not in cow_df.columns:
            raise ValueError(f"Missing required column: {metric}")
        ax.plot(cow_df["date"], cow_df[metric], marker="o", linewidth=1.5, markersize=3, label=metric)

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
        saved_to = plot_cow_timeseries(dataframe, args.animal_id, OUTPUT_DIR)
        print(f"Saved plot to: {saved_to}")
    except Exception as e:
        print(f"Error: {e}")
