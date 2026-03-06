import pandas as pd
from pathlib import Path


def load_dataset(file_path):
    """
    Load a CSV or Excel dataset and return a pandas DataFrame.
    """
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


def inspect_dataset(df, source_name="uploaded_file"):
    """
    Return a simple inspection summary for the dataset.
    """
    summary = {
        "source_file": source_name,
        "row_count": len(df),
        "column_count": len(df.columns),
        "columns": list(df.columns),
        "missing_values": df.isnull().sum().to_dict(),
    }
    return summary


if __name__ == "__main__":
    example_path = "sample_data/example.csv"

    try:
        df = load_dataset(example_path)
        summary = inspect_dataset(df, source_name=example_path)

        print("Dataset inspection summary:")
        for key, value in summary.items():
            print(f"{key}: {value}")

    except Exception as e:
        print(f"Error: {e}")
