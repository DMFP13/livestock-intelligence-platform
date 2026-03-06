This repository develops a livestock behaviour analytics platform using dairy cow sensor datasets.

Repository structure

data_pipeline/
    ingest.py
        Loads raw CSV/XLSX datasets, standardizes behavioural columns,
        parses dates, validates structure, and writes a processed dataset.

analytics/
    herd_summary.py
        Herd-level behaviour statistics.

    cow_summary.py
        Individual cow behaviour statistics.

    list_animals.py
        Lists animal IDs in the dataset.

    plot_cow_timeseries.py
        Generates time-series plots for individual cows.

sample_data/
    Danone sensor dataset 2.csv
        Raw dataset.

    processed_danone_sensor_dataset_2.csv
        Cleaned dataset used by analytics scripts.

outputs/
    plots/
        Generated time-series visualisations.

Development rules

- keep ingestion code in data_pipeline/
- keep analytics scripts in analytics/
- keep generated artefacts in outputs/
- avoid modifying unrelated files
- prefer small, testable commits
- maintain biological plausibility in analytics logic
- implement features incrementally
