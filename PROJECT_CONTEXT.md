Project: Livestock Intelligence Platform

Purpose
This repository develops tools to analyse dairy cow behaviour sensor datasets.
The goal is to build a livestock behaviour intelligence toolkit useful for research
and farm management.

Current capabilities
- Data ingestion and cleaning (data_pipeline/ingest.py)
- Herd behaviour summaries
- Individual cow summaries
- Listing animals in the dataset
- Time-series plotting for individual cows

Dataset
Danone dairy cow sensor dataset.

The processed dataset used by analytics scripts is:
sample_data/processed_danone_sensor_dataset_2.csv

Development workflow
- GitHub repository is the source of truth
- Local machine is used for running and testing code
- Codex CLI is used to implement code changes
- ChatGPT is used for planning, architecture, and reviewing ideas

Next development priorities
1. Behaviour anomaly detection
2. Herd comparison visualisations
3. Simple command-line interface
4. Dashboard layer
