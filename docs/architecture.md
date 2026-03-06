# Livestock Intelligence Platform Architecture

## Purpose

The Livestock Intelligence Platform is a modular system designed to process livestock telemetry data and convert it into useful herd intelligence.

Version 1 focuses on the Herd Intelligence module.

The system will:

1. Accept livestock sensor datasets (CSV or Excel)
2. Inspect and validate the uploaded data
3. Clean and standardise the dataset
4. Calculate herd-level and animal-level behavioural summaries
5. Display results in a dashboard

Future modules may include:

- estrus detection
- disease anomaly detection
- environmental stress monitoring
- dairy market intelligence
- farm economics and scenario modelling

---

## Core Components

The platform is divided into four main layers.

### 1 Data Ingestion

Handles uploaded files.

Responsibilities:
- detect file type (CSV or XLSX)
- inspect column names
- count rows
- extract timestamps
- report missing fields

Location in repo:

data_pipeline/

---

### 2 Data Cleaning

Standardises datasets so different sensor exports can be used together.

Responsibilities:
- timestamp parsing
- column mapping
- duplicate removal
- numeric conversion
- missing data summary

Location in repo:

data_pipeline/

---

### 3 Behaviour Analytics

Computes herd metrics and individual cow metrics.

Examples:

- mean rumination
- mean activity
- behaviour trends over time
- herd averages
- per-animal summaries

Location in repo:

analytics/

---

### 4 Herd Intelligence Interface

Displays processed results.

UI components include:

- dataset summary cards
- herd summary statistics
- animal table
- individual animal profiles
- behaviour charts
- validation warnings

Location in repo:

app/

---

## Design Principles

The system should follow these principles:

1. Modular design
2. Transparent data validation
3. Reproducible transformations
4. Separation of pipeline, analytics, and UI
5. Avoid hidden assumptions

---

## Version 1 Goal

The first version should implement a full vertical pipeline:

upload → inspect → clean → summarise → display

No predictive modelling is required in Version 1.
