# Codex Task Sheet — Sprint 1

## Project Goal

Build the first working Herd Intelligence pipeline inside the Livestock Intelligence Platform.

The goal of Sprint 1 is a simple vertical pipeline:

upload → inspect → clean → summarise

No advanced modelling or prediction is required in this stage.

---

# Task 1 — File ingestion

Create a module that accepts uploaded datasets.

Supported formats:
- CSV
- XLSX

The ingestion module should:

- detect file type
- read column names
- count rows
- report file metadata

Location in repo:

data_pipeline/

---

# Task 2 — Column inspection

Create logic that lists all detected columns and identifies likely matches for standard fields.

Examples:

Cow ID → animal_id  
Tag → animal_id  
Time → timestamp  
Activity → activity  
Rumination → rumination

Where uncertainty exists, the system should flag it rather than guess.

---

# Task 3 — Data cleaning

Create cleaning functions for:

- timestamp parsing
- duplicate removal
- numeric conversion
- empty row removal
- missing value summary

The output should be a cleaned dataset following the schema defined in:

docs/data_dictionary.md

---

# Task 4 — Validation report

After ingestion and cleaning, produce a validation report containing:

- source file name
- detected columns
- mapped columns
- unmapped columns
- row count
- missing data summary
- timestamp issues
- warnings

---

# Task 5 — Herd summary metrics

Create basic herd metrics:

- number of animals
- records per animal
- mean activity per animal
- mean rumination per animal
- herd averages

No disease detection or diagnosis should be implemented at this stage.

---

# Development Principles

The system should follow these rules:

- keep modules small and modular
- avoid hardcoding dataset-specific assumptions
- surface uncertainty instead of guessing
- prioritise transparency over automation

---

# Deliverable

A pipeline that can:

1. accept a sensor dataset
2. inspect and validate it
3. clean the dataset
4. generate herd summary metrics
