# Data Dictionary

## Purpose

This file defines the standard internal data structure for the Livestock Intelligence Platform.

Raw sensor datasets may arrive with different column names and formats.  
The data pipeline will map those raw columns into the standard fields defined here.

This dictionary is the reference point for ingestion, cleaning, analytics, and dashboard display.

---

## Standard Fields

### animal_id
**Type:** string  
**Description:** Unique identifier for the animal.  
**Examples:** Cow_01, 1234, Tag567

### timestamp
**Type:** datetime  
**Description:** Exact date and time of the observation or sensor record.  
**Examples:** 2025-06-01 14:30:00

### date
**Type:** date  
**Description:** Calendar date extracted from the timestamp.  
**Examples:** 2025-06-01

### source_file
**Type:** string  
**Description:** Name of the original uploaded file.

### activity
**Type:** numeric  
**Description:** Activity or movement value recorded by the sensor.  
**Examples:** activity count, movement score, activity index

### rumination
**Type:** numeric  
**Description:** Rumination value recorded by the sensor.  
**Examples:** rumination minutes, rumination score

### standing
**Type:** numeric  
**Description:** Standing duration or standing-related metric, if available.

### lying
**Type:** numeric  
**Description:** Lying duration or lying-related metric, if available.

### temperature
**Type:** numeric  
**Description:** Ambient temperature, if available.  
**Unit:** degrees Celsius unless otherwise stated

### humidity
**Type:** numeric  
**Description:** Relative humidity, if available.  
**Unit:** percent

### thi
**Type:** numeric  
**Description:** Temperature-Humidity Index, if present in source data or calculated later.

### battery
**Type:** numeric  
**Description:** Device battery value, if available.  
**Examples:** battery percent, battery voltage

### signal_strength
**Type:** numeric  
**Description:** Signal strength or connectivity quality, if available.

### notes
**Type:** string  
**Description:** Free text notes, warnings, or data quality comments.

---

## Notes on Missing Fields

Not every uploaded dataset will contain every standard field.

Rules:
1. Missing fields should be left empty or null.
2. Fields must not be invented if absent in the source data.
3. Ambiguous raw columns should be flagged for review.
4. Units should be documented if they differ from expected defaults.

---

## Example Standard Schema

| field            | type      | required |
|------------------|-----------|----------|
| animal_id        | string    | yes      |
| timestamp        | datetime  | yes      |
| date             | date      | no       |
| source_file      | string    | no       |
| activity         | numeric   | no       |
| rumination       | numeric   | no       |
| standing         | numeric   | no       |
| lying            | numeric   | no       |
| temperature      | numeric   | no       |
| humidity         | numeric   | no       |
| thi              | numeric   | no       |
| battery          | numeric   | no       |
| signal_strength  | numeric   | no       |
| notes            | string    | no       |

---

## Purpose in the pipeline

This schema will be used by:

- ingestion logic to map raw columns
- cleaning logic to standardise data
- analytics modules to compute herd metrics
- the dashboard to display summaries consistently
