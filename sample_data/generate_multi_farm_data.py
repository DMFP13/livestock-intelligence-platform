"""
Generate synthetic milk production and sensor telemetry data
for Nigerian dairy farms (FARM-002 through FARM-005).
FARM-001 already exists and is not regenerated.
"""

import numpy as np
import pandas as pd
from pathlib import Path

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
SEED = 42
rng = np.random.default_rng(SEED)

OUTPUT_DIR = Path("/Users/mac1/livestock-intelligence-platform/sample_data")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Farm definitions
# ---------------------------------------------------------------------------
FARMS = [
    {
        "farm_id": "FARM-001",
        "farm_name": "Jos Plateau Dairy",
        "state": "Plateau",
        "breed_type": "Wadara crossbred",
        "management_system": "Semi-extensive",
        "herd_size": 20,
        "avg_yield": 4.0,
        "lat": 9.8965,
        "lon": 8.8583,
        "cow_ids": None,           # already exists – skip generation
        "date_start": None,
        "date_end": None,
        "yield_min": 3.0,
        "yield_max": 5.0,
        "missing_pct": 0.0,
        "dip_month": None,
        "dip_factor": 1.0,
    },
    {
        "farm_id": "FARM-002",
        "farm_name": "Kaduna Semi-Intensive Farm",
        "state": "Kaduna",
        "breed_type": "Friesian×Bunaji",
        "management_system": "Semi-intensive",
        "herd_size": 30,
        "avg_yield": 9.5,
        "lat": 10.5222,
        "lon": 7.4383,
        "cow_ids": [f"COW-{10000300 + i}" for i in range(30)],
        "date_start": "2025-06-01",
        "date_end": "2025-10-31",
        "yield_min": 7.0,
        "yield_max": 12.0,
        "missing_pct": 0.0,
        "dip_month": 7,            # July dip
        "dip_factor": 0.80,
    },
    {
        "farm_id": "FARM-003",
        "farm_name": "Sokoto Pastoralist Herd",
        "state": "Sokoto",
        "breed_type": "Local Bunaji",
        "management_system": "Pastoralist",
        "herd_size": 45,
        "avg_yield": 3.0,
        "lat": 13.0059,
        "lon": 5.2476,
        "cow_ids": [f"COW-{10000400 + i}" for i in range(45)],
        "date_start": "2025-06-01",
        "date_end": "2025-09-30",
        "yield_min": 2.0,
        "yield_max": 4.0,
        "missing_pct": 0.10,
        "dip_month": None,
        "dip_factor": 1.0,
    },
    {
        "farm_id": "FARM-004",
        "farm_name": "Oyo Commercial Dairy",
        "state": "Oyo",
        "breed_type": "Exotic Friesian",
        "management_system": "Commercial intensive",
        "herd_size": 15,
        "avg_yield": 18.0,
        "lat": 7.8500,
        "lon": 3.9333,
        "cow_ids": [f"COW-{10000500 + i}" for i in range(15)],
        "date_start": "2025-06-01",
        "date_end": "2025-10-31",
        "yield_min": 14.0,
        "yield_max": 22.0,
        "missing_pct": 0.0,
        "dip_month": None,
        "dip_factor": 1.0,
    },
    {
        "farm_id": "FARM-005",
        "farm_name": "Plateau Smallholder Farm",
        "state": "Plateau",
        "breed_type": "Mixed local",
        "management_system": "Smallholder",
        "herd_size": 12,
        "avg_yield": 4.5,
        "lat": 9.2182,
        "lon": 9.5177,
        "cow_ids": [f"COW-{10000600 + i}" for i in range(12)],
        "date_start": "2025-07-01",
        "date_end": "2025-10-31",
        "yield_min": 3.0,
        "yield_max": 6.0,
        "missing_pct": 0.0,
        "dip_month": 8,            # August dip
        "dip_factor": 0.82,
    },
]

# ---------------------------------------------------------------------------
# Helper: random-walk lactation curve for one cow over a date range
# ---------------------------------------------------------------------------
def lactation_random_walk(
    dates,
    yield_min,
    yield_max,
    dip_month=None,
    dip_factor=1.0,
    high_variability=False,
    rng=rng,
):
    n = len(dates)
    mid = (yield_min + yield_max) / 2.0
    step_sd = (yield_max - yield_min) * (0.06 if not high_variability else 0.12)

    # initialise near middle of range
    start = rng.uniform(yield_min + 0.5, yield_max - 0.5)
    walk = np.zeros(n)
    walk[0] = start

    for i in range(1, n):
        step = rng.normal(0, step_sd)
        # gentle mean-reversion toward mid
        reversion = 0.05 * (mid - walk[i - 1])
        walk[i] = walk[i - 1] + step + reversion
        walk[i] = np.clip(walk[i], yield_min, yield_max)

    # apply seasonal dip
    if dip_month is not None:
        months = np.array([d.month for d in dates])
        mask = months == dip_month
        walk[mask] = walk[mask] * dip_factor

    return np.round(walk, 2)


# ---------------------------------------------------------------------------
# Helper: generate sensor row for one cow-day
# ---------------------------------------------------------------------------
def gen_sensor_row(
    record_counter,
    farm,
    cow_id,
    date,
    rng=rng,
):
    fid = farm["farm_id"]
    is_pastoralist = fid == "FARM-003"
    is_commercial = fid == "FARM-004"

    rumination_min = round(rng.uniform(200, 500), 4)
    eating_min = round(rng.uniform(80, 200), 4)

    # sitting + standing ≈ 1400–1450
    total_ss = rng.uniform(1400, 1450)
    sitting_frac = rng.uniform(0.45, 0.60)
    sitting_min = round(total_ss * sitting_frac, 4)
    standing_min = round(total_ss * (1 - sitting_frac), 4)

    coughing_count = int(rng.integers(0, 11))          # 0–10
    resting_min = round(rng.uniform(150, 300), 4)
    activity_rate = round(rng.uniform(40, 80), 6)
    mounting_count = int(rng.integers(0, 9))            # 0–8
    sniffing = round(rng.uniform(50, 200), 6)

    # heat_detection_count: 1 ~2% of records
    heat_detection_count = 1 if rng.random() < 0.02 else 0

    sit_stand_min = round(sitting_min + standing_min, 4)

    # data_collection_rate_pct
    if is_pastoralist:
        dcr = round(rng.uniform(60, 90), 0)
    else:
        dcr = round(rng.uniform(85, 100), 0)

    source_file = f"synthetic_farm_{fid}"

    return {
        "farm_id": fid,
        "farm_name": farm["farm_name"],
        "record_id": record_counter,
        "animal_id": cow_id,
        "date": date.strftime("%Y-%m-%d"),
        "rumination_min": rumination_min,
        "eating_min": eating_min,
        "sitting_min": sitting_min,
        "standing_min": standing_min,
        "coughing_count": coughing_count,
        "resting_min": resting_min,
        "activity_rate": activity_rate,
        "mounting_count": mounting_count,
        "sniffing": sniffing,
        "heat_detection_count": heat_detection_count,
        "sit_stand_min": sit_stand_min,
        "data_collection_rate_pct": dcr,
        "source_file": source_file,
    }


# ---------------------------------------------------------------------------
# Main generation loop
# ---------------------------------------------------------------------------
milk_rows = []
sensor_rows = []
record_counter = 1

summary = []

for farm in FARMS:
    if farm["farm_id"] == "FARM-001":
        summary.append(("FARM-001", "Jos Plateau Dairy", "skipped (pre-existing)", 0, 0))
        continue

    dates = pd.date_range(farm["date_start"], farm["date_end"], freq="D")
    is_pastoralist = farm["farm_id"] == "FARM-003"
    high_variability = is_pastoralist

    milk_count = 0
    sensor_count = 0

    for cow_id in farm["cow_ids"]:
        # Generate full lactation walk for this cow
        yields = lactation_random_walk(
            dates.to_pydatetime(),
            farm["yield_min"],
            farm["yield_max"],
            dip_month=farm["dip_month"],
            dip_factor=farm["dip_factor"],
            high_variability=high_variability,
            rng=rng,
        )

        for i, date in enumerate(dates):
            # Apply missing records for pastoralist
            if is_pastoralist and rng.random() < farm["missing_pct"]:
                continue

            # Milk row
            milk_rows.append(
                {
                    "Farm ID": farm["farm_id"],
                    "Farm Name": farm["farm_name"],
                    "State": farm["state"],
                    "Cow ID": cow_id,
                    "Date": date.strftime("%d/%m/%Y"),
                    "Estimated Milk Production (L/day)": yields[i],
                }
            )
            milk_count += 1

            # Sensor row
            sensor_rows.append(
                gen_sensor_row(record_counter, farm, cow_id, date, rng=rng)
            )
            sensor_count += 1
            record_counter += 1

    summary.append(
        (
            farm["farm_id"],
            farm["farm_name"],
            f"{len(farm['cow_ids'])} cows × {len(dates)} days",
            milk_count,
            sensor_count,
        )
    )

# ---------------------------------------------------------------------------
# Build DataFrames and save
# ---------------------------------------------------------------------------
milk_df = pd.DataFrame(milk_rows)
sensor_df = pd.DataFrame(sensor_rows)

# Ensure column order for sensor matches spec (farm_id, farm_name first)
sensor_cols = [
    "farm_id", "farm_name",
    "record_id", "animal_id", "date",
    "rumination_min", "eating_min", "sitting_min", "standing_min",
    "coughing_count", "resting_min", "activity_rate",
    "mounting_count", "sniffing", "heat_detection_count",
    "sit_stand_min", "data_collection_rate_pct", "source_file",
]
sensor_df = sensor_df[sensor_cols]

milk_path = OUTPUT_DIR / "multi_farm_milk_production.csv"
sensor_path = OUTPUT_DIR / "multi_farm_sensor_telemetry.csv"

milk_df.to_csv(milk_path, index=False)
sensor_df.to_csv(sensor_path, index=False)

# ---------------------------------------------------------------------------
# Farm registry (all 5 farms)
# ---------------------------------------------------------------------------
registry_rows = []
for farm in FARMS:
    registry_rows.append(
        {
            "farm_id": farm["farm_id"],
            "farm_name": farm["farm_name"],
            "state": farm["state"],
            "breed_type": farm["breed_type"],
            "management_system": farm["management_system"],
            "herd_size": farm["herd_size"],
            "avg_yield_l_per_day": farm["avg_yield"],
            "lat": farm["lat"],
            "lon": farm["lon"],
        }
    )

registry_df = pd.DataFrame(registry_rows)
registry_path = OUTPUT_DIR / "farm_registry.csv"
registry_df.to_csv(registry_path, index=False)

# ---------------------------------------------------------------------------
# Print summary
# ---------------------------------------------------------------------------
print("=" * 72)
print("SYNTHETIC DATA GENERATION COMPLETE")
print("=" * 72)
print(f"{'Farm ID':<12} {'Farm Name':<30} {'Coverage':<28} {'Milk':>6} {'Sensor':>7}")
print("-" * 72)
for row in summary:
    fid, name, coverage, mc, sc = row
    print(f"{fid:<12} {name:<30} {coverage:<28} {mc:>6,} {sc:>7,}")

print("-" * 72)
total_milk = sum(r[3] for r in summary)
total_sensor = sum(r[4] for r in summary)
print(f"{'TOTAL':<12} {'':<30} {'':<28} {total_milk:>6,} {total_sensor:>7,}")
print("=" * 72)
print()
print("Output files:")
print(f"  {milk_path}")
print(f"  {sensor_path}")
print(f"  {registry_path}")
print()
print("Farm registry:")
print(registry_df.to_string(index=False))
