from __future__ import annotations
import hashlib
import hmac
import json
import os
import threading
import time
from pathlib import Path
import shutil
import tempfile
from fastapi import FastAPI, HTTPException, Form, UploadFile, File
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
import pandas as pd

# ---------------------------------------------------------------------------
# Auth config — set DASHBOARD_USER / DASHBOARD_PASS env vars in production
# ---------------------------------------------------------------------------

_AUTH_USER   = os.environ.get("DASHBOARD_USER", "rlih")
_AUTH_PASS   = os.environ.get("DASHBOARD_PASS", "nigeria2025")
_SECRET      = os.environ.get("DASHBOARD_SECRET", "change-me-in-production").encode()
_TOKEN_TTL   = 8 * 3600  # 8 hours


def _make_token(username: str) -> str:
    expires = int(time.time()) + _TOKEN_TTL
    payload = f"{username}:{expires}"
    sig = hmac.new(_SECRET, payload.encode(), hashlib.sha256).hexdigest()
    return f"{payload}:{sig}"


def _verify_token(token: str) -> bool:
    try:
        parts = token.split(":")
        if len(parts) != 3:
            return False
        username, expires_str, sig = parts
        expires = int(expires_str)
        if time.time() > expires:
            return False
        payload = f"{username}:{expires_str}"
        expected = hmac.new(_SECRET, payload.encode(), hashlib.sha256).hexdigest()
        return hmac.compare_digest(sig, expected)
    except Exception:
        return False

ROOT          = Path(__file__).resolve().parents[1]
FARMS_DIR     = ROOT / "storage" / "farms"
REGISTRY_FILE = ROOT / "storage" / "farms_registry.json"
MARKET_FILE   = ROOT / "storage" / "market_data.json"
MARKET_REFRESH_HOURS = 6

BEHAVIOUR_COLS = [
    "timestamp", "activity_rate", "rumination_min", "eating_min",
    "resting_min", "standing_min", "sitting_min",
    "coughing_count", "heat_detection_count", "mounting_count",
    "data_collection_rate_pct",
]

app = FastAPI(title="Nigeria Dairy Data Herd API")


# ---------------------------------------------------------------------------
# Market data — background refresh
# ---------------------------------------------------------------------------

def _refresh_market_data():
    try:
        import sys
        if str(ROOT / "scripts") not in sys.path:
            sys.path.insert(0, str(ROOT))
        from scripts.fetch_market_data import fetch_all
        fetch_all()
        print("Market data refreshed.")
    except Exception as exc:
        print(f"Market data refresh failed: {exc}")


def _market_refresh_loop():
    # Refresh on startup if stale (>6h) or missing, then every 6h
    needs_refresh = True
    if MARKET_FILE.exists():
        age_hours = (time.time() - MARKET_FILE.stat().st_mtime) / 3600
        needs_refresh = age_hours > MARKET_REFRESH_HOURS
    if needs_refresh:
        _refresh_market_data()
    while True:
        time.sleep(MARKET_REFRESH_HOURS * 3600)
        _refresh_market_data()


threading.Thread(target=_market_refresh_loop, daemon=True).start()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _farm_dir(farm_id: str) -> Path:
    d = FARMS_DIR / farm_id
    if not d.exists():
        raise HTTPException(status_code=404, detail=f"Farm '{farm_id}' not found")
    return d

def _load_herd(farm_id: str) -> dict:
    return json.loads((_farm_dir(farm_id) / "herd_data.json").read_text())

def _load_telemetry(farm_id: str) -> pd.DataFrame:
    d = _farm_dir(farm_id)
    parquet = d / "telemetry_events.parquet"
    csv     = d / "telemetry_events.csv"
    if parquet.exists():
        return pd.read_parquet(parquet)
    if csv.exists():
        return pd.read_csv(csv)
    raise HTTPException(status_code=404, detail=f"No telemetry data for farm '{farm_id}'")

def _registry() -> dict:
    return json.loads(REGISTRY_FILE.read_text())


# ---------------------------------------------------------------------------
# Multi-farm endpoints
# ---------------------------------------------------------------------------

@app.get("/api/farms")
def list_farms():
    index = FARMS_DIR / "farms_index.json"
    if index.exists():
        return JSONResponse(json.loads(index.read_text()))
    # Fallback: build from registry metadata only
    reg = _registry()
    return JSONResponse({"farms": reg["farms"]})

@app.get("/api/farms/compare")
def compare_farms():
    index = FARMS_DIR / "farms_index.json"
    if not index.exists():
        raise HTTPException(status_code=404, detail="farms_index.json not built yet — run build_telemetry_store.py")
    return JSONResponse(json.loads(index.read_text()))

@app.get("/api/farms/registry")
def get_registry():
    return JSONResponse(_registry())


# ---------------------------------------------------------------------------
# Per-farm endpoints
# ---------------------------------------------------------------------------

@app.get("/api/farm/{farm_id}/full")
def farm_full(farm_id: str):
    return JSONResponse(_load_herd(farm_id))

@app.get("/api/farm/{farm_id}/summary")
def farm_summary(farm_id: str):
    return JSONResponse(_load_herd(farm_id)["summary"])

@app.get("/api/farm/{farm_id}/animal/{animal_id}")
def farm_animal(farm_id: str, animal_id: str):
    df = _load_telemetry(farm_id)
    animal_df = df[df["animal_id"] == animal_id]
    if animal_df.empty:
        raise HTTPException(status_code=404, detail=f"Animal {animal_id} not found in farm {farm_id}")
    cols = [c for c in BEHAVIOUR_COLS if c in animal_df.columns]
    records = (
        animal_df[cols]
        .sort_values("timestamp")
        .assign(timestamp=lambda x: x["timestamp"].astype(str))
        .round(3)
        .to_dict(orient="records")
    )
    return JSONResponse({"farm_id": farm_id, "animal_id": animal_id, "records": records})


# ---------------------------------------------------------------------------
# Milk production endpoints
# ---------------------------------------------------------------------------

@app.get("/api/farm/{farm_id}/milk")
def farm_milk(farm_id: str):
    milk_file = _farm_dir(farm_id) / "milk_production.csv"
    if not milk_file.exists():
        raise HTTPException(status_code=404, detail=f"No milk data for farm '{farm_id}'")
    df = pd.read_csv(milk_file)
    daily = (
        df.groupby("date")["yield_l"]
        .agg(avg_yield="mean", min_yield="min", max_yield="max", cow_count="count")
        .reset_index()
        .round(3)
    )
    return JSONResponse({
        "farm_id": farm_id,
        "daily": daily.to_dict(orient="records"),
        "summary": {
            "avg_yield_l_per_cow_per_day": round(df["yield_l"].mean(), 3),
            "total_cows": int(df["cow_id"].nunique()),
            "date_range": [df["date"].min(), df["date"].max()],
        }
    })

@app.get("/api/milk/all")
def milk_all_farms():
    result = []
    for farm_dir in sorted(FARMS_DIR.iterdir()):
        milk_file = farm_dir / "milk_production.csv"
        if not milk_file.exists():
            continue
        df = pd.read_csv(milk_file)
        result.append({
            "farm_id": farm_dir.name,
            "avg_yield_l_per_cow_per_day": round(df["yield_l"].mean(), 3),
            "total_cows": int(df["cow_id"].nunique()),
            "date_range": [df["date"].min(), df["date"].max()],
        })
    return JSONResponse({"farms": result})


# ---------------------------------------------------------------------------
# Upload endpoints
# ---------------------------------------------------------------------------

def _rebuild_herd(farm_id: str) -> dict:
    """Re-run the full analytics pipeline for one farm and return summary."""
    import sys
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    from data.ingestion.telemetry.reconcile_datasets import reconcile_datasets
    from data.validation.telemetry_validation import validate_telemetry
    from analytics.herd.herd_metrics import herd_summary, daily_averages, records_per_animal, missingness, coverage_by_animal
    from analytics.herd.alerts import add_prototype_alerts

    out_dir = FARMS_DIR / farm_id
    parquet = out_dir / "telemetry_events.parquet"
    if not parquet.exists():
        raise HTTPException(status_code=404, detail="No existing telemetry to rebuild from")

    combined = pd.read_parquet(parquet)
    validation = validate_telemetry(combined, combined)
    enriched   = add_prototype_alerts(combined)

    estrus_df = (
        enriched[enriched["estrus_status"].isin(["HIGH","MEDIUM"])]
        [["animal_id","timestamp","estrus_score","estrus_status"]]
        .assign(timestamp=lambda x: x["timestamp"].dt.strftime("%Y-%m-%d"))
        .sort_values(["estrus_score","timestamp"], ascending=[False,False])
        .head(12)
    )
    health_df = (
        enriched[enriched["health_status"].isin(["ALERT","WATCH"])]
        [["animal_id","timestamp","health_score","health_status"]]
        .assign(timestamp=lambda x: x["timestamp"].dt.strftime("%Y-%m-%d"))
        .sort_values(["health_score","timestamp"], ascending=[False,False])
        .head(12)
    )

    herd = {
        "farm_id":        farm_id,
        "summary":        herd_summary(combined),
        "comparison":     {"merge_strategy": "single_file_rebuild", "overlap_animal_timestamp_pairs": 0,
                           "same_animals": True, "file1_date_range": [], "file2_date_range": []},
        "validation":     validation,
        "daily_averages": daily_averages(combined).round(3).to_dict(orient="records"),
        "records_per_animal": records_per_animal(combined).to_dict(orient="records"),
        "missingness":    missingness(combined).to_dict(orient="records"),
        "coverage":       coverage_by_animal(combined).to_dict(orient="records"),
        "estrus_alerts":  estrus_df.to_dict(orient="records"),
        "health_alerts":  health_df.to_dict(orient="records"),
    }
    (out_dir / "herd_data.json").write_text(json.dumps(herd, default=str))
    return herd["summary"]


@app.post("/api/farm/{farm_id}/upload/telemetry")
async def upload_telemetry(farm_id: str, file: UploadFile = File(...)):
    """Accept a BODIT Excel or CSV export, append to telemetry, rebuild herd analytics."""
    _farm_dir(farm_id)  # 404 if farm doesn't exist
    import sys
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    from data.ingestion.telemetry.danone_loader import load_excel, load_csv
    from data.ingestion.telemetry.reconcile_datasets import reconcile_datasets

    suffix = Path(file.filename).suffix.lower()
    if suffix not in {".xlsx", ".xls", ".csv"}:
        raise HTTPException(status_code=400, detail="Only .xlsx, .xls, or .csv files accepted")

    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
        shutil.copyfileobj(file.file, tmp)
        tmp_path = Path(tmp.name)

    try:
        new_df = load_excel(tmp_path) if suffix in {".xlsx", ".xls"} else load_csv(tmp_path)
    except Exception as e:
        tmp_path.unlink(missing_ok=True)
        raise HTTPException(status_code=422, detail=f"Could not parse file: {e}")
    finally:
        tmp_path.unlink(missing_ok=True)

    out_dir = FARMS_DIR / farm_id
    parquet  = out_dir / "telemetry_events.parquet"

    if parquet.exists():
        existing = pd.read_parquet(parquet)
        combined = pd.concat([existing, new_df], ignore_index=True).drop_duplicates(
            subset=["animal_id", "timestamp"]
        )
    else:
        combined = new_df

    combined.to_parquet(parquet, index=False)
    summary = _rebuild_herd(farm_id)

    return JSONResponse({
        "status": "ok",
        "farm_id": farm_id,
        "filename": file.filename,
        "new_records": len(new_df),
        "total_records": len(combined),
        "summary": summary,
    })


@app.post("/api/farm/{farm_id}/upload/milk")
async def upload_milk(farm_id: str, file: UploadFile = File(...)):
    """Accept a milk production CSV or Excel. Expected columns: cow_id, date, yield_l (or similar)."""
    _farm_dir(farm_id)

    suffix = Path(file.filename).suffix.lower()
    if suffix not in {".xlsx", ".xls", ".csv"}:
        raise HTTPException(status_code=400, detail="Only .xlsx, .xls, or .csv files accepted")

    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
        shutil.copyfileobj(file.file, tmp)
        tmp_path = Path(tmp.name)

    try:
        raw = pd.read_excel(tmp_path) if suffix in {".xlsx", ".xls"} else pd.read_csv(tmp_path)
    except Exception as e:
        tmp_path.unlink(missing_ok=True)
        raise HTTPException(status_code=422, detail=f"Could not parse file: {e}")
    finally:
        tmp_path.unlink(missing_ok=True)

    # Normalise column names flexibly
    raw.columns = [c.strip().lower().replace(" ", "_") for c in raw.columns]
    col_map = {}
    for c in raw.columns:
        if any(k in c for k in ("cow", "animal", "tag", "id")) and "cow_id" not in col_map.values():
            col_map[c] = "cow_id"
        elif any(k in c for k in ("date", "day", "time")) and "date" not in col_map.values():
            col_map[c] = "date"
        elif any(k in c for k in ("yield", "milk", "litre", "liter", "production", "volume")) and "yield_l" not in col_map.values():
            col_map[c] = "yield_l"
    raw = raw.rename(columns=col_map)

    required = {"cow_id", "date", "yield_l"}
    missing  = required - set(raw.columns)
    if missing:
        raise HTTPException(status_code=422, detail=f"Could not identify columns: {missing}. Found: {list(raw.columns)}")

    new_df = raw[["cow_id", "date", "yield_l"]].copy()
    new_df["cow_id"]  = new_df["cow_id"].astype(str).str.strip()
    new_df["date"]    = pd.to_datetime(new_df["date"], dayfirst=True, errors="coerce").dt.strftime("%Y-%m-%d")
    new_df["yield_l"] = pd.to_numeric(new_df["yield_l"], errors="coerce")
    new_df = new_df.dropna()

    milk_file = FARMS_DIR / farm_id / "milk_production.csv"
    if milk_file.exists():
        existing = pd.read_csv(milk_file)
        combined = pd.concat([existing, new_df], ignore_index=True).drop_duplicates(
            subset=["cow_id", "date"]
        )
    else:
        combined = new_df

    combined.to_csv(milk_file, index=False)

    return JSONResponse({
        "status": "ok",
        "farm_id": farm_id,
        "filename": file.filename,
        "new_records": len(new_df),
        "total_records": len(combined),
        "summary": {
            "avg_yield_l_per_cow_per_day": round(combined["yield_l"].mean(), 3),
            "total_cows": int(combined["cow_id"].nunique()),
            "date_range": [combined["date"].min(), combined["date"].max()],
        }
    })


# ---------------------------------------------------------------------------
# Legacy endpoints (farm_001 passthrough — keeps existing dashboard working)
# ---------------------------------------------------------------------------

@app.get("/api/herd/full")
def herd_full():
    return JSONResponse(_load_herd("farm_001"))

@app.get("/api/herd/summary")
def herd_summary_legacy():
    return JSONResponse(_load_herd("farm_001")["summary"])

@app.get("/api/herd/daily")
def herd_daily():
    return JSONResponse(_load_herd("farm_001")["daily_averages"])

@app.get("/api/herd/records_per_animal")
def herd_records():
    return JSONResponse(_load_herd("farm_001")["records_per_animal"])

@app.get("/api/herd/animal/{animal_id}")
def herd_animal(animal_id: str):
    df = _load_telemetry("farm_001")
    animal_df = df[df["animal_id"] == animal_id]
    if animal_df.empty:
        raise HTTPException(status_code=404, detail=f"Animal {animal_id} not found")
    cols = [c for c in BEHAVIOUR_COLS if c in animal_df.columns]
    records = (
        animal_df[cols]
        .sort_values("timestamp")
        .assign(timestamp=lambda x: x["timestamp"].astype(str))
        .round(3)
        .to_dict(orient="records")
    )
    return JSONResponse({"animal_id": animal_id, "records": records})


# ---------------------------------------------------------------------------
# Market data endpoints
# ---------------------------------------------------------------------------

@app.get("/api/market")
def market_data():
    if not MARKET_FILE.exists():
        raise HTTPException(status_code=503, detail="Market data not yet available — fetch in progress")
    return JSONResponse(json.loads(MARKET_FILE.read_text()))

@app.post("/api/market/refresh")
def market_refresh():
    threading.Thread(target=_refresh_market_data, daemon=True).start()
    return JSONResponse({"status": "refresh started"})


# ---------------------------------------------------------------------------
# Auth endpoints
# ---------------------------------------------------------------------------

@app.post("/api/auth/login")
def auth_login(username: str = Form(...), password: str = Form(...)):
    if username == _AUTH_USER and password == _AUTH_PASS:
        return JSONResponse({"token": _make_token(username)})
    raise HTTPException(status_code=401, detail="Invalid credentials")


@app.get("/api/auth/verify")
def auth_verify(token: str = ""):
    if _verify_token(token):
        return JSONResponse({"ok": True})
    raise HTTPException(status_code=401, detail="Invalid or expired token")


app.mount("/", StaticFiles(directory=ROOT / "dashboard", html=True), name="static")
