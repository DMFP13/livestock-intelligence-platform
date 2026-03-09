from __future__ import annotations
import hashlib
import hmac
import json
import os
import threading
import time
from pathlib import Path
from fastapi import FastAPI, HTTPException, Form
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
