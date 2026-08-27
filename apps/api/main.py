from __future__ import annotations

from pathlib import Path
from typing import Any
from uuid import uuid4

from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool

from apps.api import analytics
from apps.api.auth import ForbiddenError, UnauthorizedError
from apps.api.service import PlatformService

service = PlatformService()

app = FastAPI(title="Livestock Intelligence Platform API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(UnauthorizedError)
def _unauthorized_handler(request: Request, exc: UnauthorizedError) -> JSONResponse:
    return JSONResponse(status_code=401, content={"error": str(exc)})


@app.exception_handler(ForbiddenError)
def _forbidden_handler(request: Request, exc: ForbiddenError) -> JSONResponse:
    return JSONResponse(status_code=403, content={"error": str(exc)})


@app.exception_handler(ValueError)
def _value_error_handler(request: Request, exc: ValueError) -> JSONResponse:
    return JSONResponse(status_code=400, content={"error": str(exc)})


def _authenticate(request: Request) -> None:
    principal = service.auth.authenticate_headers(request.headers)
    service.set_current_principal(principal)


@app.middleware("http")
async def _auth_and_cleanup(request: Request, call_next):
    try:
        if request.url.path != "/health":
            _authenticate(request)
        response = await call_next(request)
    finally:
        service.set_current_principal(None)
    return response


@app.get("/health")
def health() -> dict[str, Any]:
    return {"status": "ok"}


@app.get("/connectors")
def connectors() -> dict[str, Any]:
    return {"connectors": service.registry.list()}


@app.get("/connectors/metadata")
def connectors_metadata() -> dict[str, Any]:
    return {"connectors": service.list_connectors_metadata()}


@app.get("/auth/me")
def auth_me() -> dict[str, Any]:
    return service.current_user()


@app.get("/auth/permissions")
def auth_permissions() -> dict[str, Any]:
    me = service.current_user()
    return {"permissions": me.get("permissions", []), "roles": me.get("roles", [])}


@app.get("/auth/scope")
def auth_scope() -> dict[str, Any]:
    return service.current_user()


@app.get("/farms")
def farms(limit: int = 200) -> dict[str, Any]:
    return {"farms": service.list_farms(limit=limit)}


@app.post("/farms")
async def create_farm(request: Request) -> dict[str, Any]:
    payload = await request.json()
    return service.create_farm(
        name=str(payload.get("name")),
        location=payload.get("location"),
        region=payload.get("region"),
        species=payload.get("species"),
        sensor_system=payload.get("sensorSystem") or payload.get("system"),
        contact=payload.get("contact"),
        notes=payload.get("notes"),
        organization_id=payload.get("organizationId") or "ORG-001",
        farm_id=payload.get("id"),
    )


@app.delete("/farms/{farm_id}")
def delete_farm(farm_id: str) -> dict[str, Any]:
    service.delete_farm(farm_id)
    return {"deleted": farm_id}


@app.post("/admin/truncate/{table}")
def truncate_table(table: str) -> dict[str, Any]:
    service.truncate_table(table)
    analytics.bust_cache()
    return {"truncated": table}


@app.get("/animals")
def animals(limit: int = 200, farm_id: str | None = None) -> dict[str, Any]:
    return {"animals": service.list_animals(limit=limit, farm_id=farm_id)}


@app.get("/observations")
def observations(limit: int = 200, farm_id: str | None = None) -> dict[str, Any]:
    return {"observations": service.list_observations(limit=limit, farm_id=farm_id)}


@app.get("/events")
def events(limit: int = 200) -> dict[str, Any]:
    return {"events": service.list_events(limit=limit)}


@app.get("/alerts")
def alerts(limit: int = 200, farm_id: str | None = None) -> dict[str, Any]:
    return {"alerts": service.list_alerts(limit=limit, farm_id=farm_id)}


@app.get("/overview")
def overview(farm_id: str | None = None) -> dict[str, Any]:
    return analytics.build_overview(service, selected_farm=farm_id)


@app.get("/farms/{farm_id}/profile")
def farm_profile(farm_id: str) -> dict[str, Any]:
    payload = analytics.build_farm_profile(service, farm_id)
    if payload is None:
        return JSONResponse(status_code=404, content={"error": "farm not found or has no data"})
    return payload


@app.get("/market-finance")
def market_finance() -> dict[str, Any]:
    return analytics.build_market_finance(service)


@app.get("/feed-environment")
def feed_environment(farm_id: str | None = None) -> dict[str, Any]:
    return analytics.build_feed_environment(service, selected_farm=farm_id)


@app.get("/animals/{animal_id}/profile")
def animal_profile(animal_id: str) -> dict[str, Any]:
    payload = analytics.build_animal_profile(service, animal_id)
    if payload is None:
        return JSONResponse(status_code=404, content={"error": "animal not found or has no data"})
    return payload


@app.get("/animals/{animal_id}/timeseries")
def animal_timeseries(animal_id: str, metrics: str = "") -> dict[str, Any]:
    metric_list = [m for m in metrics.split(",") if m]
    return analytics.build_animal_timeseries(service, animal_id, metric_list)


@app.get("/outcomes")
def outcomes() -> dict[str, Any]:
    return analytics.build_outcomes(service)


@app.post("/ingestion/upload")
async def ingestion_upload(
    file: UploadFile = File(...),
    farm_id: str = Form(...),
    connector_key: str = Form("sensor_upload"),
    source_system: str = Form("api_upload"),
) -> dict[str, Any]:
    uploads_dir = Path("data/uploads")
    uploads_dir.mkdir(parents=True, exist_ok=True)
    suffix = Path(file.filename or "upload.csv").suffix or ".csv"
    tmp_path = uploads_dir / f"{connector_key}_{uuid4().hex}{suffix}"
    tmp_path.write_bytes(await file.read())
    try:
        result = await run_in_threadpool(
            service.run_ingestion,
            connector_key=connector_key,
            source_system=source_system,
            mode="uploaded_file",
            config={"file_path": str(tmp_path), "farm_id": farm_id},
        )
        analytics.bust_cache()
        return result
    finally:
        tmp_path.unlink(missing_ok=True)


@app.get("/reference-series")
def reference_series(limit: int = 200) -> dict[str, Any]:
    return {"referenceSeries": service.list_reference_series(limit=limit)}


@app.get("/ingestion-runs")
def ingestion_runs(limit: int = 100) -> dict[str, Any]:
    return {"ingestionRuns": service.list_ingestion_runs(limit=limit)}


@app.get("/data-quality")
def data_quality() -> dict[str, Any]:
    return service.data_quality_summary()


@app.get("/source-configs")
def source_configs(
    connector_key: str | None = None,
    mode: str | None = None,
    active_only: bool = False,
    limit: int = 500,
) -> dict[str, Any]:
    return {
        "sourceConfigs": service.list_source_configs(
            connector_key=connector_key,
            mode=mode,
            active_only=active_only,
            limit=limit,
        )
    }


@app.get("/source-health")
def source_health() -> dict[str, Any]:
    return service.source_health_summary()


@app.post("/source-configs/upsert")
async def upsert_source_config(request: Request) -> dict[str, Any]:
    payload = await request.json()
    return service.upsert_source_config(
        connector_key=str(payload.get("connectorKey")),
        source_system=str(payload.get("sourceSystem")),
        mode=str(payload.get("mode", "polling")),
        endpoint_url=payload.get("endpointUrl"),
        api_key_ref=payload.get("apiKeyRef"),
        auth=dict(payload.get("auth") or {}),
        polling_interval_sec=payload.get("pollingIntervalSec"),
        is_active=bool(payload.get("isActive", False)),
        webhook_secret_ref=payload.get("webhookSecretRef"),
        config=dict(payload.get("config") or {}),
        retry_max=int(payload.get("retryMax", 2)),
    )


@app.post("/live-sync/run")
async def live_sync_run(request: Request) -> dict[str, Any]:
    payload = await request.json()
    return service.run_live_sync_for_source(str(payload.get("sourceConfigId")))


@app.post("/live-sync/poll-cycle")
async def live_sync_poll_cycle(request: Request) -> dict[str, Any]:
    payload = await request.json()
    return {"results": service.run_live_poll_cycle(max_jobs=int(payload.get("maxJobs", 10)))}


@app.post("/ingestion/run")
async def ingestion_run(request: Request) -> dict[str, Any]:
    payload = await request.json()
    result = await run_in_threadpool(
        service.run_ingestion,
        connector_key=str(payload.get("connectorKey")),
        source_system=str(payload.get("sourceSystem")),
        mode=str(payload.get("mode", "uploaded_file")),
        config=dict(payload.get("config") or {}),
    )
    analytics.bust_cache()
    return result


def run(host: str = "0.0.0.0", port: int = 8080) -> None:
    import uvicorn

    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    run()
