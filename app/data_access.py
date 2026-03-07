from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pandas as pd
import streamlit as st

from apps.api.service import PlatformService
from services.canonical_queries import (
    build_validation_report_from_store,
    query_canonical_observations,
)


@st.cache_resource(show_spinner=False)
def get_platform_service() -> PlatformService:
    return PlatformService()


def load_app_payload_from_store(limit: int = 250000) -> tuple[pd.DataFrame, dict]:
    service = get_platform_service()
    df = query_canonical_observations(service, limit=limit)
    return df, build_validation_report_from_store(df)


def ingest_sensor_upload(service: PlatformService, uploaded_file) -> dict:
    uploads_dir = Path("data/uploads")
    uploads_dir.mkdir(parents=True, exist_ok=True)
    tmp_path = uploads_dir / f"sensor_upload_{uuid4().hex}.csv"
    tmp_path.write_bytes(uploaded_file.getbuffer())
    try:
        return service.run_ingestion(
            connector_key="sensor_upload",
            source_system="streamlit_sensor_upload",
            mode="uploaded_file",
            config={
                "file_path": str(tmp_path),
                "organization_id": "ORG-001",
                "farm_id": "FARM-001",
                "herd_id": "HERD-001",
                "device_id": "SENSOR-UPLOAD",
            },
        )
    finally:
        tmp_path.unlink(missing_ok=True)
