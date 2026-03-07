from __future__ import annotations

import streamlit as st

from services.source_health import build_quality_flag_rows, build_source_health_rows


def render_data_quality(
    validation_report: dict,
    build_data_validation_table,
    build_metric_registry_table,
    milk_validation,
    repro_validation,
    source_health,
    connector_list: list[str],
    sensor_upload_result,
) -> None:
    st.subheader("Data Quality")
    st.caption("Canonical schema, source health, and quality checks")

    st.markdown("#### Source Health")
    health_rows = build_source_health_rows(source_health)
    if health_rows:
        st.dataframe(health_rows, use_container_width=True, hide_index=True)
        st.caption(f"Registered connectors: {', '.join(connector_list) if connector_list else 'none'}")
    else:
        st.info("No ingestion runs recorded yet.")

    quality_rows = build_quality_flag_rows(source_health)
    if quality_rows:
        st.dataframe(quality_rows, use_container_width=True, hide_index=True)

    if sensor_upload_result is not None:
        st.markdown("#### Latest Manual Sensor Ingestion")
        st.json(sensor_upload_result)

    st.markdown("#### Validation Summary")
    st.dataframe(build_data_validation_table(validation_report), use_container_width=True, hide_index=True)

    st.markdown("#### Missingness")
    st.dataframe(validation_report["missingness"], use_container_width=True, hide_index=True)

    st.markdown("#### Metric Coverage + Range Checks")
    st.dataframe(validation_report["metric_coverage"], use_container_width=True, hide_index=True)

    st.markdown("#### Dtype Validation")
    st.dataframe(validation_report["schema"]["dtype_validation"], use_container_width=True, hide_index=True)

    st.markdown("#### Metric Registry")
    st.dataframe(build_metric_registry_table(), use_container_width=True, hide_index=True)

    st.markdown("#### Event Input Validation")
    if milk_validation is not None:
        st.write("Milk input validation")
        st.dataframe(
            [
                {"check": "row_count", "value": milk_validation.get("row_count")},
                {"check": "missing_required_columns", "value": ", ".join(milk_validation.get("missing_required_columns", []))},
                {"check": "invalid_date_count", "value": milk_validation.get("invalid_date_count")},
                {"check": "animal_date_duplicate_rows", "value": milk_validation.get("animal_date_duplicate_rows")},
            ],
            use_container_width=True,
            hide_index=True,
        )

    if repro_validation is not None:
        st.write("Reproduction input validation")
        st.dataframe(
            [
                {"check": "row_count", "value": repro_validation.get("row_count")},
                {"check": "missing_required_columns", "value": ", ".join(repro_validation.get("missing_required_columns", []))},
                {"check": "invalid_date_count", "value": repro_validation.get("invalid_date_count")},
                {"check": "animal_date_duplicate_rows", "value": repro_validation.get("animal_date_duplicate_rows")},
            ],
            use_container_width=True,
            hide_index=True,
        )
