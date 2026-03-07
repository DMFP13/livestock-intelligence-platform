from __future__ import annotations

import streamlit as st

from services.operator_controls import (
    create_or_update_source_config,
    list_source_operator_rows,
    run_sync_now,
    test_connector_config,
    toggle_connector_active,
)
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
    platform_service=None,
    live_source_health=None,
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

    st.markdown("#### Live Source Operator Controls")
    if platform_service is None:
        st.info("Canonical service unavailable; live source controls are disabled.")
    else:
        try:
            meta = platform_service.list_connectors_metadata()
            mode_by_connector = {str(m.get("key")): list(m.get("modes") or []) for m in meta}
            operator_rows = list_source_operator_rows(platform_service)
        except Exception as exc:
            st.warning(f"Live source controls unavailable for current service instance: {exc}")
            st.info("Restart Streamlit to refresh service cache after backend changes.")
            operator_rows = []
            mode_by_connector = {}

        if operator_rows:
            st.dataframe(operator_rows, use_container_width=True, hide_index=True)
        else:
            st.info("No source configurations yet.")

        with st.expander("Add or update source configuration", expanded=False):
            connector_options = sorted(mode_by_connector.keys())
            if not connector_options:
                st.warning("No connectors registered.")
            else:
                selected_connector = st.selectbox(
                    "Connector name",
                    options=connector_options,
                    key="op_cfg_connector",
                )
                mode_options = mode_by_connector.get(selected_connector, ["manual_upload"])
                with st.form("source_config_form"):
                    selected_mode = st.selectbox("Mode", options=mode_options, key="op_cfg_mode")
                    source_system = st.text_input("Source system", value=f"{selected_connector}_source", key="op_cfg_source")
                    is_active = st.checkbox("Active", value=False, key="op_cfg_active")
                    endpoint_url = st.text_input("Endpoint URL", value="", key="op_cfg_endpoint")
                    api_key_ref = st.text_input("API key reference", value="", key="op_cfg_apikey")
                    polling_interval = st.number_input(
                        "Polling interval (seconds)",
                        min_value=15,
                        value=300,
                        step=15,
                        key="op_cfg_poll",
                    )
                    config_json = st.text_area("Config JSON", value="{}", key="op_cfg_config_json", height=100)
                    retry_max = st.number_input("Retry max", min_value=0, value=2, step=1, key="op_cfg_retry")
                    submitted = st.form_submit_button("Save source configuration")
                if submitted:
                    try:
                        row = create_or_update_source_config(
                            platform_service,
                            connector_key=selected_connector,
                            source_system=source_system,
                            mode=selected_mode,
                            is_active=is_active,
                            endpoint_url=endpoint_url,
                            api_key_ref=api_key_ref,
                            polling_interval_sec=int(polling_interval) if selected_mode == "polling" else None,
                            config_json=config_json,
                            retry_max=int(retry_max),
                        )
                        st.success("Source configuration saved")
                        st.json(row)
                    except Exception as exc:
                        st.error(f"Failed to save source config: {exc}")

        with st.expander("Manual actions", expanded=False):
            if not operator_rows:
                st.caption("Create a source configuration first.")
            else:
                row_by_id = {str(r["source_config_id"]): r for r in operator_rows}
                selected_id = st.selectbox(
                    "Source configuration",
                    options=list(row_by_id.keys()),
                    format_func=lambda rid: (
                        f"{row_by_id[rid]['connector']} | {row_by_id[rid]['source_system']} | {row_by_id[rid]['mode']} | "
                        f"{row_by_id[rid]['status']}"
                    ),
                    key="op_action_source_id",
                )
                c1, c2, c3, c4 = st.columns(4)
                with c1:
                    if st.button("Run sync now", key="op_run_now"):
                        st.json(run_sync_now(platform_service, selected_id))
                with c2:
                    if st.button("Test connector config", key="op_test_cfg"):
                        st.json(test_connector_config(platform_service, selected_id))
                with c3:
                    if st.button("Enable connector", key="op_enable_cfg"):
                        try:
                            st.json(toggle_connector_active(platform_service, selected_id, True))
                        except Exception as exc:
                            st.error(f"Enable failed: {exc}")
                with c4:
                    if st.button("Disable connector", key="op_disable_cfg"):
                        st.json(toggle_connector_active(platform_service, selected_id, False))

        if live_source_health and live_source_health.get("sources"):
            st.caption(
                "Live source health: "
                f"{live_source_health.get('active_sources', 0)} active / "
                f"{live_source_health.get('failing_sources', 0)} failing / "
                f"{live_source_health.get('total_sources', 0)} total"
            )

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
