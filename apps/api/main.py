from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

from apps.api.service import PlatformService


service = PlatformService()


class Handler(BaseHTTPRequestHandler):
    def _send(self, status: int, payload: dict | list) -> None:
        body = json.dumps(payload, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        limit = int(query.get("limit", ["200"])[0])

        if parsed.path == "/health":
            self._send(200, {"status": "ok"})
            return
        if parsed.path == "/connectors":
            self._send(200, {"connectors": service.registry.list()})
            return
        if parsed.path == "/connectors/metadata":
            self._send(200, {"connectors": service.list_connectors_metadata()})
            return
        if parsed.path == "/farms":
            self._send(200, service.list_farms(limit=limit))
            return
        if parsed.path == "/animals":
            self._send(200, service.list_animals(limit=limit))
            return
        if parsed.path == "/observations":
            self._send(200, service.list_observations(limit=limit))
            return
        if parsed.path == "/events":
            self._send(200, service.list_events(limit=limit))
            return
        if parsed.path == "/alerts":
            self._send(200, service.list_alerts(limit=limit))
            return
        if parsed.path == "/reference-series":
            self._send(200, service.list_reference_series(limit=limit))
            return
        if parsed.path == "/ingestion-runs":
            self._send(200, service.list_ingestion_runs(limit=limit))
            return
        if parsed.path.startswith("/ingestion-runs/"):
            run_id = parsed.path.split("/")[-1]
            run = service.get_ingestion_run(run_id)
            if run is None:
                self._send(404, {"error": "not found"})
                return
            self._send(200, run)
            return
        if parsed.path == "/data-quality":
            self._send(200, service.data_quality_summary())
            return
        if parsed.path == "/source-configs":
            connector_key = query.get("connector_key", [None])[0]
            mode = query.get("mode", [None])[0]
            active_only = query.get("active_only", ["false"])[0].lower() in {"1", "true", "yes"}
            self._send(
                200,
                service.list_source_configs(
                    connector_key=connector_key,
                    mode=mode,
                    active_only=active_only,
                    limit=limit,
                ),
            )
            return
        if parsed.path == "/source-health":
            self._send(200, service.source_health_summary())
            return

        self._send(404, {"error": "unknown route"})

    def do_POST(self) -> None:  # noqa: N802
        if self.path not in {"/ingestion/run", "/source-configs/upsert", "/live-sync/run", "/live-sync/poll-cycle"}:
            self._send(404, {"error": "unknown route"})
            return

        content_length = int(self.headers.get("Content-Length", "0"))
        payload = json.loads(self.rfile.read(content_length) or "{}")

        if self.path == "/source-configs/upsert":
            result = service.upsert_source_config(
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
            self._send(200, result)
            return
        if self.path == "/live-sync/run":
            result = service.run_live_sync_for_source(str(payload.get("sourceConfigId")))
            self._send(200, result)
            return
        if self.path == "/live-sync/poll-cycle":
            result = service.run_live_poll_cycle(max_jobs=int(payload.get("maxJobs", 10)))
            self._send(200, result)
            return

        result = service.run_ingestion(
            connector_key=str(payload.get("connectorKey")),
            source_system=str(payload.get("sourceSystem")),
            mode=str(payload.get("mode", "uploaded_file")),
            config=dict(payload.get("config") or {}),
        )
        self._send(200, result)


def run(host: str = "0.0.0.0", port: int = 8080) -> None:
    server = HTTPServer((host, port), Handler)
    print(f"API listening on http://{host}:{port}")
    server.serve_forever()


if __name__ == "__main__":
    run()
