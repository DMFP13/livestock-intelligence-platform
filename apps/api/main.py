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

        self._send(404, {"error": "unknown route"})

    def do_POST(self) -> None:  # noqa: N802
        if self.path != "/ingestion/run":
            self._send(404, {"error": "unknown route"})
            return

        content_length = int(self.headers.get("Content-Length", "0"))
        payload = json.loads(self.rfile.read(content_length) or "{}")

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
