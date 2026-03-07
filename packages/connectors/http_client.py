from __future__ import annotations

import json
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def fetch_json_rows(
    *,
    endpoint_url: str,
    headers: dict[str, str] | None = None,
    query_params: dict[str, Any] | None = None,
    timeout_sec: int = 20,
    response_path: str | None = None,
) -> list[dict[str, Any]]:
    url = endpoint_url
    if query_params:
        qp = urlencode({k: v for k, v in query_params.items() if v is not None})
        if qp:
            sep = "&" if "?" in url else "?"
            url = f"{url}{sep}{qp}"

    req = Request(url=url, headers=headers or {}, method="GET")
    with urlopen(req, timeout=timeout_sec) as resp:
        status = getattr(resp, "status", 200)
        if int(status) >= 400:
            raise RuntimeError(f"http error {status}")
        payload = json.loads(resp.read().decode("utf-8"))

    extracted = _extract(payload, response_path)
    if isinstance(extracted, list):
        return [dict(r) for r in extracted if isinstance(r, dict)]
    if isinstance(extracted, dict):
        candidate = extracted.get("rows") or extracted.get("data")
        if isinstance(candidate, list):
            return [dict(r) for r in candidate if isinstance(r, dict)]
    raise ValueError("response did not contain a row list")


def build_headers(config: dict[str, Any]) -> dict[str, str]:
    headers: dict[str, str] = {"Accept": "application/json"}
    auth = config.get("auth")
    if isinstance(auth, dict):
        raw_headers = auth.get("headers")
        if isinstance(raw_headers, dict):
            headers.update({str(k): str(v) for k, v in raw_headers.items()})
        bearer = auth.get("bearer_token")
        if bearer:
            headers["Authorization"] = f"Bearer {bearer}"
        api_header = auth.get("api_key_header")
        api_value = auth.get("api_key_value")
        if api_header and api_value:
            headers[str(api_header)] = str(api_value)

    api_key = config.get("api_key")
    if api_key:
        headers.setdefault("X-API-Key", str(api_key))
    elif config.get("api_key_ref"):
        # Placeholder fallback until a secret manager resolves references.
        headers.setdefault("X-API-Key", str(config.get("api_key_ref")))

    return headers


def map_row_fields(row: dict[str, Any], field_map: dict[str, str], passthrough: list[str] | None = None) -> dict[str, Any]:
    mapped: dict[str, Any] = {}
    for canonical_key, source_key in field_map.items():
        mapped[canonical_key] = row.get(source_key)
    for key in passthrough or []:
        if key in row and key not in mapped:
            mapped[key] = row.get(key)
    return mapped


def _extract(payload: Any, response_path: str | None) -> Any:
    if not response_path:
        return payload
    node = payload
    for part in response_path.split("."):
        if isinstance(node, dict) and part in node:
            node = node[part]
        else:
            raise ValueError(f"response_path '{response_path}' not found")
    return node
