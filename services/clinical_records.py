from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

import pandas as pd

CLINICAL_ROOT = Path("outputs/clinical_records")


def _slug(animal_id: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in str(animal_id))


def _animal_dir(animal_id: str) -> Path:
    path = CLINICAL_ROOT / _slug(animal_id)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _record_path(animal_id: str) -> Path:
    return _animal_dir(animal_id) / "record.json"


def _history_path(animal_id: str) -> Path:
    return _animal_dir(animal_id) / "history.jsonl"


def load_animal_clinical_record(animal_id: str) -> dict[str, Any]:
    path = _record_path(animal_id)
    if not path.exists():
        return {
            "animal_id": str(animal_id),
            "note": "",
            "photo_path": None,
            "updated_at": None,
            "updated_by": "streamlit",
        }
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {
            "animal_id": str(animal_id),
            "note": "",
            "photo_path": None,
            "updated_at": None,
            "updated_by": "streamlit",
        }


def load_animal_clinical_history(animal_id: str, limit: int = 50) -> pd.DataFrame:
    path = _history_path(animal_id)
    if not path.exists():
        return pd.DataFrame(columns=["updated_at", "note_preview", "photo_updated"])

    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except Exception:
            continue
        note = str(payload.get("note") or "")
        rows.append(
            {
                "updated_at": payload.get("updated_at"),
                "note_preview": (note[:80] + "...") if len(note) > 80 else note,
                "photo_updated": bool(payload.get("photo_path")),
            }
        )

    if not rows:
        return pd.DataFrame(columns=["updated_at", "note_preview", "photo_updated"])
    out = pd.DataFrame(rows).tail(limit)
    return out.sort_values("updated_at", ascending=False)


def save_animal_clinical_record(
    animal_id: str,
    *,
    note: str,
    uploaded_photo,
    updated_by: str = "streamlit",
) -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    record = load_animal_clinical_record(animal_id)

    photo_path = record.get("photo_path")
    if uploaded_photo is not None:
        ext = Path(str(getattr(uploaded_photo, "name", "photo.bin"))).suffix.lower() or ".bin"
        if ext not in {".png", ".jpg", ".jpeg", ".webp"}:
            ext = ".bin"
        target = _animal_dir(animal_id) / f"photo{ext}"
        target.write_bytes(uploaded_photo.getbuffer())
        photo_path = str(target)

    payload = {
        "animal_id": str(animal_id),
        "note": str(note or ""),
        "photo_path": photo_path,
        "updated_at": now,
        "updated_by": updated_by,
    }

    _record_path(animal_id).write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
    with _history_path(animal_id).open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=True) + "\n")

    return payload
