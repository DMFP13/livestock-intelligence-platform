from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class QualityFlag(str, Enum):
    good = "good"
    suspect = "suspect"
    bad = "bad"
    quarantined = "quarantined"


@dataclass
class Provenance:
    sourceSystem: str
    timestamp: datetime
    sourceRecordId: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ObservationRecord:
    organizationId: str | None
    farmId: str | None
    herdId: str | None
    animalId: str | None
    locationId: str | None
    deviceId: str | None
    metric: str
    value: float | int | str | None
    unit: str | None
    observedAt: datetime
    qualityFlag: QualityFlag
    sourceSystem: str
    sourceRecordId: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class EventRecord:
    organizationId: str | None
    farmId: str | None
    herdId: str | None
    animalId: str | None
    eventType: str
    eventAt: datetime
    severity: str | None
    qualityFlag: QualityFlag
    sourceSystem: str
    sourceRecordId: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class AlertRecord:
    organizationId: str | None
    farmId: str | None
    herdId: str | None
    animalId: str | None
    alertType: str
    alertAt: datetime
    status: str
    qualityFlag: QualityFlag
    sourceSystem: str
    sourceRecordId: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ReferenceSeriesRecord:
    organizationId: str | None
    farmId: str | None
    seriesType: str
    seriesKey: str
    pointAt: datetime
    value: float
    unit: str | None
    qualityFlag: QualityFlag
    sourceSystem: str
    sourceRecordId: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
