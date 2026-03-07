"""Source connector interfaces and registry."""

from .base import ConnectorCapabilities, ConnectorContext, ConnectorResult, DataConnector
from .registry import ConnectorRegistry

__all__ = ["ConnectorCapabilities", "ConnectorContext", "ConnectorResult", "DataConnector", "ConnectorRegistry"]
