"""Core data transfer objects for event detection and resolution pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional


@dataclass(frozen=True)
class WeatherAlert:
    """A single NWS severe weather alert."""
    alert_id: str
    event_type: str
    severity: str
    certainty: str
    onset: Optional[datetime]
    expires: Optional[datetime]
    area_desc: str
    headline: str


@dataclass(frozen=True)
class DivergenceSignal:
    """Detected divergence between alert state and prior model probability."""
    alert_id: str
    event_type: str
    prior_prob: float
    posterior_prob: float
    delta_pct: float
    severity_score: float
    detected_at: datetime


@dataclass
class RouteSignal:
    """Resolved execution vector ready for downstream dispatch."""
    alert_id: str
    event_type: str
    resolved: bool
    confidence: float
    resolution_source: str
    resolved_at: datetime
    latency_ms: float
    metadata: dict = field(default_factory=dict)


@dataclass
class ResolutionResult:
    """Final outcome of an event resolution attempt."""
    alert_id: str
    success: bool
    confidence: float
    latency_ms: float
    reason: str
    resolved_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


SEVERITY_SCORES: dict[str, float] = {
    "Extreme":  1.0,
    "Severe":   0.75,
    "Moderate": 0.50,
    "Minor":    0.25,
    "Unknown":  0.10,
}

CERTAINTY_MULTIPLIERS: dict[str, float] = {
    "Observed": 1.0,
    "Likely":   0.80,
    "Possible": 0.50,
    "Unlikely": 0.20,
    "Unknown":  0.10,
}

HIGH_IMPACT_EVENTS = frozenset({
    "Tornado Warning",
    "Tornado Emergency",
    "Flash Flood Emergency",
    "Severe Thunderstorm Warning",
    "Flash Flood Warning",
    "Blizzard Warning",
    "Ice Storm Warning",
    "Hurricane Warning",
    "Tsunami Warning",
})
