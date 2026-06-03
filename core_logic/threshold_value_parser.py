"""Threshold and severity parsing utilities for weather alert processing."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional


@dataclass
class AlertThreshold:
    """Parsed severity threshold derived from an NWS alert."""
    event_type: str
    severity: str
    certainty: str
    severity_score: float
    certainty_multiplier: float
    composite_score: float
    expires: Optional[datetime]
    seconds_to_expiry: float


_SEVERITY_WEIGHTS = {
    "Extreme":  1.00,
    "Severe":   0.75,
    "Moderate": 0.50,
    "Minor":    0.25,
    "Unknown":  0.10,
}

_CERTAINTY_WEIGHTS = {
    "Observed": 1.00,
    "Likely":   0.80,
    "Possible": 0.50,
    "Unlikely": 0.20,
    "Unknown":  0.10,
}

_WIND_SPEED_RE = re.compile(r"(\d+)\s*(?:mph|knots|kt)", re.IGNORECASE)
_RAINFALL_RE = re.compile(r"(\d+(?:\.\d+)?)\s*(?:inches?|in)\s*(?:per|/)\s*hour", re.IGNORECASE)


def parse_alert_threshold(
    event_type: str,
    severity: str,
    certainty: str,
    expires: Optional[datetime] = None,
) -> Optional[AlertThreshold]:
    """Convert NWS alert metadata into a structured threshold specification."""
    sev_score = _SEVERITY_WEIGHTS.get(severity, _SEVERITY_WEIGHTS["Unknown"])
    cert_mult = _CERTAINTY_WEIGHTS.get(certainty, _CERTAINTY_WEIGHTS["Unknown"])
    composite = sev_score * cert_mult
    if composite < 0.05:
        return None

    now = datetime.now(timezone.utc)
    secs_to_expiry = 0.0
    if expires is not None:
        delta = (expires - now).total_seconds()
        secs_to_expiry = max(0.0, delta)

    return AlertThreshold(
        event_type=event_type,
        severity=severity,
        certainty=certainty,
        severity_score=round(sev_score, 4),
        certainty_multiplier=round(cert_mult, 4),
        composite_score=round(composite, 4),
        expires=expires,
        seconds_to_expiry=round(secs_to_expiry, 1),
    )


def extract_wind_speed_mph(description: str) -> Optional[float]:
    """Extract wind speed in mph from an alert description text."""
    match = _WIND_SPEED_RE.search(description)
    if match:
        return float(match.group(1))
    return None


def extract_rainfall_rate(description: str) -> Optional[float]:
    """Extract rainfall rate in inches/hour from an alert description text."""
    match = _RAINFALL_RE.search(description)
    if match:
        return float(match.group(1))
    return None


def filter_eligible_alerts(
    alerts: list,
    *,
    min_composite_score: float = 0.35,
    max_seconds_to_expiry: float = 7200.0,
    min_seconds_to_expiry: float = 60.0,
) -> list:
    """Filter alerts to those worth processing based on score and timing."""
    now = datetime.now(timezone.utc)
    out = []
    for alert in alerts:
        spec = parse_alert_threshold(alert.event_type, alert.severity, alert.certainty, alert.expires)
        if spec is None or spec.composite_score < min_composite_score:
            continue
        if spec.expires is not None:
            remaining = (spec.expires - now).total_seconds()
            if remaining < min_seconds_to_expiry or remaining > max_seconds_to_expiry:
                continue
        out.append(alert)
    return out
