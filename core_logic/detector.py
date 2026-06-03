"""Divergence detector — compares NWS alert probability against prior model estimates."""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Optional

from ..models.types import WeatherAlert, DivergenceSignal, SEVERITY_SCORES, CERTAINTY_MULTIPLIERS
from .implied_prob import implied_prob_from_alert

logger = logging.getLogger(__name__)

# Minimum posterior-vs-prior delta to be considered a divergence worth acting on
MIN_DELTA_PCT = 20.0

# Climatological base rates by event type (approximate annual exceedance probability)
_BASE_RATES: dict[str, float] = {
    "Tornado Warning":                 0.02,
    "Tornado Emergency":               0.005,
    "Flash Flood Emergency":           0.01,
    "Severe Thunderstorm Warning":     0.08,
    "Flash Flood Warning":             0.06,
    "Blizzard Warning":                0.04,
    "Ice Storm Warning":               0.03,
    "Hurricane Warning":               0.005,
    "Tsunami Warning":                 0.001,
}
_DEFAULT_BASE_RATE = 0.05


def _prior_for_event(event_type: str) -> float:
    return _BASE_RATES.get(event_type, _DEFAULT_BASE_RATE)


def _severity_score(severity: str, certainty: str) -> float:
    sev = SEVERITY_SCORES.get(severity, SEVERITY_SCORES["Unknown"])
    cert = CERTAINTY_MULTIPLIERS.get(certainty, CERTAINTY_MULTIPLIERS["Unknown"])
    return sev * cert


class DivergenceDetector:
    """Detects probability divergences when a new alert is issued.

    When NWS issues a high-impact alert, the posterior probability of the physical
    event occurring jumps significantly above the climatological prior. This detector
    quantifies that jump and emits a DivergenceSignal when it exceeds the threshold.
    """

    def __init__(
        self,
        *,
        min_delta_pct: float = MIN_DELTA_PCT,
        min_severity_score: float = 0.35,
    ) -> None:
        self._min_delta_pct = min_delta_pct
        self._min_severity_score = min_severity_score
        self._processed: dict[str, float] = {}  # alert_id → processed_at

    def evaluate(self, alert: WeatherAlert) -> Optional[DivergenceSignal]:
        """Return a DivergenceSignal if the alert creates a significant divergence."""
        if alert.alert_id in self._processed:
            return None

        sev_score = _severity_score(alert.severity, alert.certainty)
        if sev_score < self._min_severity_score:
            logger.debug(
                "Alert %s severity score %.2f below threshold %.2f — skipped",
                alert.alert_id, sev_score, self._min_severity_score,
            )
            return None

        prior = _prior_for_event(alert.event_type)
        result = implied_prob_from_alert(
            alert.severity, alert.certainty, base_rate=prior
        )
        posterior = result.prob_above
        delta_pct = (posterior - prior) / max(prior, 1e-9) * 100.0

        self._processed[alert.alert_id] = time.monotonic()
        # Prune old entries
        if len(self._processed) > 5000:
            cutoff = time.monotonic() - 86400
            self._processed = {k: v for k, v in self._processed.items() if v > cutoff}

        if delta_pct < self._min_delta_pct:
            logger.debug(
                "Alert %s delta %.1f%% below threshold %.1f%% — skipped",
                alert.alert_id, delta_pct, self._min_delta_pct,
            )
            return None

        signal = DivergenceSignal(
            alert_id=alert.alert_id,
            event_type=alert.event_type,
            prior_prob=round(prior, 4),
            posterior_prob=round(posterior, 4),
            delta_pct=round(delta_pct, 2),
            severity_score=round(sev_score, 4),
            detected_at=datetime.now(timezone.utc),
        )
        logger.info(
            "Divergence detected: %s | prior=%.3f → posterior=%.3f (Δ%.1f%%) | %s",
            alert.event_type, prior, posterior, delta_pct, alert.area_desc[:60],
        )
        return signal
