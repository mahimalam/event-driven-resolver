"""Atomic resolution executor with latency budget enforcement."""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Optional

from ..config.latency_profile import MAX_ROUND_TRIP_MS, ATOMIC_EXECUTION_RETRIES
from ..models.types import DivergenceSignal, RouteSignal, WeatherAlert

logger = logging.getLogger(__name__)


class AsyncExecutor:
    """Resolves divergence signals into RouteSignals within the latency budget.

    If resolution cannot complete within MAX_ROUND_TRIP_MS, the payload aborts
    via a KILL_ON_FAILURE directive to preserve pipeline integrity.
    """

    def __init__(self) -> None:
        self._total_resolved = 0
        self._total_aborted = 0

    async def resolve(
        self,
        signal: DivergenceSignal,
        alert: WeatherAlert,
    ) -> Optional[RouteSignal]:
        """Attempt to resolve a divergence signal into a route within the time budget."""
        deadline_ms = MAX_ROUND_TRIP_MS
        t0 = time.monotonic()

        for attempt in range(1, ATOMIC_EXECUTION_RETRIES + 1):
            try:
                route = await asyncio.wait_for(
                    self._resolve_once(signal, alert),
                    timeout=deadline_ms / 1000.0,
                )
                elapsed_ms = (time.monotonic() - t0) * 1000.0
                route.latency_ms = round(elapsed_ms, 2)
                self._total_resolved += 1
                logger.info(
                    "Resolved %s in %.1fms (attempt %d/%d)",
                    signal.alert_id, elapsed_ms, attempt, ATOMIC_EXECUTION_RETRIES,
                )
                return route
            except asyncio.TimeoutError:
                elapsed_ms = (time.monotonic() - t0) * 1000.0
                logger.warning(
                    "Resolution timeout for %s after %.1fms (attempt %d/%d) — KILL_ON_FAILURE",
                    signal.alert_id, elapsed_ms, attempt, ATOMIC_EXECUTION_RETRIES,
                )
                if attempt == ATOMIC_EXECUTION_RETRIES:
                    self._total_aborted += 1
                    return None
            except Exception as exc:
                logger.error("Resolution error for %s: %s", signal.alert_id, exc)
                return None

        return None

    async def _resolve_once(
        self,
        signal: DivergenceSignal,
        alert: WeatherAlert,
    ) -> RouteSignal:
        """Build the resolution vector from the divergence signal."""
        # Simulate sub-millisecond local computation (no network I/O in resolution path)
        await asyncio.sleep(0)

        confidence = signal.severity_score * signal.posterior_prob
        resolved = signal.posterior_prob >= 0.5 and signal.delta_pct >= 20.0

        return RouteSignal(
            alert_id=signal.alert_id,
            event_type=signal.event_type,
            resolved=resolved,
            confidence=round(confidence, 4),
            resolution_source="divergence_detector",
            resolved_at=datetime.now(timezone.utc),
            latency_ms=0.0,
            metadata={
                "prior_prob": signal.prior_prob,
                "posterior_prob": signal.posterior_prob,
                "delta_pct": signal.delta_pct,
                "area": alert.area_desc[:100],
                "severity": alert.severity,
                "certainty": alert.certainty,
            },
        )

    @property
    def stats(self) -> dict:
        return {
            "total_resolved": self._total_resolved,
            "total_aborted": self._total_aborted,
        }
