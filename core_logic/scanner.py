"""Main scanner loop — ties together trigger, detection, and resolution."""

from __future__ import annotations

import asyncio
import logging
from typing import Callable, Awaitable

from ..models.types import WeatherAlert, DivergenceSignal, RouteSignal
from .external_event_trigger import AlertEventTrigger
from .detector import DivergenceDetector
from ..execution.async_executor import AsyncExecutor

logger = logging.getLogger(__name__)


async def scan_cross_node_loop(
    on_signal: Callable[[RouteSignal], Awaitable[None]],
    *,
    poll_interval: float = 15.0,
    min_delta_pct: float = 20.0,
    min_severity_score: float = 0.35,
    area: str | None = None,
) -> None:
    """Main event-driven resolution loop.

    Continuously ingests NWS alerts, detects probability divergences, and
    dispatches resolution vectors for confirmed high-impact events.

    Args:
        on_signal: Async callback invoked with each resolved RouteSignal.
        poll_interval: NWS API polling cadence in seconds.
        min_delta_pct: Minimum posterior-vs-prior probability jump to act on.
        min_severity_score: Minimum combined severity×certainty score.
        area: Optional two-letter US state code to filter alerts geographically.
    """
    alert_queue: asyncio.Queue[WeatherAlert] = asyncio.Queue(maxsize=512)
    detector = DivergenceDetector(
        min_delta_pct=min_delta_pct,
        min_severity_score=min_severity_score,
    )
    executor = AsyncExecutor()
    trigger = AlertEventTrigger(
        alert_queue,
        poll_interval=poll_interval,
        area=area,
    )

    await trigger.start()
    logger.info(
        "Scanner online | poll=%.0fs min_delta=%.0f%% area=%s",
        poll_interval, min_delta_pct, area or "ALL",
    )

    try:
        while True:
            alert: WeatherAlert = await alert_queue.get()
            signal: DivergenceSignal | None = detector.evaluate(alert)
            if signal is None:
                continue
            route = await executor.resolve(signal, alert)
            if route is not None:
                await on_signal(route)
    except asyncio.CancelledError:
        logger.info("Scanner cancelled — shutting down")
        raise
    finally:
        await trigger.stop()
