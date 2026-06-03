"""NWS alert ingestion — polls NOAA's public API for severe weather events."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

import aiohttp

from ..models.types import WeatherAlert, HIGH_IMPACT_EVENTS

logger = logging.getLogger(__name__)

NWS_ALERTS_URL = "https://api.weather.gov/alerts/active"
NWS_HEADERS = {
    "User-Agent": "event-driven-resolver/1.0 (research; contact@example.com)",
    "Accept": "application/geo+json",
}
POLL_INTERVAL_SEC = 15.0


def _parse_alert(feature: dict) -> Optional[WeatherAlert]:
    """Extract a WeatherAlert from a GeoJSON feature dict."""
    props = feature.get("properties", {})
    alert_id = props.get("id") or feature.get("id")
    event_type = props.get("event", "")
    if not alert_id or not event_type:
        return None

    def _parse_dt(s: Optional[str]) -> Optional[datetime]:
        if not s:
            return None
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except ValueError:
            return None

    return WeatherAlert(
        alert_id=str(alert_id),
        event_type=event_type,
        severity=props.get("severity", "Unknown"),
        certainty=props.get("certainty", "Unknown"),
        onset=_parse_dt(props.get("onset")),
        expires=_parse_dt(props.get("expires")),
        area_desc=props.get("areaDesc", "")[:200],
        headline=props.get("headline", "")[:300],
    )


async def fetch_active_alerts(
    session: aiohttp.ClientSession,
    *,
    event_types: Optional[list[str]] = None,
    area: Optional[str] = None,
    timeout: float = 10.0,
) -> list[WeatherAlert]:
    """Fetch current active NWS alerts, optionally filtered by event type and area."""
    params: dict = {"status": "actual", "message_type": "alert"}
    if event_types:
        params["event"] = ",".join(event_types)
    if area:
        params["area"] = area

    try:
        async with session.get(
            NWS_ALERTS_URL,
            headers=NWS_HEADERS,
            params=params,
            timeout=aiohttp.ClientTimeout(total=timeout),
        ) as resp:
            if resp.status != 200:
                logger.warning("NWS API returned HTTP %d", resp.status)
                return []
            data = await resp.json(content_type=None)
    except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
        logger.warning("NWS fetch failed: %s", exc)
        return []

    alerts = []
    for feature in data.get("features", []):
        alert = _parse_alert(feature)
        if alert is not None:
            alerts.append(alert)
    return alerts


class AlertEventTrigger:
    """Continuously polls NWS for new high-impact alerts and emits them to a queue."""

    def __init__(
        self,
        queue: asyncio.Queue,
        *,
        poll_interval: float = POLL_INTERVAL_SEC,
        event_filter: Optional[frozenset] = None,
        area: Optional[str] = None,
    ) -> None:
        self._queue = queue
        self._poll_interval = poll_interval
        self._event_filter = event_filter or HIGH_IMPACT_EVENTS
        self._area = area
        self._seen: set[str] = set()
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()

    async def start(self) -> None:
        self._task = asyncio.create_task(self._poll_loop(), name="alert_trigger_poll")

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass

    async def _poll_loop(self) -> None:
        async with aiohttp.ClientSession() as session:
            while not self._stop.is_set():
                alerts = await fetch_active_alerts(
                    session,
                    event_types=list(self._event_filter),
                    area=self._area,
                )
                now = datetime.now(timezone.utc)
                new_count = 0
                for alert in alerts:
                    if alert.alert_id in self._seen:
                        continue
                    if alert.expires and alert.expires < now:
                        continue
                    self._seen.add(alert.alert_id)
                    await self._queue.put(alert)
                    new_count += 1
                if new_count:
                    logger.info("AlertTrigger: %d new alerts detected", new_count)
                # Prune stale seen IDs (keep last 2000)
                if len(self._seen) > 2000:
                    self._seen = set(list(self._seen)[-1000:])
                try:
                    await asyncio.wait_for(
                        asyncio.shield(self._stop.wait()),
                        timeout=self._poll_interval,
                    )
                except asyncio.TimeoutError:
                    pass
