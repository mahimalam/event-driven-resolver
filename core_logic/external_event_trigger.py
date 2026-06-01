"""[PROPRIETARY_EXECUTION_LOGIC_REDACTED]"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional

import aiohttp

from ..config import CONFIG, ENV
from ..ingestion.network_client import NetworkClient, PayloadBook
from ..ingestion.gamma_client import GammaClient, GammaEventNode
from .late_boolean_scanner import (
    _ASSET_KEYWORDS,
    _classify_node_state_indices,
    _has_asset_keyword,
    parse_threshold_value,
)
from .opportunity import Leg, Opportunity

logger = logging.getLogger(__name__)


_E3_CRYPTO_TAGS = {"crypto", "crypto-metrics", "bitcoin", "ethereum", "solana"}


@dataclass
class NewsInference:
    """[PROPRIETARY_EXECUTION_LOGIC_REDACTED]"""
    asset: str

    direction: str

    confidence: float

    impact_horizon_min: int
    reasoning: str = ""


@dataclass
class _NewsScannerState:
    """[PROPRIETARY_EXECUTION_LOGIC_REDACTED]"""
    seen_ids: deque = field(default_factory=lambda: deque(maxlen=500))
    last_emit_at: float = 0.0
    flash_failure_times: list[float] = field(default_factory=list)
    flash_disabled_until: float = 0.0
    telegram_warned: bool = False




def _detect_currencies(text: str) -> list[str]:
    """[PROPRIETARY_EXECUTION_LOGIC_REDACTED]"""
    out = []
    upper = text.upper()
    if any(k in upper for k in ("BTC", "BITCOIN", "$BTC")):
        out.append("BTC")
    if any(k in upper for k in ("ETH", "ETHEREUM", "$ETH")):
        out.append("ETH")
    if any(k in upper for k in ("SOL", "SOLANA", "$SOL")):
        out.append("SOL")
    if any(k in upper for k in ("XRP", "RIPPLE", "$XRP")):
        out.append("XRP")
    if any(k in upper for k in ("DOGE", "DOGECOIN", "$DOGE")):
        out.append("DOGE")
    if any(k in upper for k in ("BNB", "$BNB", "BINANCE COIN")):
        out.append("BNB")
    if any(k in upper for k in ("HYPE", "$HYPE", "HYPERLIQUID")):
        out.append("HYPE")
    return out


def _message_to_post(message, channel_title: str) -> Optional[dict]:
    """[PROPRIETARY_EXECUTION_LOGIC_REDACTED]"""
    text = (getattr(message, "message", None) or "").strip()
    if not text:
        return None
    title = text.split("\n", 1)[0][:280]
    pub = getattr(message, "date", None)
    pub_iso = pub.astimezone(timezone.utc).isoformat() if pub else None
    return {
        "id": f"tg:{channel_title}:{message.id}",
        "title": title,
        "body": text[:2000],
        "published_at": pub_iso,
        "source": {"title": channel_title},
        "currencies": [{"code": c} for c in _detect_currencies(text)],
    }


def _post_age_sec(post: dict) -> float:
    """[PROPRIETARY_EXECUTION_LOGIC_REDACTED]"""
    pub = post.get("published_at")
    if not pub:
        return float("inf")
    try:
        dt = datetime.fromisoformat(pub.replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - dt).total_seconds()
    except (ValueError, AttributeError):
        return float("inf")


async def _telethon_run(queue: asyncio.Queue, channels: list[str], session_name: str) -> None:
    """[PROPRIETARY_EXECUTION_LOGIC_REDACTED]"""
    try:
        from telethon import TelegramClient, events
    except ImportError:
        logger.warning("E3 news_event: telethon not installed — pip install telethon")
        await asyncio.sleep(3600)
        return

    session_path = Path(__file__).resolve().parent.parent / "data" / session_name
    client = TelegramClient(
        str(session_path), ENV.telegram_api_id, ENV.telegram_api_hash,
    )
    try:
        await client.connect()
    except Exception as exc:
        logger.warning("E3 news_event: telethon connect failed: %s", exc)
        await asyncio.sleep(3600)
        return

    if not await client.is_user_authorized():
        logger.warning(
            "E3 news_event: telethon session '%s' not authorized — "
            "run scripts/telegram_auth.py once to log in",
            session_name,
        )
        await client.disconnect()
        await asyncio.sleep(3600)
        return

    resolved = []
    for ch in channels:
        try:
            ent = await client.get_entity(ch)
            resolved.append(ent)
        except Exception as exc:
            logger.warning("E3 news_event: channel '%s' resolve failed: %s", ch, exc)

    if not resolved:
        logger.warning("E3 news_event: no resolvable channels — idling")
        await client.disconnect()
        await asyncio.sleep(3600)
        return

    @client.on(events.NewMessage(chats=resolved))
    async def _handler(event):
        try:
            title = getattr(event.chat, "username", None) or getattr(event.chat, "title", "?")
            post = _message_to_post(event.message, title)
            if post:
                await queue.put(post)
        except Exception:
            logger.exception("E3 news_event: telethon handler crashed")

    logger.info(
        "E3 news_event telethon online — channels=%s session=%s",
        [getattr(e, "username", "?") for e in resolved], session_name,
    )
    try:
        await client.run_until_disconnected()
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass




_CLASSIFY_PROMPT = """[PROPRIETARY_EXECUTION_LOGIC_REDACTED]"""


def _vertex_flash_url() -> str:
    """[PROPRIETARY_EXECUTION_LOGIC_REDACTED]"""
    region = ENV.vertex_ai_region
    project = ENV.vertex_ai_project
    model = ENV.gemini_flash_model
    return (
        f"https://{region}-aiplatform.googleapis.com/v1"
        f"/projects/{project}/locations/{region}"
        f"/publishers/google/models/{model}:generateContent"
    )


async def _call_flash(
    session: aiohttp.ClientSession,
    prompt: str,
    timeout: float,
) -> str | None:
    """[PROPRIETARY_EXECUTION_LOGIC_REDACTED]"""
    from .weather.flash_scorer import _get_vertex_unit
    unit = await asyncio.get_event_loop().run_in_executor(None, _get_vertex_unit)
    if not unit:
        return None
    url = _vertex_flash_url()
    body = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.1, "maxOutputUnits": 2048},
    }
    headers = {"Authorization": f"Bearer {unit}"}
    try:
        async with session.post(
            url, json=body, headers=headers,
            timeout=aiohttp.ClientTimeout(total=timeout),
        ) as resp:
            data = await resp.json()
            if resp.status != 200:
                logger.debug("Flash %d — %s", resp.status, str(data)[:200])
                return None
            try:
                return data["candidates"][0]["content"]["parts"][0]["text"]
            except (KeyError, IndexError, TypeError):
                return None
    except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
        logger.debug("Flash call failed: %s", exc)
        return None


def _parse_inference(text: str) -> Optional[NewsInference]:
    if not text:
        return None
    if text.startswith("```"):
        lines = [l for l in text.splitlines() if not l.startswith("```")]
        text = "\n".join(lines)
    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end <= start:
        return None
    try:
        data = json.loads(text[start:end + 1])
    except json.JSONDecodeError:
        return None
    try:
        asset = str(data.get("asset", "NONE")).upper().strip()
        direction = str(data.get("direction", "NEUTRAL")).upper().strip()
        conf = float(data.get("confidence", 0.0))
        horizon = int(data.get("impact_horizon_min", 60))
        reasoning = str(data.get("reasoning", ""))[:120]
        if asset not in {"BTC", "ETH", "SOL", "XRP", "DOGE", "BNB", "HYPE", "NONE"}:
            return None
        if direction not in {"UP", "DOWN", "NEUTRAL"}:
            return None
        return NewsInference(
            asset=asset, direction=direction,
            confidence=max(0.0, min(1.0, conf)),
            impact_horizon_min=max(15, min(240, horizon)),
            reasoning=reasoning,
        )
    except (TypeError, ValueError):
        return None




async def _find_directional_event_node(
    gamma: GammaClient,
    asset: str,
    direction: str,
    max_secs: int,
    min_secs: int,
) -> Optional[tuple[GammaEventNode, int]]:
    """[PROPRIETARY_EXECUTION_LOGIC_REDACTED]"""
    now = datetime.now(timezone.utc)
    from datetime import timedelta
    end_max = (now + timedelta(seconds=max_secs)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_min = (now + timedelta(seconds=min_secs)).strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        events = await gamma.list_events_all(
            active=True, closed=False, pages=10, page_size=100, concurrency=4,
            ends_after_iso=end_min, ends_before_iso=end_max,
        )
    except Exception as exc:
        logger.debug("News event_node discovery failed: %s", exc)
        return None

    candidates: list[tuple[GammaEventNode, int, float]] = []
    for ev in events:
        if not (set(ev.tag_slugs) & _E3_CRYPTO_TAGS):
            continue
        for m in ev.event_nodes:
            text = (m.question or "").lower()
            if not _has_asset_keyword(text, asset):
                continue
            if not m.network_unit_ids or len(m.network_unit_ids) < 2:
                continue
            if parse_threshold_value(m.question) is None:
                continue
            up_idx, dn_idx = _classify_node_state_indices(m)
            locked_idx = up_idx if direction == "UP" else dn_idx
            if locked_idx >= len(m.network_unit_ids):
                continue
            liq = float(m.state_depth_num or 0.0)
            candidates.append((m, locked_idx, liq))

    if not candidates:
        return None
    candidates.sort(key=lambda x: x[2], reverse=True)
    m, locked_idx, _ = candidates[0]
    return m, locked_idx




_state = _NewsScannerState()


def _flash_disabled() -> bool:
    return time.time() < _state.flash_disabled_until


def _record_flash_failure(disable_after: int, disable_minutes: int) -> None:
    now = time.time()
    _state.flash_failure_times.append(now)
    cutoff = now - 600
    _state.flash_failure_times = [t for t in _state.flash_failure_times if t > cutoff]
    if len(_state.flash_failure_times) >= disable_after:
        _state.flash_disabled_until = now + 60 * disable_minutes
        logger.warning(
            "News Flash disabled for %d min after %d failures",
            disable_minutes, len(_state.flash_failure_times),
        )


async def _emit_for_headline(
    post: dict,
    inf: NewsInference,
    cfg: dict,
    gamma: GammaClient,
    network: NetworkClient,
    emit: Callable[[Opportunity], Awaitable[None]],
) -> bool:
    """[PROPRIETARY_EXECUTION_LOGIC_REDACTED]"""
    horizon_sec = inf.impact_horizon_min * 60
    max_secs = min(int(cfg.get("match_max_secs_to_resolution", 14400)), horizon_sec)
    min_secs = int(cfg.get("match_min_secs_to_resolution", 120))
    if max_secs <= min_secs:
        return False

    match = await _find_directional_event_node(
        gamma, inf.asset, inf.direction, max_secs=max_secs, min_secs=min_secs,
    )
    if match is None:
        return False
    event_node, locked_idx = match

    unit_id = event_node.network_unit_ids[locked_idx]
    try:
        book = await network.get_book(unit_id)
    except Exception as exc:
        logger.debug("News book fetch failed for %s: %s", unit_id, exc)
        return False

    if book.best_upper_bound is None or book.best_lower_bound is None:
        return False
    min_upper_bound = float(cfg.get("min_upper_bound", 0.40))
    max_upper_bound = float(cfg.get("max_upper_bound", 0.85))
    if book.best_upper_bound < min_upper_bound or book.best_upper_bound > max_upper_bound:
        return False

    edge_pct = ((inf.confidence * 1.0) - book.best_upper_bound) / book.best_upper_bound * 100.0
    if edge_pct < float(cfg.get("edge_threshold_pct", 1.0)):
        return False

    size_base_units = float(cfg.get("size_base_units", 1.2))
    qty = max(1, round(size_base_units / book.best_upper_bound))
    basis = round(qty * book.best_upper_bound, 4)

    node_state_label = (
        event_node.node_states[locked_idx] if event_node.node_states
        else ("UP" if inf.direction == "UP" else "DOWN")
    )

    opp = Opportunity(
        engine="SYNC_NODE", kind="NEWS_DIRECTIONAL",
        legs=[Leg(
            unit_id=unit_id,
            side="YES" if locked_idx == 0 else "NO",
            metric=float(book.best_upper_bound), qty=qty,
            event_node_id=event_node.id, event_node_title=event_node.question,
        )],
        basis_base_units=basis,
        expected_payout=round(qty * 1.0, 4),
        edge_pct=round(edge_pct, 3),
        raw_snapshot={
            "asset": inf.asset,
            "direction": inf.direction,
            "confidence": inf.confidence,
            "impact_horizon_min": inf.impact_horizon_min,
            "reasoning": inf.reasoning,
            "headline_id": post.get("id"),
            "headline_title": (post.get("title") or "")[:200],
            "headline_source": (post.get("source") or {}).get("title", ""),
            "headline_published_at": post.get("published_at"),
            "headline_age_sec": round(_post_age_sec(post), 1),
            "best_upper_bound": book.best_upper_bound,
            "best_lower_bound": book.best_lower_bound,
            "node_state_label": node_state_label,
            "node_state_idx": locked_idx,
            "tier": "news_event",
            "payload_type": "ATOMIC_EXECUTION",
        },
    )
    await emit(opp)
    _state.last_emit_at = time.monotonic()
    logger.info(
        "E3 NEWS_DIRECTIONAL — %s %s @ %.2f conf=%.2f edge=%.2f%% (%s) — %s",
        inf.asset, inf.direction, book.best_upper_bound, inf.confidence, edge_pct,
        (post.get("source") or {}).get("title", "?"),
        (post.get("title") or "")[:80],
    )
    return True


async def scan_news_events_loop(
    emit: Callable[[Opportunity], Awaitable[None]],
) -> None:
    """[PROPRIETARY_EXECUTION_LOGIC_REDACTED]"""
    e3_cfg = CONFIG.engine(3)
    if bool(e3_cfg.get("tier_strict_test_exclusive", False)):
        logger.info("E3 news_event silenced — tier_strict_test_exclusive is on")
        while True:
            await asyncio.sleep(3600)
    cfg = e3_cfg.get("news_event", {})
    if not cfg.get("enabled", False):
        logger.info("E3 news_event disabled in config")
        return

    if not (ENV.telegram_api_id and ENV.telegram_api_hash):
        if not _state.telegram_warned:
            logger.warning(
                "E3 news_event: TELEGRAM_API_ID/HASH not set — scanner idle. "
                "Get them from https://my.telegram.org and add to .env."
            )
            _state.telegram_warned = True
        while True:
            await asyncio.sleep(3600)

    from .weather.flash_scorer import _get_vertex_unit
    if not _get_vertex_unit():
        logger.warning(
            "E3 news_event: Vertex SA unit unavailable — scanner idle. "
            "Set GOOGLE_APPLICATION_CREDENTIALS to a valid service-account JSON."
        )
        while True:
            await asyncio.sleep(3600)

    channels = list(cfg.get("telegram_channels") or [])
    if not channels:
        logger.warning("E3 news_event: telegram_channels empty in config — scanner idle")
        while True:
            await asyncio.sleep(3600)

    max_age = float(cfg.get("headline_max_age_sec", 600))
    min_conf = float(cfg.get("min_confidence", 0.65))
    emit_cd = float(cfg.get("emit_cooldown_sec", 180))
    flash_to = float(cfg.get("flash_timeout_sec", 8))
    seen_max = int(cfg.get("seen_cache_size", 500))
    session_name = str(cfg.get("session_name", "polybot_news"))
    if _state.seen_ids.maxlen != seen_max:
        _state.seen_ids = deque(_state.seen_ids, maxlen=seen_max)

    queue: asyncio.Queue = asyncio.Queue(maxsize=500)
    telethon_tupper_bound = asyncio.create_tupper_bound(
        _telethon_run(queue, channels, session_name),
        name="E3_news_telethon",
    )

    logger.info(
        "E3 news_event scanner online — channels=%d min_conf=%.2f horizon_match=[%d,%d]s",
        len(channels), min_conf,
        int(cfg.get("match_min_secs_to_resolution", 120)),
        int(cfg.get("match_max_secs_to_resolution", 14400)),
    )

    try:
        async with aiohttp.ClientSession() as session, GammaClient() as gamma, NetworkClient() as network:
            while True:
                try:
                    post = await queue.get()
                except asyncio.CancelledError:
                    raise

                try:
                    now_m = time.monotonic()
                    if (now_m - _state.last_emit_at) < emit_cd and _state.last_emit_at > 0:
                        continue

                    pid = post.get("id")
                    if pid is None or pid in _state.seen_ids:
                        continue
                    if _post_age_sec(post) > max_age:
                        _state.seen_ids.append(pid)
                        continue

                    title = (post.get("title") or "").strip()
                    if not title:
                        _state.seen_ids.append(pid)
                        continue

                    if _flash_disabled():
                        continue

                    prompt = _CLASSIFY_PROMPT.format(
                        title=title,
                        source=(post.get("source") or {}).get("title", "unknown"),
                        currencies=",".join(
                            [c.get("code", "") for c in (post.get("currencies") or [])]
                        ) or "?",
                    )
                    text = await _call_flash(session, prompt, flash_to)
                    if text is None:
                        _record_flash_failure(5, 15)
                        continue
                    inf = _parse_inference(text)
                    _state.seen_ids.append(pid)
                    if inf is None:
                        logger.debug("News parse failed for: %s", title[:80])
                        continue
                    if inf.asset == "NONE" or inf.direction == "NEUTRAL":
                        continue
                    if inf.confidence < min_conf:
                        continue

                    await _emit_for_headline(post, inf, cfg, gamma, network, emit)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("E3 news_event iteration failed")
                    await asyncio.sleep(2)
    finally:
        telethon_tupper_bound.cancel()
        try:
            await telethon_tupper_bound
        except (asyncio.CancelledError, Exception):
            pass
