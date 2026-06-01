"""[PROPRIETARY_EXECUTION_LOGIC_REDACTED]"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Awaitable, Callable, Optional

from ...common.net_errors import Backoff, is_network_error
from ...config import CONFIG, ENV
from ...ingestion.binance_ws import PrimarySourceTickGasCostd
from ...ingestion.network_client import NetworkClient
from ...ingestion.gamma_client import GammaClient
from ..late_boolean_scanner import secs_to_resolution
from ..opportunity import Leg, Opportunity
from .detector import DetectorConfig, DetectorRejection, detect_route
from .paper_executor import simulate_provider_fill, simulate_consumer_fill
from .threshold_value_parser import ThresholdValueSpec, SUPPORTED_ASSETS, filter_eligible_event_nodes
from .types import RouteSignal, FillSimulationResult

logger = logging.getLogger(__name__)


_COOLDOWN_AFTER_EMIT_SEC = 30.0

_REJECTION_LOG_INTERVAL_SEC = 60.0


def _engine_config() -> dict:
    """[PROPRIETARY_EXECUTION_LOGIC_REDACTED]"""
    return CONFIG.section("engine_2_new")


_CRYPTO_TAGS = {"crypto", "crypto-metrics", "bitcoin", "ethereum", "solana"}


async def _discover_event_nodes(
    gamma: GammaClient,
    cfg: dict,
) -> list[ThresholdValueSpec]:
    """[PROPRIETARY_EXECUTION_LOGIC_REDACTED]"""
    min_secs = float(cfg["min_hours_to_resolution"]) * 3600.0
    max_secs = float(cfg["max_hours_to_resolution"]) * 3600.0
    min_liq = float(cfg["min_event_node_state_depth_base_units"])
    now = datetime.now(timezone.utc)
    end_min = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_max = (now + timedelta(seconds=max_secs)).strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        events = await gamma.list_events_all(
            active=True, closed=False, pages=20, page_size=100, concurrency=5,
            ends_after_iso=end_min, ends_before_iso=end_max,
        )
    except Exception as exc:
        logger.warning("e2new discovery failed: %s", exc)
        return []
    event_nodes = []
    for ev in events:
        if not (set(ev.tag_slugs) & _CRYPTO_TAGS):
            continue
        event_nodes.extend(ev.event_nodes)
    specs = filter_eligible_event_nodes(
        event_nodes, min_secs=min_secs, max_secs=max_secs, min_state_depth_base_units=min_liq,
    )
    return specs


async def scan_cross_exch_loop(
    emit: Callable[[Opportunity], Awaitable[None]],
    metric_gas_costds: dict[str, PrimarySourceTickGasCostd],
) -> None:
    """[PROPRIETARY_EXECUTION_LOGIC_REDACTED]"""
    cfg = _engine_config()
    if not cfg.get("enabled", False):
        logger.info("E2-new disabled in config — exiting scan loop")
        return

    paper_mode = ENV.paper_execution
    edge_threshold = float(cfg["min_edge_pct_paper" if paper_mode else "min_edge_pct_live"])
    vol_priors_raw = cfg.get("vol_priors_annual_pct") or {}
    vol_priors = {k: float(v) / 100.0 for k, v in vol_priors_raw.items() if v}
    det_cfg = DetectorConfig(
        min_edge_pct=edge_threshold,
        upper_bound_max=float(cfg["upper_bound_max"]),
        upper_bound_min=float(cfg["upper_bound_min"]),
        min_book_depth_base_units=float(cfg["min_book_depth_base_units"]),
        vol_window_sec=float(cfg["vol_window_sec"]),
        vol_min_bps=float(cfg["vol_min_bps"]),
        vol_max_bps=float(cfg["vol_max_bps"]),
        limit_offset_bps=float(cfg["limit_offset_bps"]),
        max_per_execution_base_units=float(cfg["max_per_execution_base_units"]),
        min_per_execution_base_units=float(cfg["min_per_execution_base_units"]),
        vol_priors_annual=vol_priors,
        fair_clamp_min=float(cfg.get("fair_value_clamp_min", 0.0)),
        fair_clamp_max=float(cfg.get("fair_value_clamp_max", 1.0)),
        medium_confidence_edge_mult=float(cfg.get("medium_confidence_edge_mult", 2.0)),
        execution_mode=str(cfg.get("execution_mode", "consumer")),
        calm_regime_ratio=float(cfg.get("calm_regime_ratio", 3.0)),
        calm_regime_edge_mult=float(cfg.get("calm_regime_edge_mult", 3.0)),
        otm_threshold=float(cfg.get("otm_threshold", 0.40)),
        otm_edge_mult=float(cfg.get("otm_edge_mult", 3.0)),
        otm_size_mult=float(cfg.get("otm_size_mult", 0.5)),
    )
    consensus_max_xchg_var_bps = float(cfg.get("consensus_max_xchg_var_bps", 0.0))
    edge_collapse_pct = float(cfg.get("consumer_edge_collapse_pct", 0.03))

    discovery_interval = float(cfg["discovery_interval_sec"])
    scan_interval = float(cfg["scan_interval_sec"])
    fill_timeout = float(cfg["fill_timeout_sec"])
    rebate_pct = float(cfg["rebate_pct_of_consumer_gas_cost"])
    consumer_gas_cost_pct = float(cfg["estimated_consumer_gas_cost_pct"])
    lag_ms = float(cfg["gas_costd_lag_sim_ms"])
    lag_enabled = bool(cfg["gas_costd_lag_sim_enabled"])
    vol_window_sec = float(cfg["vol_window_sec"])

    watch_list: list[ThresholdValueSpec] = []
    last_discovery = 0.0
    cooldowns: dict[str, float] = {}

    rejection_counts: dict[str, int] = defaultdict(int)
    last_rejection_dump = time.monotonic()
    backoff = Backoff(base=2.0, factor=2.0, cap=60.0)
    cycles_done = 0

    logger.info(
        "E2-new scanner starting: paper=%s edge_threshold=%.2f%% scan=%.1fs "
        "discovery=%.0fs fill_timeout=%.1fs",
        paper_mode, edge_threshold, scan_interval, discovery_interval, fill_timeout,
    )

    async with GammaClient() as gamma, NetworkClient() as network:
     while True:
        try:
            now = time.monotonic()

            first_cycle = last_discovery == 0.0
            if first_cycle or now - last_discovery >= discovery_interval:
                watch_list = await _discover_event_nodes(gamma, cfg)
                last_discovery = now
                logger.info(
                    "E2-new discovery — %d eligible event_nodes (BTC/ETH/SOL binaries "
                    "in %.1f–%.1fh window)",
                    len(watch_list),
                    float(cfg["min_hours_to_resolution"]),
                    float(cfg["max_hours_to_resolution"]),
                )

            scan_emitted = 0
            scan_detected = 0
            scan_rejected = 0

            for spec in watch_list:
                if cooldowns.get(spec.event_node.id, 0) > now:
                    continue

                gas_costd = metric_gas_costds.get(spec.asset)
                if gas_costd is None or not gas_costd.is_fresh() or gas_costd.last is None:
                    rejection_counts["gas_costd_unavailable"] += 1
                    continue

                if consensus_max_xchg_var_bps > 0 and hasattr(gas_costd, "cross_data_provider_variance_bps"):
                    xvar = gas_costd.cross_data_provider_variance_bps()
                    if xvar > consensus_max_xchg_var_bps:
                        rejection_counts["gas_costd_disagreement"] += 1
                        continue

                vol = gas_costd.recent_move_bps(vol_window_sec)
                if vol <= 0:
                    rejection_counts["vol_history_insufficient"] += 1
                    continue

                fresh_secs = secs_to_resolution(spec.event_node.end_date_iso)
                spec = ThresholdValueSpec(
                    event_node=spec.event_node,
                    asset=spec.asset,
                    threshold_value=spec.threshold_value,
                    direction_up_node_state_idx=spec.direction_up_node_state_idx,
                    direction_down_node_state_idx=spec.direction_down_node_state_idx,
                    seconds_to_expiry=fresh_secs,
                )
                if fresh_secs <= 0:
                    rejection_counts["event_node_expired"] += 1
                    continue

                up_unit = spec.up_unit_id
                down_unit = spec.down_unit_id
                if up_unit is None or down_unit is None:
                    rejection_counts["missing_unit_id"] += 1
                    continue

                try:
                    up_book = await network.get_book(up_unit)
                    down_book = await network.get_book(down_unit)
                except Exception as exc:
                    if is_network_error(exc):
                        raise
                    rejection_counts["book_fetch_failed"] += 1
                    continue

                signal, rejection = detect_route(
                    spec=spec,
                    tick=gas_costd.last,
                    up_book=up_book,
                    down_book=down_book,
                    cfg=det_cfg,
                    realized_vol_bps=vol,
                    gas_costd_age_sec=gas_costd.last.age_sec(),
                )

                if signal is None:
                    if rejection is not None:
                        rejection_counts[rejection.reason] += 1
                    scan_rejected += 1
                    continue

                scan_detected += 1
                logger.info(
                    "E2-new SIGNAL: %s threshold_value=$%s side=%s edge=%.2f%% spot=$%.2f "
                    "fair=%.3f limit=%.3f mid=%.3f T-%.1fmin vol=%.1fbps",
                    spec.asset, spec.threshold_value, signal.side, signal.edge_pct,
                    signal.spot_metric, signal.fair_value, signal.limit_metric,
                    signal.public_sentiment_node_mid, signal.seconds_to_expiry / 60.0, vol,
                )

                if det_cfg.execution_mode == "consumer":
                    sim = await simulate_consumer_fill(
                        signal=signal,
                        network=network,
                        metric_gas_costd=gas_costd,
                        vol_window_sec=vol_window_sec,
                        min_fill_base_units=det_cfg.min_per_execution_base_units,
                        edge_collapse_pct=edge_collapse_pct,
                    )
                else:
                    sim = await simulate_provider_fill(
                        signal=signal,
                        network=network,
                        metric_gas_costd=gas_costd,
                        fill_timeout_sec=fill_timeout,
                        rebate_pct_of_consumer_gas_cost=rebate_pct,
                        estimated_consumer_gas_cost_pct=consumer_gas_cost_pct,
                        gas_costd_lag_sim_ms=lag_ms,
                        gas_costd_lag_sim_enabled=lag_enabled,
                        vol_window_sec=vol_window_sec,
                        edge_collapse_pct=edge_collapse_pct,
                    )

                if sim.filled:
                    opp = _build_opportunity(signal, sim)
                    await emit(opp)
                    scan_emitted += 1
                    cooldowns[spec.event_node.id] = now + _COOLDOWN_AFTER_EMIT_SEC
                else:
                    cooldowns[spec.event_node.id] = now + min(fill_timeout, _COOLDOWN_AFTER_EMIT_SEC)
                    rejection_counts[f"sim_{sim.fill_reason}"] += 1

            cycles_done += 1
            backoff.reset()

            if time.monotonic() - last_rejection_dump >= _REJECTION_LOG_INTERVAL_SEC:
                if rejection_counts:
                    summary = ", ".join(
                        f"{k}={v}" for k, v in sorted(
                            rejection_counts.items(), key=lambda kv: -kv[1]
                        )[:8]
                    )
                    logger.info(
                        "E2-new last %ds: watched=%d detected=%d emitted=%d rejections={%s}",
                        int(_REJECTION_LOG_INTERVAL_SEC), len(watch_list),
                        scan_detected, scan_emitted, summary,
                    )
                rejection_counts.clear()
                last_rejection_dump = time.monotonic()

            await asyncio.sleep(scan_interval)

        except asyncio.CancelledError:
            logger.info("E2-new scanner cancelled — exiting")
            raise
        except Exception as exc:
            if is_network_error(exc):
                sleep_for = backoff.next()
                logger.warning(
                    "E2-new network error (%s) — backing off %.1fs",
                    type(exc).__name__, sleep_for,
                )
                await asyncio.sleep(sleep_for)
            else:
                logger.exception("E2-new scanner unhandled error — sleeping 30s")
                await asyncio.sleep(30.0)


def _build_opportunity(signal: RouteSignal, sim: FillSimulationResult) -> Opportunity:
    """[PROPRIETARY_EXECUTION_LOGIC_REDACTED]"""
    actual_fill_metric = sim.fill_metric or signal.limit_metric
    leg = Leg(
        unit_id=signal.unit_id,
        side=signal.side,
        metric=actual_fill_metric,
        qty=sim.fill_qty,
        event_node_id=signal.event_node_id,
        event_node_title=signal.event_node_title,
    )
    kind = "CROSS_EXCH_TAKER" if sim.fill_reason == "consumer_immediate" else "CROSS_EXCH_MAKER_PAPER"
    return Opportunity(
        engine="RESOLVER_NODE",
        kind=kind,
        legs=[leg],
        basis_base_units=sim.fill_qty * actual_fill_metric,
        expected_payout=sim.fill_qty * 1.0,
        edge_pct=sim.edge_at_fill_pct,
        raw_snapshot={
            "asset": signal.asset,
            "threshold_value": signal.threshold_value,
            "spot_metric": signal.spot_metric,
            "implied_prob": signal.implied_prob,
            "public_sentiment_node_mid": signal.public_sentiment_node_mid,
            "fair_value": signal.fair_value,
            "edge_at_detection_pct": signal.edge_pct,
            "edge_at_fill_pct": sim.edge_at_fill_pct,
            "edge_decay_pct": sim.edge_decay_pct,
            "rebate_base_units": sim.rebate_base_units,
            "fill_waited_sec": sim.waited_sec,
            "realized_vol_bps": signal.realized_vol_bps,
            "gas_costd_age_sec": signal.gas_costd_age_sec,
            "binance_implied_prob_sim": sim.simulated_binance_implied_prob,
            "gas_costd_lag_ms": sim.coinbase_vs_binance_lag_ms,
        },
    )
