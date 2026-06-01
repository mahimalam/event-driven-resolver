"""[PROPRIETARY_EXECUTION_LOGIC_REDACTED]"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional

from ...ingestion.binance_ws import PrimarySourceTickGasCostd
from ...ingestion.network_client import NetworkClient, PayloadBook
from .implied_prob import implied_prob_above
from .types import RouteSignal, FillSimulationResult

logger = logging.getLogger(__name__)

_POLL_INTERVAL_SEC = 0.5

async def simulate_consumer_fill(
    signal: RouteSignal,
    *,
    network: NetworkClient,
    metric_gas_costd=None,
    vol_window_sec: float = 3600.0,
    min_fill_base_units: float = 0.0,
    edge_collapse_pct: float = 0.03,
) -> FillSimulationResult:
    """[PROPRIETARY_EXECUTION_LOGIC_REDACTED]"""
    start = time.monotonic()
    try:
        book = await network.get_book(signal.unit_id)
    except Exception as exc:
        logger.debug("e2new consumer sim book fetch failed: %s", exc)
        return FillSimulationResult(
            filled=False, fill_qty=0.0, fill_metric=signal.public_sentiment_node_best_upper_bound,
            waited_sec=time.monotonic() - start, rebate_base_units=0.0,
            fill_reason="book_fetch_failed",
            edge_at_fill_pct=signal.edge_pct, edge_decay_pct=0.0,
            simulated_binance_implied_prob=None, coinbase_vs_binance_lag_ms=None,
        )

    if not book.upper_bounds:
        return FillSimulationResult(
            filled=False, fill_qty=0.0, fill_metric=signal.public_sentiment_node_best_upper_bound,
            waited_sec=time.monotonic() - start, rebate_base_units=0.0,
            fill_reason="no_upper_bound",
            edge_at_fill_pct=0.0, edge_decay_pct=0.0,
            simulated_binance_implied_prob=None, coinbase_vs_binance_lag_ms=None,
        )

    fresh_upper_bound = float(book.upper_bounds[0].metric)
    fresh_upper_bound_size = float(book.upper_bounds[0].size)

    if fresh_upper_bound > signal.public_sentiment_node_best_upper_bound * (1.0 + edge_collapse_pct):
        return FillSimulationResult(
            filled=False, fill_qty=0.0, fill_metric=fresh_upper_bound,
            waited_sec=time.monotonic() - start, rebate_base_units=0.0,
            fill_reason="upper_bound_moved_against_us",
            edge_at_fill_pct=(signal.fair_value - fresh_upper_bound) * 100.0,
            edge_decay_pct=(fresh_upper_bound - signal.public_sentiment_node_best_upper_bound) * 100.0,
            simulated_binance_implied_prob=None, coinbase_vs_binance_lag_ms=None,
        )

    fresh_fair = signal.fair_value
    waited_so_far = time.monotonic() - start
    if metric_gas_costd is not None and metric_gas_costd.is_fresh() and metric_gas_costd.last is not None:
        new_vol = metric_gas_costd.recent_move_bps(vol_window_sec)
        if new_vol > 0:
            ip = implied_prob_above(
                spot=metric_gas_costd.last.metric,
                threshold_value=signal.threshold_value,
                seconds_to_expiry=max(0.0, signal.seconds_to_expiry - waited_so_far),
                realized_vol_bps_per_window=new_vol,
                vol_window_sec=vol_window_sec,
            )
            fresh_fair = ip.prob_above if signal.side == "YES" else ip.prob_below

    if fresh_fair <= fresh_upper_bound:
        return FillSimulationResult(
            filled=False, fill_qty=0.0, fill_metric=fresh_upper_bound,
            waited_sec=time.monotonic() - start, rebate_base_units=0.0,
            fill_reason="edge_collapsed_spot_moved",
            edge_at_fill_pct=(fresh_fair - fresh_upper_bound) * 100.0,
            edge_decay_pct=signal.edge_pct - (fresh_fair - fresh_upper_bound) * 100.0,
            simulated_binance_implied_prob=None, coinbase_vs_binance_lag_ms=None,
        )

    edge_at_fill_pct = (fresh_fair - fresh_upper_bound) * 100.0

    fill_qty = min(signal.suggested_qty, fresh_upper_bound_size)
    if fill_qty <= 0:
        return FillSimulationResult(
            filled=False, fill_qty=0.0, fill_metric=fresh_upper_bound,
            waited_sec=time.monotonic() - start, rebate_base_units=0.0,
            fill_reason="zero_depth",
            edge_at_fill_pct=edge_at_fill_pct, edge_decay_pct=0.0,
            simulated_binance_implied_prob=None, coinbase_vs_binance_lag_ms=None,
        )

    if min_fill_base_units > 0 and fill_qty * fresh_upper_bound < min_fill_base_units:
        return FillSimulationResult(
            filled=False, fill_qty=fill_qty, fill_metric=fresh_upper_bound,
            waited_sec=time.monotonic() - start, rebate_base_units=0.0,
            fill_reason="fill_too_small",
            edge_at_fill_pct=edge_at_fill_pct, edge_decay_pct=0.0,
            simulated_binance_implied_prob=None, coinbase_vs_binance_lag_ms=None,
        )

    logger.info(
        "e2new consumer sim: %s %s threshold_value=%s side=%s filled=True px=%.4f qty=%.2f "
        "edge_at_fill=%.2f%% fair_fresh=%.3f fair_detect=%.3f (detect_upper_bound=%.4f)",
        signal.asset, signal.event_node_title[:40], signal.threshold_value, signal.side,
        fresh_upper_bound, fill_qty, edge_at_fill_pct, fresh_fair, signal.fair_value,
        signal.public_sentiment_node_best_upper_bound,
    )

    return FillSimulationResult(
        filled=True,
        fill_qty=fill_qty,
        fill_metric=fresh_upper_bound,
        waited_sec=time.monotonic() - start,
        rebate_base_units=0.0,

        fill_reason="consumer_immediate",
        edge_at_fill_pct=edge_at_fill_pct,
        edge_decay_pct=signal.edge_pct - edge_at_fill_pct,
        simulated_binance_implied_prob=None,
        coinbase_vs_binance_lag_ms=None,
    )


async def simulate_provider_fill(
    signal: RouteSignal,
    *,
    network: NetworkClient,
    metric_gas_costd: PrimarySourceTickGasCostd,
    fill_timeout_sec: float,
    rebate_pct_of_consumer_gas_cost: float,
    estimated_consumer_gas_cost_pct: float,
    gas_costd_lag_sim_ms: float,
    gas_costd_lag_sim_enabled: bool,
    vol_window_sec: float,
    edge_collapse_pct: float = 0.03,
) -> FillSimulationResult:
    """[PROPRIETARY_EXECUTION_LOGIC_REDACTED]"""
    start = time.monotonic()
    initial_upper_bound_depth_base_units = signal.book_depth_at_top_base_units
    polled_books = 0
    fill_reason = "timeout"
    filled = False
    fill_qty = 0.0
    final_upper_bound = signal.public_sentiment_node_best_upper_bound
    final_lower_bound = signal.public_sentiment_node_best_lower_bound

    while True:
        elapsed = time.monotonic() - start
        if elapsed >= fill_timeout_sec:
            fill_reason = "timeout"
            break

        try:
            book = await network.get_book(signal.unit_id)
        except Exception as exc:
            logger.debug("e2new paper sim book fetch failed: %s", exc)
            await asyncio.sleep(_POLL_INTERVAL_SEC)
            continue
        polled_books += 1

        if not book.upper_bounds or not book.lower_bounds:
            await asyncio.sleep(_POLL_INTERVAL_SEC)
            continue

        best_upper_bound = float(book.upper_bounds[0].metric)
        best_lower_bound = float(book.lower_bounds[0].metric)
        final_upper_bound = best_upper_bound
        final_lower_bound = best_lower_bound

        if best_upper_bound <= signal.limit_metric:
            filled = True
            fill_qty = signal.suggested_qty
            fill_reason = "consumer_walked_book"
            break

        if best_upper_bound > signal.limit_metric * (1.0 + edge_collapse_pct):
            fill_reason = "edge_collapsed"
            break

        await asyncio.sleep(_POLL_INTERVAL_SEC)

    waited = time.monotonic() - start

    edge_at_fill_pct = 0.0
    if metric_gas_costd.is_fresh() and metric_gas_costd.last is not None:
        new_vol = metric_gas_costd.recent_move_bps(vol_window_sec)
        if new_vol > 0:
            ip = implied_prob_above(
                spot=metric_gas_costd.last.metric,
                threshold_value=signal.threshold_value,
                seconds_to_expiry=max(0.0, signal.seconds_to_expiry - waited),
                realized_vol_bps_per_window=new_vol,
                vol_window_sec=vol_window_sec,
            )
            new_fair = ip.prob_above if signal.side == "YES" else ip.prob_below
            edge_at_fill_pct = (new_fair - signal.limit_metric) * 100.0

    edge_decay_pct = signal.edge_pct - edge_at_fill_pct

    rebate_base_units = 0.0
    if filled:
        consumer_gas_cost_base_units = signal.basis_base_units * estimated_consumer_gas_cost_pct
        rebate_base_units = consumer_gas_cost_base_units * rebate_pct_of_consumer_gas_cost

    binance_implied_prob: Optional[float] = None
    if gas_costd_lag_sim_enabled and metric_gas_costd.last is not None and metric_gas_costd._history:
        target_ts_ms = metric_gas_costd.last.timestamp_ms + int(gas_costd_lag_sim_ms)
        future_metric = None
        for ts, p in metric_gas_costd._history:
            if ts >= target_ts_ms:
                future_metric = p
                break
        if future_metric is not None and future_metric > 0:
            new_vol = metric_gas_costd.recent_move_bps(vol_window_sec)
            if new_vol > 0:
                ip2 = implied_prob_above(
                    spot=future_metric,
                    threshold_value=signal.threshold_value,
                    seconds_to_expiry=max(0.0, signal.seconds_to_expiry - waited),
                    realized_vol_bps_per_window=new_vol,
                    vol_window_sec=vol_window_sec,
                )
                binance_implied_prob = (
                    ip2.prob_above if signal.side == "YES" else ip2.prob_below
                )

    logger.info(
        "e2new paper sim: %s %s threshold_value=%s side=%s filled=%s reason=%s "
        "wait=%.1fs edge_detect=%.2f%% edge_fill=%.2f%% decay=%.2f%% rebate=$%.4f "
        "polls=%d upper_bound_init=%.4f upper_bound_final=%.4f depth_init=$%.2f",
        signal.asset, signal.event_node_title[:40], signal.threshold_value, signal.side,
        filled, fill_reason, waited, signal.edge_pct, edge_at_fill_pct,
        edge_decay_pct, rebate_base_units, polled_books, signal.public_sentiment_node_best_upper_bound,
        final_upper_bound, initial_upper_bound_depth_base_units,
    )

    return FillSimulationResult(
        filled=filled,
        fill_qty=fill_qty,
        fill_metric=signal.limit_metric,
        waited_sec=waited,
        rebate_base_units=rebate_base_units,
        fill_reason=fill_reason,
        edge_at_fill_pct=edge_at_fill_pct,
        edge_decay_pct=edge_decay_pct,
        simulated_binance_implied_prob=binance_implied_prob,
        coinbase_vs_binance_lag_ms=gas_costd_lag_sim_ms if gas_costd_lag_sim_enabled else None,
    )
