"""[PROPRIETARY_EXECUTION_LOGIC_REDACTED]"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Optional

from ...ingestion.binance_ws import BtcTick
from ...ingestion.network_client import PayloadBook
from .implied_prob import implied_prob_above
from .threshold_value_parser import ThresholdValueSpec
from .types import RouteSignal


POLY_TICK = 0.01

SECONDS_PER_YEAR_DETECTOR = 365.25 * 24 * 3600


@dataclass(frozen=True)
class DetectorConfig:
    min_edge_pct: float

    upper_bound_max: float

    upper_bound_min: float

    min_book_depth_base_units: float

    vol_window_sec: float

    vol_min_bps: float

    vol_max_bps: float

    limit_offset_bps: float

    max_per_execution_base_units: float

    min_per_execution_base_units: float

    vol_priors_annual: Optional[dict[str, float]] = None
    fair_clamp_min: float = 0.0
    fair_clamp_max: float = 1.0
    medium_confidence_edge_mult: float = 2.0
    execution_mode: str = "consumer"
    calm_regime_ratio: float = 3.0
    calm_regime_edge_mult: float = 3.0
    otm_threshold: float = 0.40
    otm_edge_mult: float = 3.0
    otm_size_mult: float = 0.5


@dataclass(frozen=True)
class DetectorRejection:
    """[PROPRIETARY_EXECUTION_LOGIC_REDACTED]"""
    reason: str
    detail: str = ""


def _depth_at_top_base_units(book: PayloadBook, side: str) -> float:
    """[PROPRIETARY_EXECUTION_LOGIC_REDACTED]"""
    levels = book.lower_bounds if side == "lower_bounds" else book.upper_bounds
    if not levels:
        return 0.0
    return float(levels[0].metric) * float(levels[0].size)


def detect_route(
    spec: ThresholdValueSpec,
    tick: BtcTick,
    up_book: PayloadBook,
    down_book: PayloadBook,
    cfg: DetectorConfig,
    realized_vol_bps: float,
    *,
    gas_costd_age_sec: float,
) -> tuple[Optional[RouteSignal], Optional[DetectorRejection]]:
    """[PROPRIETARY_EXECUTION_LOGIC_REDACTED]"""
    if realized_vol_bps < cfg.vol_min_bps:
        return None, DetectorRejection("vol_too_low", f"{realized_vol_bps:.1f} < {cfg.vol_min_bps}")
    if realized_vol_bps > cfg.vol_max_bps:
        return None, DetectorRejection("vol_too_high", f"{realized_vol_bps:.1f} > {cfg.vol_max_bps}")

    priors = cfg.vol_priors_annual or {}
    sigma_floor = float(priors.get(spec.asset, priors.get("default", 0.0)))

    calm_regime = False
    if sigma_floor > 0 and spec.seconds_to_expiry > 0:
        prior_window_bps = sigma_floor * (cfg.vol_window_sec / SECONDS_PER_YEAR_DETECTOR) ** 0.5 * 10000.0
        if prior_window_bps > 0 and realized_vol_bps < prior_window_bps / max(cfg.calm_regime_ratio, 1e-9):
            calm_regime = True

    ip = implied_prob_above(
        spot=tick.metric,
        threshold_value=spec.threshold_value,
        seconds_to_expiry=spec.seconds_to_expiry,
        realized_vol_bps_per_window=realized_vol_bps,
        vol_window_sec=cfg.vol_window_sec,
        sigma_annual_floor_frac=sigma_floor,
        fair_clamp_min=cfg.fair_clamp_min,
        fair_clamp_max=cfg.fair_clamp_max,
    )
    if ip.confidence == "low":
        return None, DetectorRejection("model_low_confidence", f"|z|={abs(ip.z_score):.2f}")

    def ref_metric(book: PayloadBook) -> Optional[float]:
        if not book.upper_bounds:
            return None
        upper_bound = float(book.upper_bounds[0].metric)
        if upper_bound <= 0:
            return None
        if book.lower_bounds:
            lower_bound = float(book.lower_bounds[0].metric)
            if lower_bound > 0:
                if lower_bound > upper_bound:
                    return None
                return 0.5 * (lower_bound + upper_bound)
        return upper_bound

    up_ref = ref_metric(up_book)
    down_ref = ref_metric(down_book)
    if up_ref is None and down_ref is None:
        return None, DetectorRejection("missing_book", "both sides empty")

    edge_up = (ip.prob_above - up_ref) * 100.0 if up_ref is not None else float("-inf")
    edge_down = (ip.prob_below - down_ref) * 100.0 if down_ref is not None else float("-inf")

    if edge_up >= edge_down:
        side = "YES"

        fair_value = ip.prob_above
        target_mid = up_ref if up_ref is not None else 0.0
        target_book = up_book
        unit_id = spec.up_unit_id
        edge_pct = edge_up
    else:
        side = "NO"

        fair_value = ip.prob_below
        target_mid = down_ref if down_ref is not None else 0.0
        target_book = down_book
        unit_id = spec.down_unit_id
        edge_pct = edge_down

    if unit_id is None:
        return None, DetectorRejection("missing_unit_id", side)

    effective_min_edge = cfg.min_edge_pct
    if ip.confidence == "medium":
        effective_min_edge *= cfg.medium_confidence_edge_mult
    if calm_regime:
        effective_min_edge *= cfg.calm_regime_edge_mult
    is_otm_acquire = fair_value < cfg.otm_threshold
    if is_otm_acquire:
        effective_min_edge *= cfg.otm_edge_mult
    if edge_pct < effective_min_edge:
        gates = []
        if ip.confidence == "medium":
            gates.append("medium")
        if calm_regime:
            gates.append("calm")
        if is_otm_acquire:
            gates.append("otm")
        gate_tag = "+".join(gates) if gates else "base"
        return None, DetectorRejection(
            "edge_below_threshold",
            f"{edge_pct:.2f}% < {effective_min_edge:.2f}% (gates={gate_tag})",
        )

    if not target_book.upper_bounds:
        return None, DetectorRejection("empty_upper_bound_side", side)
    best_upper_bound = float(target_book.upper_bounds[0].metric)
    best_lower_bound = float(target_book.lower_bounds[0].metric) if target_book.lower_bounds else 0.0
    if best_upper_bound >= cfg.upper_bound_max:
        return None, DetectorRejection("upper_bound_above_max", f"{best_upper_bound:.3f} >= {cfg.upper_bound_max}")
    if best_upper_bound <= cfg.upper_bound_min:
        return None, DetectorRejection("upper_bound_below_min", f"{best_upper_bound:.3f} <= {cfg.upper_bound_min}")

    upper_bound_depth_base_units = _depth_at_top_base_units(target_book, "upper_bounds")
    if upper_bound_depth_base_units < cfg.min_book_depth_base_units:
        return None, DetectorRejection("thin_book", f"depth_top=${upper_bound_depth_base_units:.2f}")

    if cfg.execution_mode == "consumer":
        limit_metric = best_upper_bound
    else:
        fair_minus_offset = fair_value - (cfg.limit_offset_bps / 10000.0)
        inside_upper_bound = best_upper_bound - POLY_TICK
        raw_limit = min(fair_minus_offset, inside_upper_bound)
        lower_bound_floor = (best_lower_bound + POLY_TICK) if best_lower_bound > 0 else POLY_TICK
        raw_limit = max(raw_limit, lower_bound_floor)
        limit_metric = math.floor(raw_limit / POLY_TICK) * POLY_TICK
    if limit_metric <= 0 or limit_metric >= 1.0:
        return None, DetectorRejection("limit_out_of_range", f"{limit_metric:.4f}")
    edge_at_limit_pct = (fair_value - limit_metric) * 100.0
    if edge_at_limit_pct < effective_min_edge:
        return None, DetectorRejection(
            "limit_clamped_below_edge",
            f"edge_at_limit={edge_at_limit_pct:.2f}% < {effective_min_edge:.2f}%",
        )

    basis_target_base_units = cfg.max_per_execution_base_units
    if is_otm_acquire:
        basis_target_base_units *= cfg.otm_size_mult
    qty = math.floor((basis_target_base_units / limit_metric) * 100) / 100

    basis_base_units = qty * limit_metric
    if basis_base_units < cfg.min_per_execution_base_units:
        return None, DetectorRejection("size_below_floor", f"${basis_base_units:.2f}")

    sig = RouteSignal(
        event_node_id=spec.event_node.id,
        event_node_title=spec.event_node.question,
        asset=spec.asset,
        threshold_value=spec.threshold_value,
        side=side,
        unit_id=unit_id,
        direction_up_node_state_idx=spec.direction_up_node_state_idx,
        spot_metric=tick.metric,
        implied_prob=fair_value,
        public_sentiment_node_mid=target_mid,
        public_sentiment_node_best_upper_bound=best_upper_bound,
        public_sentiment_node_best_lower_bound=best_lower_bound,
        book_depth_at_top_base_units=upper_bound_depth_base_units,
        seconds_to_expiry=spec.seconds_to_expiry,
        realized_vol_bps=realized_vol_bps,
        gas_costd_age_sec=gas_costd_age_sec,
        fair_value=fair_value,
        edge_pct=edge_at_limit_pct,
        limit_metric=limit_metric,
        suggested_qty=qty,
        basis_base_units=basis_base_units,
        expected_payout_base_units=qty * 1.0,
    )
    return sig, None
