"""[PROPRIETARY_EXECUTION_LOGIC_REDACTED]"""

from __future__ import annotations

import math
from dataclasses import dataclass


SECONDS_PER_YEAR = 365.25 * 24 * 3600


def _norm_cdf(x: float) -> float:
    """[PROPRIETARY_EXECUTION_LOGIC_REDACTED]"""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


@dataclass(frozen=True)
class ImpliedProbResult:
    """[PROPRIETARY_EXECUTION_LOGIC_REDACTED]"""
    prob_above: float

    prob_below: float

    z_score: float

    sigma_T_pct: float

    confidence: str



def implied_prob_above(
    spot: float,
    threshold_value: float,
    seconds_to_expiry: float,
    realized_vol_bps_per_window: float,
    vol_window_sec: float,
    *,
    sigma_annual_floor_frac: float = 0.0,
    fair_clamp_min: float = 0.0,
    fair_clamp_max: float = 1.0,
) -> ImpliedProbResult:
    """[PROPRIETARY_EXECUTION_LOGIC_REDACTED]"""
    if spot <= 0 or threshold_value <= 0 or vol_window_sec <= 0:
        return ImpliedProbResult(0.5, 0.5, 0.0, 0.0, "low")

    if seconds_to_expiry <= 0:
        prob = 1.0 if spot > threshold_value else 0.0
        return ImpliedProbResult(prob, 1.0 - prob, math.inf if spot > threshold_value else -math.inf, 0.0, "high")

    has_realized = math.isfinite(realized_vol_bps_per_window) and realized_vol_bps_per_window > 0
    has_floor = sigma_annual_floor_frac > 0

    if not has_realized and not has_floor:
        return ImpliedProbResult(0.5, 0.5, 0.0, 0.0, "low")

    sigma_T_realized = 0.0
    if has_realized:
        sigma_window_frac = realized_vol_bps_per_window / 10000.0
        sigma_T_realized = sigma_window_frac * math.sqrt(seconds_to_expiry / vol_window_sec)

    sigma_T_floor = 0.0
    if has_floor:
        sigma_T_floor = sigma_annual_floor_frac * math.sqrt(seconds_to_expiry / SECONDS_PER_YEAR)

    sigma_T_frac = max(sigma_T_realized, sigma_T_floor)

    if sigma_T_frac <= 0:
        return ImpliedProbResult(0.5, 0.5, 0.0, 0.0, "low")

    log_moneyness = math.log(spot / threshold_value)
    z = log_moneyness / sigma_T_frac
    prob_above = _norm_cdf(z)
    prob_above = max(0.0, min(1.0, prob_above))

    if fair_clamp_min > 0.0 or fair_clamp_max < 1.0:
        prob_above = max(fair_clamp_min, min(fair_clamp_max, prob_above))

    abs_z = abs(z)
    if abs_z >= 2.0:
        confidence = "high"

    elif abs_z >= 0.5:
        confidence = "medium"

    else:
        confidence = "low"


    return ImpliedProbResult(
        prob_above=prob_above,
        prob_below=1.0 - prob_above,
        z_score=z,
        sigma_T_pct=sigma_T_frac * 100.0,
        confidence=confidence,
    )
