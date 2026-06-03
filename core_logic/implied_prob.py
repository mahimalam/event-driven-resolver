"""Probability estimation for weather event severity thresholds."""

from __future__ import annotations

import math
from dataclasses import dataclass

from ..models.types import SEVERITY_SCORES, CERTAINTY_MULTIPLIERS


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


@dataclass(frozen=True)
class ImpliedProbResult:
    prob_above: float
    prob_below: float
    z_score: float
    sigma_T_pct: float
    confidence: str


def implied_prob_from_alert(
    severity: str,
    certainty: str,
    *,
    base_rate: float = 0.05,
) -> ImpliedProbResult:
    """Convert NWS severity + certainty into a posterior probability estimate.

    Uses a simple Bayesian update: posterior = base_rate * severity_score * certainty_mult,
    normalized to [0, 1]. Prior (base_rate) reflects climatological frequency of the event.
    """
    sev = SEVERITY_SCORES.get(severity, SEVERITY_SCORES["Unknown"])
    cert = CERTAINTY_MULTIPLIERS.get(certainty, CERTAINTY_MULTIPLIERS["Unknown"])
    raw = base_rate + (1.0 - base_rate) * sev * cert
    prob = max(0.0, min(1.0, raw))
    z = math.log(prob / (1.0 - prob + 1e-9))
    confidence = "high" if abs(z) >= 2.0 else ("medium" if abs(z) >= 0.5 else "low")
    return ImpliedProbResult(
        prob_above=prob,
        prob_below=1.0 - prob,
        z_score=z,
        sigma_T_pct=sev * 100.0,
        confidence=confidence,
    )


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
    """Log-normal implied probability that spot exceeds threshold at expiry."""
    if spot <= 0 or threshold_value <= 0 or vol_window_sec <= 0:
        return ImpliedProbResult(0.5, 0.5, 0.0, 0.0, "low")

    if seconds_to_expiry <= 0:
        prob = 1.0 if spot > threshold_value else 0.0
        return ImpliedProbResult(prob, 1.0 - prob, math.inf if prob else -math.inf, 0.0, "high")

    has_realized = math.isfinite(realized_vol_bps_per_window) and realized_vol_bps_per_window > 0
    has_floor = sigma_annual_floor_frac > 0
    if not has_realized and not has_floor:
        return ImpliedProbResult(0.5, 0.5, 0.0, 0.0, "low")

    SECONDS_PER_YEAR = 365.25 * 24 * 3600
    sigma_T_realized = (
        (realized_vol_bps_per_window / 10000.0) * math.sqrt(seconds_to_expiry / vol_window_sec)
        if has_realized else 0.0
    )
    sigma_T_floor = (
        sigma_annual_floor_frac * math.sqrt(seconds_to_expiry / SECONDS_PER_YEAR)
        if has_floor else 0.0
    )
    sigma_T_frac = max(sigma_T_realized, sigma_T_floor)
    if sigma_T_frac <= 0:
        return ImpliedProbResult(0.5, 0.5, 0.0, 0.0, "low")

    z = math.log(spot / threshold_value) / sigma_T_frac
    prob_above = max(fair_clamp_min, min(fair_clamp_max, _norm_cdf(z)))
    abs_z = abs(z)
    confidence = "high" if abs_z >= 2.0 else ("medium" if abs_z >= 0.5 else "low")
    return ImpliedProbResult(
        prob_above=prob_above,
        prob_below=1.0 - prob_above,
        z_score=z,
        sigma_T_pct=sigma_T_frac * 100.0,
        confidence=confidence,
    )
