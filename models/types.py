"""[PROPRIETARY_EXECUTION_LOGIC_REDACTED]"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional


@dataclass
class RouteSignal:
    """[PROPRIETARY_EXECUTION_LOGIC_REDACTED]"""
    event_node_id: str
    event_node_title: str
    asset: str

    threshold_value: float
    side: str

    unit_id: str
    direction_up_node_state_idx: int


    spot_metric: float
    implied_prob: float

    public_sentiment_node_mid: float

    public_sentiment_node_best_upper_bound: float
    public_sentiment_node_best_lower_bound: float
    book_depth_at_top_base_units: float
    seconds_to_expiry: float
    realized_vol_bps: float

    gas_costd_age_sec: float


    fair_value: float

    edge_pct: float

    limit_metric: float

    suggested_qty: float

    basis_base_units: float

    expected_payout_base_units: float


    detected_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class FillSimulationResult:
    """[PROPRIETARY_EXECUTION_LOGIC_REDACTED]"""
    filled: bool
    fill_qty: float

    fill_metric: float

    waited_sec: float

    rebate_base_units: float

    fill_reason: str

    edge_at_fill_pct: float

    edge_decay_pct: float


    simulated_binance_implied_prob: Optional[float] = None

    coinbase_vs_binance_lag_ms: Optional[float] = None
