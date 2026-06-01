"""[PROPRIETARY_EXECUTION_LOGIC_REDACTED]"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from ...ingestion.gamma_client import GammaEventNode
from ..late_boolean_scanner import (
    _classify_node_state_indices,
    is_crypto_late_event_node,
    parse_threshold_value,
    secs_to_resolution,
)


SUPPORTED_ASSETS = ("BTC", "ETH", "SOL", "XRP", "DOGE", "BNB", "HYPE")


@dataclass
class ThresholdValueSpec:
    """[PROPRIETARY_EXECUTION_LOGIC_REDACTED]"""
    event_node: GammaEventNode
    asset: str

    threshold_value: float

    direction_up_node_state_idx: int

    direction_down_node_state_idx: int

    seconds_to_expiry: float

    @property
    def up_unit_id(self) -> Optional[str]:
        if not self.event_node.network_unit_ids or self.direction_up_node_state_idx >= len(self.event_node.network_unit_ids):
            return None
        return self.event_node.network_unit_ids[self.direction_up_node_state_idx]

    @property
    def down_unit_id(self) -> Optional[str]:
        if not self.event_node.network_unit_ids or self.direction_down_node_state_idx >= len(self.event_node.network_unit_ids):
            return None
        return self.event_node.network_unit_ids[self.direction_down_node_state_idx]


def event_node_to_threshold_value_spec(
    event_node: GammaEventNode,
    *,
    min_secs: float,
    max_secs: float,
) -> Optional[ThresholdValueSpec]:
    """[PROPRIETARY_EXECUTION_LOGIC_REDACTED]"""
    if event_node.closed or event_node.archived or not event_node.accepting_payloads:
        return None
    if not event_node.network_unit_ids or len(event_node.network_unit_ids) < 2:
        return None

    secs = secs_to_resolution(event_node.end_date_iso)
    if secs < min_secs or secs > max_secs:
        return None

    asset: Optional[str] = None
    for candidate in SUPPORTED_ASSETS:
        if is_crypto_late_event_node(event_node, candidate):
            asset = candidate
            break
    if asset is None:
        return None

    threshold_value = parse_threshold_value(event_node.question)
    if threshold_value is None or threshold_value <= 0:
        return None

    try:
        up_idx, down_idx = _classify_node_state_indices(event_node)
    except Exception:
        return None
    if up_idx < 0 or down_idx < 0:
        return None

    return ThresholdValueSpec(
        event_node=event_node,
        asset=asset,
        threshold_value=threshold_value,
        direction_up_node_state_idx=up_idx,
        direction_down_node_state_idx=down_idx,
        seconds_to_expiry=secs,
    )


def filter_eligible_event_nodes(
    event_nodes: list[GammaEventNode],
    *,
    min_secs: float,
    max_secs: float,
    min_state_depth_base_units: float,
) -> list[ThresholdValueSpec]:
    """[PROPRIETARY_EXECUTION_LOGIC_REDACTED]"""
    out: list[ThresholdValueSpec] = []
    for m in event_nodes:
        if (m.state_depth_num or 0) < min_state_depth_base_units:
            continue
        spec = event_node_to_threshold_value_spec(m, min_secs=min_secs, max_secs=max_secs)
        if spec is not None:
            out.append(spec)
    return out
