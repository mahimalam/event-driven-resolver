"""Tests for the divergence detector."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from event_driven_resolver.models.types import WeatherAlert
from event_driven_resolver.core_logic.detector import DivergenceDetector


def _alert(
    alert_id: str = "NWS-TEST-001",
    event_type: str = "Tornado Warning",
    severity: str = "Extreme",
    certainty: str = "Observed",
) -> WeatherAlert:
    return WeatherAlert(
        alert_id=alert_id,
        event_type=event_type,
        severity=severity,
        certainty=certainty,
        onset=None,
        expires=None,
        area_desc="Test County, ST",
        headline="Test alert headline",
    )


class TestDivergenceDetector:
    def test_high_severity_alert_produces_signal(self):
        det = DivergenceDetector(min_delta_pct=5.0, min_severity_score=0.1)
        signal = det.evaluate(_alert(severity="Extreme", certainty="Observed"))
        assert signal is not None
        assert signal.posterior_prob > signal.prior_prob

    def test_low_severity_alert_is_filtered(self):
        det = DivergenceDetector(min_severity_score=0.50)
        signal = det.evaluate(_alert(severity="Minor", certainty="Unlikely"))
        assert signal is None

    def test_same_alert_not_processed_twice(self):
        det = DivergenceDetector(min_delta_pct=5.0, min_severity_score=0.1)
        a = _alert(alert_id="dup-001", severity="Extreme", certainty="Observed")
        first = det.evaluate(a)
        second = det.evaluate(a)
        assert first is not None
        assert second is None

    def test_signal_delta_is_positive(self):
        det = DivergenceDetector(min_delta_pct=0.0, min_severity_score=0.0)
        signal = det.evaluate(_alert(severity="Severe", certainty="Likely"))
        assert signal is not None
        assert signal.delta_pct > 0.0

    def test_signal_fields_populated(self):
        det = DivergenceDetector(min_delta_pct=0.0, min_severity_score=0.0)
        signal = det.evaluate(_alert())
        assert signal is not None
        assert signal.alert_id == "NWS-TEST-001"
        assert signal.event_type == "Tornado Warning"
        assert 0 < signal.prior_prob < 1
        assert 0 < signal.posterior_prob <= 1
        assert signal.severity_score > 0

    def test_extreme_observed_has_high_posterior(self):
        det = DivergenceDetector(min_delta_pct=0.0, min_severity_score=0.0)
        signal = det.evaluate(_alert(severity="Extreme", certainty="Observed"))
        assert signal is not None
        assert signal.posterior_prob > 0.7

    def test_possible_moderate_has_lower_posterior(self):
        det = DivergenceDetector(min_delta_pct=0.0, min_severity_score=0.0)
        extreme = _alert(alert_id="ext", severity="Extreme", certainty="Observed")
        moderate = _alert(alert_id="mod", severity="Moderate", certainty="Possible")
        s_extreme = det.evaluate(extreme)
        s_moderate = det.evaluate(moderate)
        assert s_extreme is not None
        assert s_moderate is not None
        assert s_extreme.posterior_prob > s_moderate.posterior_prob
