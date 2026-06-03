"""Tests for the implied probability calculator."""

from __future__ import annotations

import math

import pytest

from event_driven_resolver.core_logic.implied_prob import (
    implied_prob_from_alert,
    implied_prob_above,
    ImpliedProbResult,
)


class TestImpliedProbFromAlert:
    def test_extreme_observed_gives_high_prob(self):
        result = implied_prob_from_alert("Extreme", "Observed")
        assert result.prob_above > 0.9

    def test_unknown_unknown_gives_low_prob(self):
        result = implied_prob_from_alert("Unknown", "Unknown")
        assert result.prob_above < 0.2

    def test_probs_sum_to_one(self):
        result = implied_prob_from_alert("Severe", "Likely")
        assert abs(result.prob_above + result.prob_below - 1.0) < 1e-9

    def test_prob_bounded(self):
        for sev in ("Extreme", "Severe", "Moderate", "Minor", "Unknown"):
            for cert in ("Observed", "Likely", "Possible", "Unlikely", "Unknown"):
                result = implied_prob_from_alert(sev, cert)
                assert 0.0 <= result.prob_above <= 1.0

    def test_confidence_populated(self):
        result = implied_prob_from_alert("Extreme", "Observed")
        assert result.confidence in ("low", "medium", "high")

    def test_higher_severity_gives_higher_prob(self):
        extreme = implied_prob_from_alert("Extreme", "Likely")
        minor = implied_prob_from_alert("Minor", "Likely")
        assert extreme.prob_above > minor.prob_above

    def test_higher_certainty_gives_higher_prob(self):
        observed = implied_prob_from_alert("Severe", "Observed")
        unlikely = implied_prob_from_alert("Severe", "Unlikely")
        assert observed.prob_above > unlikely.prob_above


class TestImpliedProbAbove:
    def test_spot_above_threshold_at_expiry(self):
        result = implied_prob_above(
            spot=110.0, threshold_value=100.0, seconds_to_expiry=0,
            realized_vol_bps_per_window=0, vol_window_sec=60,
        )
        assert result.prob_above == 1.0

    def test_spot_below_threshold_at_expiry(self):
        result = implied_prob_above(
            spot=90.0, threshold_value=100.0, seconds_to_expiry=0,
            realized_vol_bps_per_window=0, vol_window_sec=60,
        )
        assert result.prob_above == 0.0

    def test_zero_vol_returns_half(self):
        result = implied_prob_above(
            spot=100.0, threshold_value=100.0, seconds_to_expiry=3600,
            realized_vol_bps_per_window=0, vol_window_sec=60,
        )
        assert result.prob_above == 0.5

    def test_prob_bounded(self):
        result = implied_prob_above(
            spot=200.0, threshold_value=100.0, seconds_to_expiry=60,
            realized_vol_bps_per_window=10, vol_window_sec=60,
        )
        assert 0.0 <= result.prob_above <= 1.0

    def test_probs_sum_to_one(self):
        result = implied_prob_above(
            spot=100.0, threshold_value=95.0, seconds_to_expiry=3600,
            realized_vol_bps_per_window=100, vol_window_sec=60,
        )
        assert abs(result.prob_above + result.prob_below - 1.0) < 1e-9

    def test_invalid_spot_returns_half(self):
        result = implied_prob_above(
            spot=0.0, threshold_value=100.0, seconds_to_expiry=3600,
            realized_vol_bps_per_window=100, vol_window_sec=60,
        )
        assert result.prob_above == 0.5
