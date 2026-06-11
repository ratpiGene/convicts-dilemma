"""Tests for the match engine: payoffs, cumulative scores, simultaneity."""

from convicts_dilemma.config import PayoffMatrix
from convicts_dilemma.simulation import play_match
from convicts_dilemma.strategies.classic import (
    AlwaysCooperate,
    AlwaysDefect,
    TitForTat,
)

PAYOFF = PayoffMatrix()


def test_payoff_matrix_covers_all_outcomes():
    assert PAYOFF.score("C", "C") == (3, 3)
    assert PAYOFF.score("C", "D") == (0, 5)
    assert PAYOFF.score("D", "C") == (5, 0)
    assert PAYOFF.score("D", "D") == (1, 1)


def test_match_record_shape_and_cumulative_scores():
    records = play_match(AlwaysCooperate(), AlwaysDefect(), n_rounds=5, payoff=PAYOFF)
    assert len(records) == 5
    last = records[-1]
    assert last["round"] == 5
    # Cooperator is exploited every round: 5 * sucker vs 5 * temptation.
    assert last["cumulative_a"] == 0
    assert last["cumulative_b"] == 25
    assert all(r["reasoning_a"] is None for r in records)


def test_decisions_are_simultaneous():
    # TFT vs AlwaysDefect: TFT cooperates round 1 (it cannot see the
    # opponent's current move), then defects from round 2 onward.
    records = play_match(TitForTat(), AlwaysDefect(), n_rounds=3, payoff=PAYOFF)
    assert [r["action_a"] for r in records] == ["C", "D", "D"]
    assert [r["action_b"] for r in records] == ["D", "D", "D"]
