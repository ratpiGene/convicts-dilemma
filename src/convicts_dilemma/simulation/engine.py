"""Match engine: plays one match of N rounds between two strategies."""

from __future__ import annotations

from typing import Any

from convicts_dilemma.config import PayoffMatrix
from convicts_dilemma.strategies.base import Action, Strategy


def play_match(
    player_a: Strategy,
    player_b: Strategy,
    n_rounds: int,
    payoff: PayoffMatrix,
) -> list[dict[str, Any]]:
    """Play ``n_rounds`` rounds and return one record per round.

    Both players decide simultaneously: each only sees the history of
    *previous* rounds, never the opponent's current move.

    Args:
        player_a: First player (fresh instance, owns its seeded RNG).
        player_b: Second player.
        n_rounds: Number of rounds to play.
        payoff: Payoff matrix used to score each round.

    Returns:
        One dict per round with keys: ``round`` (1-based), ``action_a``,
        ``action_b``, ``payoff_a``, ``payoff_b``, ``cumulative_a``,
        ``cumulative_b``, ``reasoning_a``, ``reasoning_b``. Run- and
        match-level identifiers are added by the tournament scheduler.
    """
    history_a: list[Action] = []
    history_b: list[Action] = []
    cumulative_a = 0
    cumulative_b = 0
    records: list[dict[str, Any]] = []

    for round_number in range(1, n_rounds + 1):
        action_a = player_a.decide(history_a, history_b)
        action_b = player_b.decide(history_b, history_a)
        gain_a, gain_b = payoff.score(action_a, action_b)
        cumulative_a += gain_a
        cumulative_b += gain_b

        records.append(
            {
                "round": round_number,
                "action_a": action_a,
                "action_b": action_b,
                "payoff_a": gain_a,
                "payoff_b": gain_b,
                "cumulative_a": cumulative_a,
                "cumulative_b": cumulative_b,
                "reasoning_a": player_a.last_reasoning,
                "reasoning_b": player_b.last_reasoning,
            }
        )

        history_a.append(action_a)
        history_b.append(action_b)

    return records
