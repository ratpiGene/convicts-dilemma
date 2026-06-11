"""Project-wide configuration: payoff matrix and data-lake layout."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from convicts_dilemma.strategies.base import COOPERATE, Action


def data_root() -> Path:
    """Root directory of the local data lake.

    Defaults to ``./data`` (relative to where the pipeline is launched,
    i.e. the repo root) and can be overridden with the ``CONVICTS_DATA_DIR``
    environment variable — useful for tests and throwaway experiments.
    """
    return Path(os.environ.get("CONVICTS_DATA_DIR", "data"))


@dataclass(frozen=True)
class PayoffMatrix:
    """Payoffs of one prisoner's dilemma round, from one player's viewpoint.

    Defaults are Axelrod's canonical values. A valid dilemma requires
    ``temptation > reward > punishment > sucker`` and, for the *iterated*
    game, ``2 * reward > temptation + sucker`` (mutual cooperation must beat
    alternating exploitation).

    Attributes:
        reward: Both cooperate (R).
        temptation: I defect, they cooperate (T).
        sucker: I cooperate, they defect (S).
        punishment: Both defect (P).
    """

    reward: int = 3
    temptation: int = 5
    sucker: int = 0
    punishment: int = 1

    def score(self, mine: Action, theirs: Action) -> tuple[int, int]:
        """Return the (my_payoff, their_payoff) pair for one round."""
        if mine == COOPERATE:
            if theirs == COOPERATE:
                return self.reward, self.reward
            return self.sucker, self.temptation
        if theirs == COOPERATE:
            return self.temptation, self.sucker
        return self.punishment, self.punishment
