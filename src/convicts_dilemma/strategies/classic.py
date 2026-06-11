"""The ten coded strategies of the tournament (classic Axelrod roster).

Each class is intentionally tiny: the point of the project is the data
pipeline, but the roster is chosen so the Gold-layer metrics have contrast —
unconditional players (always_*), reciprocators (tit_for_tat family),
punishers (grim_trigger), exploiters (joss) and noise (random).
"""

from __future__ import annotations

import random
from typing import Sequence

from convicts_dilemma.strategies.base import COOPERATE, DEFECT, Action, Strategy


class AlwaysCooperate(Strategy):
    """Cooperates unconditionally. Baseline for maximal exploitability."""

    name = "always_cooperate"

    def decide(self, my_history: Sequence[Action], their_history: Sequence[Action]) -> Action:
        return COOPERATE


class AlwaysDefect(Strategy):
    """Defects unconditionally. The one-shot Nash equilibrium, iterated."""

    name = "always_defect"

    def decide(self, my_history: Sequence[Action], their_history: Sequence[Action]) -> Action:
        return DEFECT


class TitForTat(Strategy):
    """Cooperates first, then mirrors the opponent's previous move.

    Winner of both of Axelrod's 1980/1981 tournaments.
    """

    name = "tit_for_tat"

    def decide(self, my_history: Sequence[Action], their_history: Sequence[Action]) -> Action:
        if not their_history:
            return COOPERATE
        return their_history[-1]


class SuspiciousTitForTat(Strategy):
    """Tit for tat, but opens with a defection instead of cooperating."""

    name = "suspicious_tit_for_tat"

    def decide(self, my_history: Sequence[Action], their_history: Sequence[Action]) -> Action:
        if not their_history:
            return DEFECT
        return their_history[-1]


class GenerousTitForTat(Strategy):
    """Tit for tat that forgives a defection with probability ``generosity``.

    Generosity breaks the endless retaliation loops two tit-for-tat-like
    players can fall into, which makes this strategy a key contributor to
    the ``forgiveness_index`` Gold table.
    """

    name = "generous_tit_for_tat"

    def __init__(self, rng: random.Random | None = None, generosity: float = 0.1) -> None:
        super().__init__(rng)
        self.generosity = generosity

    def decide(self, my_history: Sequence[Action], their_history: Sequence[Action]) -> Action:
        if not their_history:
            return COOPERATE
        if their_history[-1] == DEFECT and self.rng.random() < self.generosity:
            return COOPERATE
        return their_history[-1]


class TitForTwoTats(Strategy):
    """Defects only after two *consecutive* opponent defections.

    More tolerant than tit for tat: a single betrayal is absorbed.
    """

    name = "tit_for_two_tats"

    def decide(self, my_history: Sequence[Action], their_history: Sequence[Action]) -> Action:
        if len(their_history) >= 2 and their_history[-1] == DEFECT and their_history[-2] == DEFECT:
            return DEFECT
        return COOPERATE


class GrimTrigger(Strategy):
    """Cooperates until betrayed once, then defects forever.

    Stateful: the trigger persists for the rest of the match, which is why
    strategies are instantiated per match.
    """

    name = "grim_trigger"

    def __init__(self, rng: random.Random | None = None) -> None:
        super().__init__(rng)
        self._triggered = False

    def decide(self, my_history: Sequence[Action], their_history: Sequence[Action]) -> Action:
        if their_history and their_history[-1] == DEFECT:
            self._triggered = True
        return DEFECT if self._triggered else COOPERATE


class Pavlov(Strategy):
    """Win-stay / lose-shift.

    Repeats its previous move if it "won" the round (opponent cooperated,
    i.e. payoff was R or T), switches if it "lost" (opponent defected,
    payoff S or P). Cooperates on the first round.
    """

    name = "pavlov"

    def decide(self, my_history: Sequence[Action], their_history: Sequence[Action]) -> Action:
        if not my_history:
            return COOPERATE
        if their_history[-1] == COOPERATE:
            return my_history[-1]
        return COOPERATE if my_history[-1] == DEFECT else DEFECT


class Joss(Strategy):
    """Sneaky tit for tat: where TFT would cooperate, defects with
    probability ``sneak_rate`` to test if exploitation goes unpunished.

    Submitted to Axelrod's first tournament; famously triggered long echo
    feuds against retaliating strategies.
    """

    name = "joss"

    def __init__(self, rng: random.Random | None = None, sneak_rate: float = 0.1) -> None:
        super().__init__(rng)
        self.sneak_rate = sneak_rate

    def decide(self, my_history: Sequence[Action], their_history: Sequence[Action]) -> Action:
        if their_history and their_history[-1] == DEFECT:
            return DEFECT
        return DEFECT if self.rng.random() < self.sneak_rate else COOPERATE


class RandomStrategy(Strategy):
    """Cooperates with probability ``p_cooperate``, ignoring all history."""

    name = "random"

    def __init__(self, rng: random.Random | None = None, p_cooperate: float = 0.5) -> None:
        super().__init__(rng)
        self.p_cooperate = p_cooperate

    def decide(self, my_history: Sequence[Action], their_history: Sequence[Action]) -> Action:
        return COOPERATE if self.rng.random() < self.p_cooperate else DEFECT
