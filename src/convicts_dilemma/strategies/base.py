"""Core types shared by every strategy: actions and the Strategy interface."""

from __future__ import annotations

import random
from abc import ABC, abstractmethod
from typing import ClassVar, Literal, Sequence

#: A single move in the prisoner's dilemma. "C" = cooperate, "D" = defect.
Action = Literal["C", "D"]

COOPERATE: Action = "C"
DEFECT: Action = "D"


class Strategy(ABC):
    """Interface implemented by every player (coded strategy or LLM agent).

    A fresh instance is created for **each match**, so subclasses may keep
    internal state between rounds (e.g. Grim Trigger's "has been betrayed"
    flag) without leaking it across matches.

    Determinism contract: any randomness MUST go through ``self.rng``, which
    the tournament seeds deterministically per (run seed, match, player slot).
    Two tournaments with the same seed therefore produce identical datasets.

    Attributes:
        name: Stable snake_case identifier, used in the dataset and the
            strategy registry (e.g. ``"tit_for_tat"``).
        is_llm: True for LLM-backed agents; the tournament gives those
            matches a shorter, dedicated round count (``llm_n_rounds``).
        last_reasoning: Free-text justification of the **last** decision.
            ``None`` for coded strategies; LLM agents fill it after each
            call so the engine can record it in the Bronze layer.
    """

    name: ClassVar[str]
    is_llm: ClassVar[bool] = False

    def __init__(self, rng: random.Random | None = None) -> None:
        """Create a player for one match.

        Args:
            rng: Seeded random generator. Falls back to an unseeded one so
                strategies stay usable in quick interactive experiments.
        """
        self.rng = rng if rng is not None else random.Random()
        self.last_reasoning: str | None = None

    @abstractmethod
    def decide(
        self,
        my_history: Sequence[Action],
        their_history: Sequence[Action],
    ) -> Action:
        """Choose the next move given both players' past moves.

        Args:
            my_history: This player's moves so far, oldest first. Empty on
                the first round.
            their_history: The opponent's moves so far, same ordering and
                length as ``my_history``.

        Returns:
            ``"C"`` to cooperate or ``"D"`` to defect.
        """
