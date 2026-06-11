"""Strategy registry: map stable names to classes, build seeded instances.

The registry is the single source of truth for which players exist. The
tournament config references strategies **by name**, and the names end up
verbatim in the dataset (``player_a`` / ``player_b`` columns), so they must
stay stable across runs.
"""

from __future__ import annotations

import random

from convicts_dilemma.strategies.base import Action, COOPERATE, DEFECT, Strategy
from convicts_dilemma.strategies.classic import (
    AlwaysCooperate,
    AlwaysDefect,
    GenerousTitForTat,
    GrimTrigger,
    Joss,
    Pavlov,
    RandomStrategy,
    SuspiciousTitForTat,
    TitForTat,
    TitForTwoTats,
)

__all__ = [
    "Action",
    "COOPERATE",
    "DEFECT",
    "Strategy",
    "REGISTRY",
    "DEFAULT_ROSTER",
    "create_strategy",
]

#: All known strategies, keyed by their stable name.
REGISTRY: dict[str, type[Strategy]] = {
    cls.name: cls
    for cls in (
        AlwaysCooperate,
        AlwaysDefect,
        TitForTat,
        SuspiciousTitForTat,
        GenerousTitForTat,
        TitForTwoTats,
        GrimTrigger,
        Pavlov,
        Joss,
        RandomStrategy,
    )
}

#: Default tournament roster: every registered coded strategy.
DEFAULT_ROSTER: tuple[str, ...] = tuple(REGISTRY)


def create_strategy(name: str, rng: random.Random | None = None) -> Strategy:
    """Instantiate a strategy by registry name with its own seeded RNG.

    Args:
        name: A key of :data:`REGISTRY` (e.g. ``"tit_for_tat"``).
        rng: Per-(match, player) random generator; see the determinism
            contract on :class:`Strategy`.

    Raises:
        KeyError: If ``name`` is not registered, listing valid names.
    """
    try:
        cls = REGISTRY[name]
    except KeyError:
        raise KeyError(
            f"Unknown strategy {name!r}. Available: {', '.join(sorted(REGISTRY))}"
        ) from None
    return cls(rng=rng)
