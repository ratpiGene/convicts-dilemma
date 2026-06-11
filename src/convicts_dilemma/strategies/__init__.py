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
    "LLM_ROSTER",
    "create_strategy",
]

_CODED_CLASSES: tuple[type[Strategy], ...] = (
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

#: All known strategies, keyed by their stable name. Starts with the coded
#: roster; the LLM personas are merged in lazily (see _register_llm_agents).
REGISTRY: dict[str, type[Strategy]] = {cls.name: cls for cls in _CODED_CLASSES}

#: Default tournament roster: the coded strategies only. LLM personas are
#: opt-in (they need a running Ollama server) — add them to the roster
#: explicitly via the tournament config.
DEFAULT_ROSTER: tuple[str, ...] = tuple(cls.name for cls in _CODED_CLASSES)

_llm_registered = False


def _register_llm_agents() -> None:
    """Merge the Ollama personas into REGISTRY on first use.

    Deferred on purpose: the agents module imports ``strategies.base``,
    which executes this package's ``__init__`` first — a top-level import
    of the agents here would therefore be a circular import.
    """
    global _llm_registered
    if _llm_registered:
        return
    from convicts_dilemma.agents.ollama_agent import AGENT_CLASSES

    for cls in AGENT_CLASSES:
        REGISTRY[cls.name] = cls
    _llm_registered = True


def __getattr__(name: str):
    """PEP 562 hook: ``LLM_ROSTER`` triggers agent registration on access."""
    if name == "LLM_ROSTER":
        _register_llm_agents()
        from convicts_dilemma.agents.ollama_agent import AGENT_CLASSES

        return tuple(cls.name for cls in AGENT_CLASSES)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def create_strategy(
    name: str,
    rng: random.Random | None = None,
    *,
    ollama_model: str | None = None,
) -> Strategy:
    """Instantiate a strategy by registry name with its own seeded RNG.

    Args:
        name: A key of :data:`REGISTRY` (e.g. ``"tit_for_tat"``).
        rng: Per-(match, player) random generator; see the determinism
            contract on :class:`Strategy`.
        ollama_model: Model tag for LLM agents (ignored by coded
            strategies, which take no such parameter).

    Raises:
        KeyError: If ``name`` is not registered, listing valid names.
    """
    _register_llm_agents()
    try:
        cls = REGISTRY[name]
    except KeyError:
        raise KeyError(
            f"Unknown strategy {name!r}. Available: {', '.join(sorted(REGISTRY))}"
        ) from None
    if cls.is_llm and ollama_model is not None:
        return cls(rng=rng, model=ollama_model)  # type: ignore[call-arg]
    return cls(rng=rng)
