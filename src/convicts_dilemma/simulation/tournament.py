"""Tournament scheduler: round-robin between strategies, fully seeded.

This module is the "Generate" stage of the pipeline. It owns the two
identifiers that structure the whole data lake:

- ``run_id``: one tournament execution = one immutable snapshot. Format
  ``<UTC timestamp>-<short uuid>`` so partitions sort chronologically while
  staying collision-proof.
- ``match_id``: position of the pairing in the round-robin schedule,
  deterministic for a given roster.
"""

from __future__ import annotations

import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from convicts_dilemma.config import PayoffMatrix
from convicts_dilemma.simulation.engine import play_match
from convicts_dilemma.strategies import DEFAULT_ROSTER, create_strategy

#: Schema/engine revision, recorded in every manifest so future schema
#: changes can be told apart when comparing old and new runs.
ENGINE_VERSION = 1


@dataclass(frozen=True)
class TournamentConfig:
    """Parameters of one tournament run (everything the manifest records).

    Attributes:
        strategies: Roster of registry names entering the round-robin.
        n_rounds: Rounds per match (Axelrod-style default: 2000... the
            project spec example; kept configurable per run).
        seed: Master seed. Every per-player RNG is derived from it, so the
            same config reproduces the exact same dataset.
        payoff: Payoff matrix for every match of the run.
        include_self_play: Whether each strategy also plays against a copy
            of itself (as in Axelrod's original tournament).
    """

    strategies: tuple[str, ...] = DEFAULT_ROSTER
    n_rounds: int = 2000
    seed: int = 42
    payoff: PayoffMatrix = field(default_factory=PayoffMatrix)
    include_self_play: bool = True


@dataclass(frozen=True)
class MatchResult:
    """All rounds of one pairing, plus who played."""

    match_id: int
    player_a: str
    player_b: str
    rounds: list[dict[str, Any]]


@dataclass(frozen=True)
class TournamentResult:
    """Output of one tournament run: identifiers, manifest and all matches."""

    run_id: str
    manifest: dict[str, Any]
    matches: list[MatchResult]


def schedule_pairings(
    strategies: tuple[str, ...], include_self_play: bool
) -> list[tuple[str, str]]:
    """Build the round-robin schedule: each unordered pair plays once.

    Order is deterministic (roster order), which makes ``match_id`` stable
    for a given config.
    """
    pairings: list[tuple[str, str]] = []
    for i, name_a in enumerate(strategies):
        start = i if include_self_play else i + 1
        for name_b in strategies[start:]:
            pairings.append((name_a, name_b))
    return pairings


def run_tournament(config: TournamentConfig) -> TournamentResult:
    """Play the full round-robin tournament described by ``config``.

    Each player gets its own ``random.Random`` seeded with the string
    ``"{seed}:{match_id}:{slot}"`` — string seeding is stable across Python
    processes and versions (SHA-512 based), unlike ``hash()``.

    Returns:
        A :class:`TournamentResult` ready to be written to the Bronze layer.
    """
    created_at = datetime.now(timezone.utc)
    run_id = f"{created_at:%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}"

    pairings = schedule_pairings(config.strategies, config.include_self_play)
    matches: list[MatchResult] = []
    for match_id, (name_a, name_b) in enumerate(pairings):
        player_a = create_strategy(name_a, random.Random(f"{config.seed}:{match_id}:a"))
        player_b = create_strategy(name_b, random.Random(f"{config.seed}:{match_id}:b"))
        rounds = play_match(player_a, player_b, config.n_rounds, config.payoff)
        matches.append(MatchResult(match_id, name_a, name_b, rounds))

    manifest = {
        "run_id": run_id,
        "created_at": created_at,
        "engine_version": ENGINE_VERSION,
        "seed": config.seed,
        "n_rounds": config.n_rounds,
        "n_matches": len(matches),
        "strategies": list(config.strategies),
        "include_self_play": config.include_self_play,
        "payoff_reward": config.payoff.reward,
        "payoff_temptation": config.payoff.temptation,
        "payoff_sucker": config.payoff.sucker,
        "payoff_punishment": config.payoff.punishment,
    }
    return TournamentResult(run_id=run_id, manifest=manifest, matches=matches)
