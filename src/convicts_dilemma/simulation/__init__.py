"""Simulation package: match engine + tournament scheduler."""

from convicts_dilemma.simulation.engine import play_match
from convicts_dilemma.simulation.tournament import (
    MatchResult,
    TournamentConfig,
    TournamentResult,
    run_tournament,
    schedule_pairings,
)

__all__ = [
    "play_match",
    "MatchResult",
    "TournamentConfig",
    "TournamentResult",
    "run_tournament",
    "schedule_pairings",
]
