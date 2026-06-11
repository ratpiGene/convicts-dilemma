"""Behavioural tests for the coded strategies."""

import random

import pytest

from convicts_dilemma.strategies import DEFAULT_ROSTER, REGISTRY, create_strategy
from convicts_dilemma.strategies.base import COOPERATE, DEFECT
from convicts_dilemma.strategies.classic import (
    GrimTrigger,
    Pavlov,
    SuspiciousTitForTat,
    TitForTat,
    TitForTwoTats,
)


def test_default_roster_is_the_ten_coded_strategies():
    assert len(DEFAULT_ROSTER) == 10
    # REGISTRY may also hold the lazily-registered LLM personas.
    assert set(DEFAULT_ROSTER) <= set(REGISTRY)
    assert all(not REGISTRY[name].is_llm for name in DEFAULT_ROSTER)


def test_create_strategy_unknown_name_lists_options():
    with pytest.raises(KeyError, match="tit_for_tat"):
        create_strategy("does_not_exist")


def test_tit_for_tat_opens_cooperating_then_mirrors():
    s = TitForTat()
    assert s.decide([], []) == COOPERATE
    assert s.decide([COOPERATE], [DEFECT]) == DEFECT
    assert s.decide([COOPERATE, DEFECT], [DEFECT, COOPERATE]) == COOPERATE


def test_suspicious_tit_for_tat_opens_defecting():
    s = SuspiciousTitForTat()
    assert s.decide([], []) == DEFECT
    assert s.decide([DEFECT], [COOPERATE]) == COOPERATE


def test_tit_for_two_tats_absorbs_single_defection():
    s = TitForTwoTats()
    assert s.decide([COOPERATE], [DEFECT]) == COOPERATE
    assert s.decide([COOPERATE, COOPERATE], [DEFECT, DEFECT]) == DEFECT


def test_grim_trigger_never_forgives():
    s = GrimTrigger()
    assert s.decide([], []) == COOPERATE
    assert s.decide([COOPERATE], [DEFECT]) == DEFECT
    # Opponent returns to cooperation — grim stays triggered forever.
    assert s.decide([COOPERATE, DEFECT], [DEFECT, COOPERATE]) == DEFECT
    assert s.decide([COOPERATE, DEFECT, DEFECT], [DEFECT, COOPERATE, COOPERATE]) == DEFECT


def test_pavlov_win_stay_lose_shift():
    s = Pavlov()
    assert s.decide([], []) == COOPERATE
    # Won (they cooperated) -> repeat my move.
    assert s.decide([COOPERATE], [COOPERATE]) == COOPERATE
    assert s.decide([DEFECT], [COOPERATE]) == DEFECT
    # Lost (they defected) -> switch my move.
    assert s.decide([COOPERATE], [DEFECT]) == DEFECT
    assert s.decide([DEFECT], [DEFECT]) == COOPERATE


@pytest.mark.parametrize("name", ["random", "joss", "generous_tit_for_tat"])
def test_stochastic_strategies_are_deterministic_under_seed(name):
    history_mine = [COOPERATE, DEFECT, COOPERATE, COOPERATE]
    history_theirs = [DEFECT, COOPERATE, DEFECT, COOPERATE]

    def sequence():
        s = create_strategy(name, random.Random("fixed-seed"))
        return [
            s.decide(history_mine[:i], history_theirs[:i])
            for i in range(len(history_mine) + 1)
        ]

    assert sequence() == sequence()
