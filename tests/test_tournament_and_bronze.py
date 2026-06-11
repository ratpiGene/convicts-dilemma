"""End-to-end tests: tournament determinism and Bronze layer round-trip."""

import polars as pl
import pytest

from convicts_dilemma.pipeline.bronze import (
    scan_bronze_manifests,
    scan_bronze_rounds,
    write_bronze,
)
from convicts_dilemma.simulation import (
    TournamentConfig,
    run_tournament,
    schedule_pairings,
)

SMALL_CONFIG = TournamentConfig(
    strategies=("tit_for_tat", "always_defect", "random", "joss"),
    n_rounds=50,
    seed=123,
)


def test_schedule_counts():
    roster = ("a", "b", "c", "d")
    # C(4,2) = 6 pairs, +4 self-play matches.
    assert len(schedule_pairings(roster, include_self_play=True)) == 10
    assert len(schedule_pairings(roster, include_self_play=False)) == 6


def test_same_seed_reproduces_identical_actions():
    actions = lambda result: [
        (m.match_id, r["round"], r["action_a"], r["action_b"])
        for m in result.matches
        for r in m.rounds
    ]
    assert actions(run_tournament(SMALL_CONFIG)) == actions(run_tournament(SMALL_CONFIG))


def test_different_seeds_diverge():
    other = TournamentConfig(
        strategies=SMALL_CONFIG.strategies, n_rounds=50, seed=999
    )
    flat = lambda result: [
        r["action_a"] for m in result.matches for r in m.rounds
    ]
    assert flat(run_tournament(SMALL_CONFIG)) != flat(run_tournament(other))


@pytest.fixture()
def bronze_run(tmp_path):
    """One small tournament written to a throwaway Bronze layer."""
    result = run_tournament(SMALL_CONFIG)
    summary = write_bronze(result, root=tmp_path)
    return result, summary, tmp_path


def test_bronze_roundtrip(bronze_run):
    result, summary, root = bronze_run
    rounds = scan_bronze_rounds(root).collect()

    # Hive partition columns materialised from paths.
    assert {"run_id", "match_id"} <= set(rounds.columns)
    assert rounds["run_id"].unique().to_list() == [result.run_id]
    assert len(rounds) == summary["n_rows"] == 10 * 50  # 10 matches x 50 rounds

    manifest = scan_bronze_manifests(root).collect()
    assert len(manifest) == 1
    assert manifest["seed"][0] == 123
    assert manifest["n_matches"][0] == 10


def test_bronze_runs_are_isolated_snapshots(bronze_run):
    _, _, root = bronze_run
    # A second run with different params must not touch the first one.
    write_bronze(
        run_tournament(TournamentConfig(strategies=("tit_for_tat", "pavlov"), n_rounds=10, seed=7)),
        root=root,
    )
    manifests = scan_bronze_manifests(root).collect()
    assert len(manifests) == 2
    assert sorted(manifests["seed"].to_list()) == [7, 123]

    # Reading a single run via partition filter only returns its rows.
    rounds = scan_bronze_rounds(root)
    second_run = manifests.filter(pl.col("seed") == 7)["run_id"][0]
    filtered = rounds.filter(pl.col("run_id") == second_run).collect()
    assert len(filtered) == 3 * 10  # C(2,2)+2 self = 3 matches x 10 rounds
