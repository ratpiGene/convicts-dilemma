"""Tests for the Silver transform: reshape, derived columns, idempotency."""

import polars as pl
import pytest

from convicts_dilemma.pipeline.bronze import write_bronze
from convicts_dilemma.pipeline.silver import (
    build_silver_run,
    pending_run_ids,
    scan_silver_rounds,
    transform_pending,
)
from convicts_dilemma.simulation import TournamentConfig, run_tournament


@pytest.fixture()
def lake(tmp_path):
    """A tiny deterministic run written to Bronze in a throwaway lake.

    Roster has no stochastic strategy, so every assertion below is exact:
    - tit_for_tat vs always_defect: TFT plays C then D forever.
    - pavlov vs always_defect: pavlov alternates C, D, C, D... (win-stay/
      lose-shift always "loses" vs a defector), exercising `forgave`.
    """
    config = TournamentConfig(
        strategies=("tit_for_tat", "always_defect", "pavlov"),
        n_rounds=8,
        seed=1,
        include_self_play=False,
    )
    result = run_tournament(config)
    write_bronze(result, root=tmp_path)
    return tmp_path, result


def perspective(frame: pl.DataFrame, player: str, opponent: str) -> pl.DataFrame:
    return frame.filter(
        (pl.col("player") == player) & (pl.col("opponent") == opponent)
    ).sort("round")


def test_silver_is_player_centric_long_format(lake):
    root, result = lake
    frame = build_silver_run(result.run_id, root=root)
    # 3 matches x 8 rounds x 2 perspectives.
    assert len(frame) == 3 * 8 * 2
    # Both perspectives of the same match see mirrored pairings.
    tft = perspective(frame, "tit_for_tat", "always_defect")
    ad = perspective(frame, "always_defect", "tit_for_tat")
    assert tft["action"].to_list() == ad["opponent_action"].to_list()
    assert tft["payoff"].to_list() == [0] + [1] * 7  # sucker once, then P forever


def test_memory_and_expanding_features(lake):
    root, result = lake
    tft = perspective(
        build_silver_run(result.run_id, root=root), "tit_for_tat", "always_defect"
    )
    assert tft["action"].to_list() == ["C"] + ["D"] * 7
    # 1-round memory: first round has no previous action.
    assert tft["prev_action"].to_list() == [None, "C"] + ["D"] * 6
    assert tft["prev_opponent_action"].to_list() == [None] + ["D"] * 7
    # Expanding cooperation rate: 1/1, 1/2, 1/3...
    assert tft["coop_rate_so_far"].to_list() == pytest.approx([1 / n for n in range(1, 9)])
    assert tft["defections_so_far"].to_list() == list(range(8))
    # TFT never returns to cooperation against a constant defector...
    assert not tft["forgave"].any()
    # ...it retaliates from round 2 onward.
    assert tft["retaliated"].to_list() == [False] + [True] * 7


def test_forgiveness_flag_with_pavlov(lake):
    root, result = lake
    pavlov = perspective(
        build_silver_run(result.run_id, root=root), "pavlov", "always_defect"
    )
    # Win-stay/lose-shift vs always_defect alternates C, D, C, D...
    assert pavlov["action"].to_list() == ["C", "D"] * 4
    # Every return to C right after being betrayed counts as forgiveness.
    assert pavlov["forgave"].to_list() == [False, False] + [True, False] * 3


def test_round_bucket_grain(lake):
    root, result = lake
    frame = build_silver_run(result.run_id, root=root, bucket_size=3)
    buckets = perspective(frame, "tit_for_tat", "always_defect")["round_bucket"]
    # Rounds 1-8 with bucket_size=3 -> buckets starting at 1, 4, 7.
    assert buckets.to_list() == [1, 1, 1, 4, 4, 4, 7, 7]


def test_transform_pending_is_incremental_and_idempotent(lake):
    root, result = lake
    first = transform_pending(root=root)
    assert [s["run_id"] for s in first] == [result.run_id]
    # Everything processed: nothing pending, second call is a no-op.
    assert pending_run_ids(root=root) == []
    assert transform_pending(root=root) == []

    silver = scan_silver_rounds(root=root).collect()
    assert silver["run_id"].unique().to_list() == [result.run_id]
    assert len(silver) == 3 * 8 * 2
