"""Silver layer: cleaned, player-centric, feature-enriched rounds (Polars).

Two things happen between Bronze and Silver:

1. **Reshape** — Bronze is match-centric (one row per round with ``_a``/``_b``
   column pairs). Silver unpivots each round into **two rows, one per player
   perspective**: (player, opponent, action, payoff...). Every downstream
   per-strategy metric (cooperation rate, forgiveness...) then becomes a
   plain group-by instead of a union of two column sets.

2. **Enrich** — derived columns required by the Gold tables, all computed
   with Polars window expressions partitioned *per player within a match*
   (``.over([match_id, player_slot])``):

   - ``prev_action`` / ``prev_opponent_action``: 1-round memory (``shift``).
   - ``cooperated``, ``betrayed``, ``mutual_cooperation``, ``mutual_defection``.
   - ``coop_rate_so_far``: expanding cooperation rate (``cum_sum / cum_count``).
   - ``rolling_coop_rate``: rolling mean over the last N rounds — the
     behavioural-drift signal.
   - ``defections_so_far``: expanding betrayal count.
   - ``forgave``: cooperated immediately after being betrayed (basis of the
     ``forgiveness_index`` Gold table).
   - ``retaliated``: defected immediately after being betrayed.
   - ``round_bucket``: first round of the bucket the row falls in
     (1, 101, 201... by default) — grain of ``behavioral_drift``.

Layout mirrors Bronze: ``silver/rounds/run_id=<id>/rounds.parquet`` with
``run_id`` only in the path. The transform is **incremental and idempotent**:
it processes exactly the Bronze runs that have no Silver partition yet, so
re-running the asset never recomputes or duplicates anything.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl

from convicts_dilemma.config import data_root
from convicts_dilemma.pipeline.bronze import bronze_rounds_dir, scan_bronze_rounds

#: Rounds covered by the rolling cooperation rate.
DEFAULT_ROLLING_WINDOW = 50
#: Rounds per ``round_bucket`` (the spec suggests 1-100, 101-200...).
DEFAULT_BUCKET_SIZE = 100

#: Window of every per-player expression: one player's rounds in one match.
_PLAYER_WINDOW = ("match_id", "player_slot")


def silver_rounds_dir(root: Path | None = None) -> Path:
    """Directory holding every run's Silver partitions."""
    return (root or data_root()) / "silver" / "rounds"


def list_partition_run_ids(directory: Path) -> set[str]:
    """Run ids present in a layer directory (from ``run_id=...`` dir names)."""
    if not directory.exists():
        return set()
    return {
        entry.name.removeprefix("run_id=")
        for entry in directory.iterdir()
        if entry.is_dir() and entry.name.startswith("run_id=")
    }


def pending_run_ids(root: Path | None = None) -> list[str]:
    """Bronze runs that have no Silver partition yet (chronological order)."""
    done = list_partition_run_ids(silver_rounds_dir(root))
    available = list_partition_run_ids(bronze_rounds_dir(root))
    return sorted(available - done)


def _player_view(rounds: pl.LazyFrame, slot: str) -> pl.LazyFrame:
    """Project the match-centric frame onto one player's perspective."""
    me, them = ("a", "b") if slot == "a" else ("b", "a")
    return rounds.select(
        pl.col("match_id"),
        pl.col("round"),
        pl.lit(slot).alias("player_slot"),
        pl.col(f"player_{me}").alias("player"),
        pl.col(f"player_{them}").alias("opponent"),
        pl.col(f"action_{me}").alias("action"),
        pl.col(f"action_{them}").alias("opponent_action"),
        pl.col(f"payoff_{me}").alias("payoff"),
        pl.col(f"cumulative_{me}").alias("cumulative_score"),
        pl.col(f"reasoning_{me}").alias("reasoning"),
    )


def build_silver_run(
    run_id: str,
    root: Path | None = None,
    rolling_window: int = DEFAULT_ROLLING_WINDOW,
    bucket_size: int = DEFAULT_BUCKET_SIZE,
) -> pl.DataFrame:
    """Clean, reshape and enrich one Bronze run into the Silver frame.

    Args:
        run_id: Bronze partition to transform.
        root: Data-lake root override (tests).
        rolling_window: Window (in rounds) of ``rolling_coop_rate``.
        bucket_size: Width of ``round_bucket``.

    Returns:
        The collected Silver DataFrame, sorted by (match, player, round).
    """
    bronze = scan_bronze_rounds(root).filter(pl.col("run_id") == run_id).drop("run_id")

    long = pl.concat([_player_view(bronze, "a"), _player_view(bronze, "b")])

    cleaned = (
        long
        # Cleaning: enforce the action domain and key completeness. Bronze
        # is machine-generated so this normally drops nothing, but the
        # Silver contract must hold even if a future generator (an LLM...)
        # writes garbage.
        .filter(
            pl.col("action").is_in(["C", "D"])
            & pl.col("opponent_action").is_in(["C", "D"])
            & pl.col("round").is_not_null()
            & pl.col("payoff").is_not_null()
        )
        .unique(subset=["match_id", "round", "player_slot"], keep="first")
        .sort("match_id", "player_slot", "round")
    )

    enriched = (
        cleaned
        .with_columns(
            cooperated=pl.col("action") == "C",
            betrayed=pl.col("opponent_action") == "D",
            mutual_cooperation=(pl.col("action") == "C") & (pl.col("opponent_action") == "C"),
            mutual_defection=(pl.col("action") == "D") & (pl.col("opponent_action") == "D"),
            round_bucket=(((pl.col("round") - 1) // bucket_size) * bucket_size + 1).cast(pl.Int32),
        )
        .with_columns(
            prev_action=pl.col("action").shift(1).over(_PLAYER_WINDOW),
            prev_opponent_action=pl.col("opponent_action").shift(1).over(_PLAYER_WINDOW),
            coop_rate_so_far=(
                pl.col("cooperated").cum_sum() / pl.col("cooperated").cum_count()
            ).over(_PLAYER_WINDOW),
            rolling_coop_rate=pl.col("cooperated")
            .cast(pl.Float64)
            .rolling_mean(window_size=rolling_window, min_samples=1)
            .over(_PLAYER_WINDOW),
            defections_so_far=(pl.col("cooperated").not_()).cum_sum().over(_PLAYER_WINDOW),
        )
        .with_columns(
            # fill_null(False): on round 1 there is no previous round, so
            # neither forgiving nor retaliating is possible (Polars would
            # otherwise propagate null through the boolean AND).
            forgave=pl.col("cooperated")
            & (pl.col("prev_opponent_action") == "D").fill_null(False),
            retaliated=pl.col("cooperated").not_()
            & (pl.col("prev_opponent_action") == "D").fill_null(False),
        )
    )

    return enriched.collect()


def write_silver_run(
    run_id: str,
    root: Path | None = None,
    rolling_window: int = DEFAULT_ROLLING_WINDOW,
    bucket_size: int = DEFAULT_BUCKET_SIZE,
) -> dict[str, Any]:
    """Transform one run and persist it under its Silver partition.

    Returns:
        Summary dict (run_id, n_rows, path) for Dagster metadata.
    """
    frame = build_silver_run(run_id, root, rolling_window, bucket_size)
    path = silver_rounds_dir(root) / f"run_id={run_id}" / "rounds.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(path)
    return {"run_id": run_id, "n_rows": len(frame), "path": str(path)}


def transform_pending(
    root: Path | None = None,
    rolling_window: int = DEFAULT_ROLLING_WINDOW,
    bucket_size: int = DEFAULT_BUCKET_SIZE,
) -> list[dict[str, Any]]:
    """Transform every Bronze run still missing from Silver (idempotent)."""
    return [
        write_silver_run(run_id, root, rolling_window, bucket_size)
        for run_id in pending_run_ids(root)
    ]


def scan_silver_rounds(root: Path | None = None) -> pl.LazyFrame:
    """Lazily scan all Silver runs with the ``run_id`` partition column."""
    return pl.scan_parquet(
        silver_rounds_dir(root) / "**" / "*.parquet",
        hive_partitioning=True,
    )
