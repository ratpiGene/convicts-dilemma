"""Bronze layer: persist raw tournament output as Hive-partitioned Parquet.

Layout (under the data root, see :func:`convicts_dilemma.config.data_root`):

```
bronze/
├── manifests/run_id=<id>/manifest.parquet      # 1 row: the run parameters
└── rounds/run_id=<id>/match_id=<n>/rounds.parquet
```

Design notes:

- Partition columns (``run_id``, ``match_id``) live **only in the path**,
  not inside the files — DuckDB and Polars both re-materialise them when
  reading with ``hive_partitioning``. This avoids column-duplication
  conflicts and keeps files smaller.
- One run = one new ``run_id=`` directory: previous runs are never touched,
  which is the snapshot-isolation property of the versioned lake.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from convicts_dilemma.config import data_root
from convicts_dilemma.simulation.tournament import TournamentResult

#: Explicit schema of one rounds file. Enforced at write time so every run
#: in the lake is union-compatible.
ROUNDS_SCHEMA: dict[str, pl.DataType] = {
    "player_a": pl.Utf8,
    "player_b": pl.Utf8,
    "round": pl.Int32,
    "action_a": pl.Utf8,
    "action_b": pl.Utf8,
    "payoff_a": pl.Int32,
    "payoff_b": pl.Int32,
    "cumulative_a": pl.Int64,
    "cumulative_b": pl.Int64,
    "reasoning_a": pl.Utf8,
    "reasoning_b": pl.Utf8,
}


def bronze_rounds_dir(root: Path | None = None) -> Path:
    """Directory holding every run's rounds partitions."""
    return (root or data_root()) / "bronze" / "rounds"


def bronze_manifests_dir(root: Path | None = None) -> Path:
    """Directory holding every run's manifest partitions."""
    return (root or data_root()) / "bronze" / "manifests"


def write_bronze(result: TournamentResult, root: Path | None = None) -> dict[str, object]:
    """Write one tournament run to the Bronze layer.

    Args:
        result: Output of :func:`convicts_dilemma.simulation.run_tournament`.
        root: Data-lake root override (tests); defaults to ``data_root()``.

    Returns:
        Summary dict (run_id, n_matches, n_rows, paths) — surfaced as
        Dagster materialisation metadata.
    """
    run_partition = f"run_id={result.run_id}"

    manifest_path = bronze_manifests_dir(root) / run_partition / "manifest.parquet"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_df = pl.DataFrame([{k: v for k, v in result.manifest.items() if k != "run_id"}])
    manifest_df.write_parquet(manifest_path)

    n_rows = 0
    rounds_run_dir = bronze_rounds_dir(root) / run_partition
    for match in result.matches:
        match_df = pl.DataFrame(
            [
                {"player_a": match.player_a, "player_b": match.player_b, **record}
                for record in match.rounds
            ],
            schema=ROUNDS_SCHEMA,
        )
        match_path = rounds_run_dir / f"match_id={match.match_id}" / "rounds.parquet"
        match_path.parent.mkdir(parents=True, exist_ok=True)
        match_df.write_parquet(match_path)
        n_rows += len(match_df)

    return {
        "run_id": result.run_id,
        "n_matches": len(result.matches),
        "n_rows": n_rows,
        "rounds_path": str(rounds_run_dir),
        "manifest_path": str(manifest_path),
    }


def scan_bronze_rounds(root: Path | None = None) -> pl.LazyFrame:
    """Lazily scan **all** runs' rounds with Hive partition columns.

    The returned LazyFrame includes ``run_id`` and ``match_id`` columns
    materialised from the directory names; filter on ``run_id`` to read a
    single snapshot (partition pruning keeps this cheap).
    """
    return pl.scan_parquet(
        bronze_rounds_dir(root) / "**" / "*.parquet",
        hive_partitioning=True,
    )


def scan_bronze_manifests(root: Path | None = None) -> pl.LazyFrame:
    """Lazily scan every run manifest (one row per run), with ``run_id``."""
    return pl.scan_parquet(
        bronze_manifests_dir(root) / "**" / "*.parquet",
        hive_partitioning=True,
    )
