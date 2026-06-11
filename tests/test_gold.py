"""End-to-end Gold test: Bronze -> Silver -> dbt build in a throwaway lake.

Invokes dbt programmatically (dbtRunner) against the real dbt project, with
CONVICTS_DATA_DIR pointing at a tmp lake — exactly what a grader replicating
the project experiences, minus Dagster.
"""

from pathlib import Path

import duckdb
import pytest
from dbt.cli.main import dbtRunner

from convicts_dilemma.pipeline.bronze import write_bronze
from convicts_dilemma.pipeline.silver import transform_pending
from convicts_dilemma.simulation import TournamentConfig, run_tournament

REPO_ROOT = Path(__file__).resolve().parents[1]
DBT_DIR = REPO_ROOT / "dbt"

ROSTER = ("tit_for_tat", "always_defect", "pavlov", "grim_trigger")


@pytest.fixture()
def gold_lake(tmp_path, monkeypatch):
    """Tiny tournament taken all the way through dbt build."""
    result = run_tournament(TournamentConfig(strategies=ROSTER, n_rounds=120, seed=11))
    write_bronze(result, root=tmp_path)

    # data_root() and the dbt profile both honour CONVICTS_DATA_DIR.
    monkeypatch.setenv("CONVICTS_DATA_DIR", str(tmp_path))
    transform_pending()
    (tmp_path / "gold").mkdir()

    outcome = dbtRunner().invoke(
        [
            "build",
            "--project-dir", str(DBT_DIR),
            "--profiles-dir", str(DBT_DIR),
            "--target-path", str(tmp_path / "dbt_target"),
        ]
    )
    assert outcome.success, "dbt build (models + data tests) failed"
    return tmp_path, result


def test_gold_tables_shape_and_semantics(gold_lake):
    root, result = gold_lake
    # No read_only: dbt-duckdb's in-process connection to this file is still
    # cached, and DuckDB only shares a database between connections whose
    # configuration matches exactly.
    con = duckdb.connect(str(root / "gold" / "gold.duckdb"))

    # Exported Parquet files exist next to the database.
    for model in ("tournament_summary", "matchup_matrix", "behavioral_drift", "forgiveness_index"):
        assert (root / "gold" / f"{model}.parquet").exists()

    # tournament_summary: one row per strategy, and every player appears in
    # 5 perspectives (3 opponents + both sides of self-play).
    summary = con.sql(
        "SELECT player, n_matches, rank, coop_rate, total_score"
        " FROM tournament_summary ORDER BY rank, player"
    ).fetchall()
    assert len(summary) == len(ROSTER)
    assert all(row[1] == 5 for row in summary)
    # rank() semantics: starts at 1, ties share a rank (TFT and grim_trigger
    # score identically against this roster), scores descend with rank.
    ranks = [row[2] for row in summary]
    scores = [row[4] for row in summary]
    assert ranks[0] == 1
    assert all(1 <= r <= len(ROSTER) for r in ranks)
    assert scores == sorted(scores, reverse=True)

    # always_defect never cooperates; that's a hard semantic invariant.
    (ad_coop,) = [row[3] for row in summary if row[0] == "always_defect"]
    assert ad_coop == 0.0

    # matchup_matrix is the full cross product of the roster.
    (n_matchups,) = con.sql("SELECT count(*) FROM matchup_matrix").fetchone()
    assert n_matchups == len(ROSTER) ** 2

    # behavioral_drift: 120 rounds with default bucket size 100 -> buckets
    # starting at 1 and 101 for each player.
    drift = con.sql(
        "SELECT DISTINCT round_bucket FROM behavioral_drift ORDER BY round_bucket"
    ).fetchall()
    assert [row[0] for row in drift] == [1, 101]

    # forgiveness_index: never-forgiving strategies stay at zero events,
    # and rates are valid (redundant with the dbt is_rate test, on purpose).
    forgiveness = dict(
        con.sql("SELECT player, forgiveness_events FROM forgiveness_index").fetchall()
    )
    assert forgiveness["always_defect"] == 0
    assert forgiveness["grim_trigger"] == 0
    assert forgiveness["pavlov"] > 0  # alternates C/D vs defectors
