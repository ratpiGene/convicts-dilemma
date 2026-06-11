"""Dagster assets orchestrating the pipeline.

Run config (seed, rounds, roster...) is exposed through Dagster's run
configuration, so a new tournament snapshot is launched either from the UI
(Materialize → with config) or from the CLI — see the README "how to".
"""

import dagster as dg

from convicts_dilemma.config import PayoffMatrix
from convicts_dilemma.pipeline.bronze import write_bronze
from convicts_dilemma.simulation.tournament import TournamentConfig, run_tournament
from convicts_dilemma.strategies import DEFAULT_ROSTER


class TournamentRunConfig(dg.Config):
    """Dagster-facing mirror of :class:`TournamentConfig` (flat & validated).

    Defaults reproduce the project-spec example: full coded roster,
    2000 rounds, Axelrod payoffs, fixed seed.
    """

    strategies: list[str] = list(DEFAULT_ROSTER)
    n_rounds: int = 2000
    seed: int = 42
    include_self_play: bool = True
    payoff_reward: int = 3
    payoff_temptation: int = 5
    payoff_sucker: int = 0
    payoff_punishment: int = 1

    def to_domain(self) -> TournamentConfig:
        """Convert to the plain domain config used by the simulation."""
        return TournamentConfig(
            strategies=tuple(self.strategies),
            n_rounds=self.n_rounds,
            seed=self.seed,
            include_self_play=self.include_self_play,
            payoff=PayoffMatrix(
                reward=self.payoff_reward,
                temptation=self.payoff_temptation,
                sucker=self.payoff_sucker,
                punishment=self.payoff_punishment,
            ),
        )


@dg.asset(
    group_name="bronze",
    kinds={"python", "parquet"},
    description=(
        "Raw tournament snapshot: plays a seeded round-robin between the "
        "configured strategies and writes round-by-round Parquet under "
        "data/bronze/, Hive-partitioned by run_id and match_id, plus a "
        "one-row run manifest. Each materialisation creates a NEW run_id "
        "(append-only snapshots — previous runs are never overwritten)."
    ),
)
def bronze_tournament(
    context: dg.AssetExecutionContext, config: TournamentRunConfig
) -> dg.MaterializeResult:
    """Generate one tournament run and persist it to the Bronze layer."""
    result = run_tournament(config.to_domain())
    summary = write_bronze(result)
    context.log.info(
        "Run %s written: %s matches, %s rows",
        summary["run_id"],
        summary["n_matches"],
        summary["n_rows"],
    )
    return dg.MaterializeResult(
        metadata={
            "run_id": dg.MetadataValue.text(str(summary["run_id"])),
            "n_matches": dg.MetadataValue.int(int(summary["n_matches"])),  # type: ignore[arg-type]
            "n_rows": dg.MetadataValue.int(int(summary["n_rows"])),  # type: ignore[arg-type]
            "rounds_path": dg.MetadataValue.path(str(summary["rounds_path"])),
            "manifest_path": dg.MetadataValue.path(str(summary["manifest_path"])),
        }
    )
