"""Dagster assets orchestrating the pipeline.

Run config (seed, rounds, roster...) is exposed through Dagster's run
configuration, so a new tournament snapshot is launched either from the UI
(Materialize → with config) or from the CLI — see the README "how to".
"""

import dagster as dg

from convicts_dilemma.config import PayoffMatrix
from convicts_dilemma.pipeline.bronze import write_bronze
from convicts_dilemma.pipeline.silver import (
    DEFAULT_BUCKET_SIZE,
    DEFAULT_ROLLING_WINDOW,
    transform_pending,
)
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
    # LLM agents (opt-in): add e.g. "llm_empathetic" to `strategies`.
    # Requires a running Ollama server with `ollama_model` pulled.
    llm_n_rounds: int = 100
    ollama_model: str = "llama3.2:3b"

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
            llm_n_rounds=self.llm_n_rounds,
            ollama_model=self.ollama_model,
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


class SilverConfig(dg.Config):
    """Tunable enrichment parameters of the Silver transform."""

    rolling_window: int = DEFAULT_ROLLING_WINDOW
    bucket_size: int = DEFAULT_BUCKET_SIZE


@dg.asset(
    deps=[bronze_tournament],
    group_name="silver",
    kinds={"polars", "parquet"},
    description=(
        "Cleaned, player-centric, feature-enriched rounds built with Polars. "
        "Unpivots Bronze into two rows per round (one per player perspective) "
        "and adds windowed features: 1-round memory, expanding/rolling "
        "cooperation rates, forgiveness and retaliation flags, round buckets. "
        "Incremental: only Bronze runs without a Silver partition are "
        "processed, so re-materialising never duplicates data."
    ),
)
def silver_rounds(
    context: dg.AssetExecutionContext, config: SilverConfig
) -> dg.MaterializeResult:
    """Transform every pending Bronze run into the Silver layer."""
    summaries = transform_pending(
        rolling_window=config.rolling_window, bucket_size=config.bucket_size
    )
    for summary in summaries:
        context.log.info("Silver written for run %s: %s rows", summary["run_id"], summary["n_rows"])
    if not summaries:
        context.log.info("No pending Bronze runs — Silver already up to date.")
    return dg.MaterializeResult(
        metadata={
            "processed_runs": dg.MetadataValue.json([s["run_id"] for s in summaries]),
            "n_runs": dg.MetadataValue.int(len(summaries)),
            "n_rows": dg.MetadataValue.int(sum(int(s["n_rows"]) for s in summaries)),  # type: ignore[arg-type]
        }
    )
