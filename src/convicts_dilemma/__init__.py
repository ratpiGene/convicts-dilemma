"""convicts_dilemma — iterated prisoner's dilemma tournament + data pipeline.

The package is organised along the pipeline stages:

- :mod:`convicts_dilemma.strategies` — the decision-making agents (coded
  strategies now, Ollama-backed LLM agents later). All implement the same
  :class:`~convicts_dilemma.strategies.base.Strategy` interface.
- :mod:`convicts_dilemma.simulation` — the game engine (one match) and the
  tournament scheduler (round-robin between all strategies).
- :mod:`convicts_dilemma.pipeline` — the medallion layers: Bronze (raw
  round-by-round Parquet, Hive-partitioned by ``run_id``), Silver (Polars
  enrichment) and the hand-off to the dbt Gold layer.
- :mod:`convicts_dilemma.defs` — the Dagster definitions (assets) that
  orchestrate everything.
"""

__version__ = "0.1.0"
