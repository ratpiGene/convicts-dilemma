# Convicts' Dilemma — Iterated Prisoner's Dilemma ETL Pipeline

A complete data-engineering pipeline around a simulation of the **iterated
prisoner's dilemma** (Axelrod's 1981 tournament), built for the Ynov M2
"Outils ETL (& ELT)" course.

Coded strategies (and later, local LLM agents via Ollama) play a round-robin
tournament. Every round is recorded and flows through a **medallion
architecture** on local Parquet:

```
 Generate                Bronze                  Silver                Gold
┌──────────────┐   ┌─────────────────┐   ┌─────────────────┐   ┌──────────────────┐
│ Tournament    │   │ Raw rounds       │   │ Cleaned +        │   │ dbt models on    │
│ engine        │──▶│ Parquet, Hive-   │──▶│ enriched rounds  │──▶│ DuckDB: aggre-   │
│ (seeded)      │   │ partitioned by   │   │ (Polars)         │   │ gates only       │
│               │   │ run_id/match_id  │   │                  │   │                  │
└──────────────┘   └─────────────────┘   └─────────────────┘   └──────────────────┘
        orchestrated end-to-end by Dagster · analyst access via DuckDB SQL
```

**Stack**: Python 3.13 · [uv](https://docs.astral.sh/uv/) · Dagster · Polars · DuckDB · dbt-duckdb · Parquet

## Project status

| Stage | Status |
|---|---|
| Bronze — tournament generation + raw Parquet | ✅ done |
| Silver — Polars enrichment | 🚧 next |
| Gold — dbt-duckdb aggregate models | 🚧 planned |
| Ollama LLM agents | 🚧 planned |
| Multi-run comparison + EDA notebook | 🚧 planned |

## Setup

Prerequisites: [uv](https://docs.astral.sh/uv/getting-started/installation/)
(manages Python 3.13 and the virtualenv for you).

```bash
git clone <this repo>
cd convicts-dilemma
uv sync          # creates .venv and installs everything from uv.lock
```

No data ships with the repo (by design — see the project spec): you
regenerate everything locally with the commands below.

## How to: run each step

### 1. Run the test suite

```bash
uv run pytest
```

### 2. Generate a tournament snapshot (Bronze)

Either through the **Dagster UI**:

```bash
uv run dagster dev          # then open http://127.0.0.1:3000
```

In the UI: *Assets → bronze_tournament → Materialize*. Use *Open launchpad*
instead to override the run config (seed, rounds, roster, payoff matrix).

Or directly from the **CLI** with default config (10 strategies, 2000
rounds, seed 42, Axelrod payoffs):

```bash
uv run dagster asset materialize --select bronze_tournament -m convicts_dilemma.defs
```

To override the config from the CLI, pass `--config-json`:

```bash
uv run dagster asset materialize --select bronze_tournament -m convicts_dilemma.defs `
  --config-json '{"ops": {"bronze_tournament": {"config": {"seed": 7, "n_rounds": 500, "strategies": ["tit_for_tat", "always_defect", "pavlov"]}}}}'
```

Every materialisation creates a **new immutable snapshot** under a fresh
`run_id` — previous runs are never overwritten. Re-running with the same
seed and parameters reproduces the exact same actions and payoffs.

### 3. Query the lake (analyst access)

```python
import duckdb

duckdb.sql("""
    SELECT * FROM read_parquet(
        'data/bronze/rounds/*/*/*.parquet', hive_partitioning=true)
    WHERE run_id = '<your run id>'
""")

# All runs ever generated, with their parameters:
duckdb.sql("""
    SELECT * FROM read_parquet(
        'data/bronze/manifests/*/*.parquet', hive_partitioning=true)
""")
```

## Data layout

```
data/                                   # git-ignored, regenerated locally
└── bronze/
    ├── manifests/
    │   └── run_id=<ts>-<uuid>/manifest.parquet   # 1 row: seed, rounds, roster,
    │                                             # payoff matrix, timestamp
    └── rounds/
        └── run_id=<ts>-<uuid>/
            └── match_id=<n>/rounds.parquet       # 1 row per round
```

Rounds schema: `player_a`, `player_b`, `round`, `action_a`, `action_b`
(`"C"`/`"D"`), `payoff_a`, `payoff_b`, `cumulative_a`, `cumulative_b`,
`reasoning_a`, `reasoning_b` (LLM justification, null for coded strategies).
`run_id` and `match_id` come from the Hive partition paths.

This per-`run_id` partitioning is the project's **versioned data lake**
answer: each run is an isolated snapshot, manifests make runs discoverable
by their parameters, and comparing runs is a partition-filtered query.

## The strategies

| Name | Behaviour |
|---|---|
| `always_cooperate` | Cooperates unconditionally |
| `always_defect` | Defects unconditionally |
| `tit_for_tat` | Cooperates first, then mirrors the opponent's last move |
| `suspicious_tit_for_tat` | Tit for tat, but opens with a defection |
| `generous_tit_for_tat` | Tit for tat that forgives a defection 10% of the time |
| `tit_for_two_tats` | Defects only after two consecutive opponent defections |
| `grim_trigger` | Cooperates until betrayed once, then defects forever |
| `pavlov` | Win-stay / lose-shift |
| `joss` | Tit for tat that sneaks a defection 10% of the time |
| `random` | Coin flip every round |

## Reproducibility

- One master `seed` per run; every stochastic strategy gets its own RNG
  seeded with the stable string `"{seed}:{match_id}:{slot}"` (SHA-512
  string seeding — identical across processes and Python versions).
- All run parameters are recorded in the run's manifest.
- Dependencies are locked in `uv.lock`.
