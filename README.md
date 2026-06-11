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
| Silver — Polars enrichment | ✅ done |
| Gold — dbt-duckdb aggregate models | ✅ done |
| Ollama LLM agents | 🚧 next |
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

### 3. Enrich the rounds (Silver)

```bash
uv run dagster asset materialize --select silver_rounds -m convicts_dilemma.defs
```

(or materialize it from the Dagster UI). The Silver transform is
**incremental and idempotent**: it processes exactly the Bronze runs that
have no Silver partition yet, so you can re-run it any time. The rolling
window (default 50 rounds) and the drift bucket size (default 100) are
exposed as run config.

### 4. Build the Gold aggregates (dbt)

Through Dagster (recommended — also runs the 16 dbt data tests as asset
checks):

```bash
uv run dagster asset materialize -m convicts_dilemma.defs `
  --select "int_match_results,tournament_summary,matchup_matrix,behavioral_drift,forgiveness_index"
```

Or with dbt directly:

```bash
mkdir data/gold      # first time only: DuckDB doesn't create parent dirs
cd dbt
uv run dbt build     # models + data tests
uv run dbt docs generate   # optional: browsable docs + lineage graph
```

The four Gold tables (`tournament_summary`, `matchup_matrix`,
`behavioral_drift`, `forgiveness_index`) contain **aggregates only** — no
raw rows. Each is materialised twice by dbt-duckdb's `external` strategy:
as a Parquet file under `data/gold/` and as a view in
`data/gold/gold.duckdb`.

You can also materialize the **whole pipeline end-to-end** (new tournament
→ Silver → Gold + tests) in one command:

```bash
uv run dagster asset materialize --select "*" -m convicts_dilemma.defs
```

### 5. Query the lake (analyst access)

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
├── bronze/
│   ├── manifests/
│   │   └── run_id=<ts>-<uuid>/manifest.parquet   # 1 row: seed, rounds, roster,
│   │                                             # payoff matrix, timestamp
│   └── rounds/
│       └── run_id=<ts>-<uuid>/
│           └── match_id=<n>/rounds.parquet       # 1 row per round
├── silver/
│   └── rounds/
│       └── run_id=<ts>-<uuid>/rounds.parquet     # 2 rows per round (player-centric)
└── gold/
    ├── gold.duckdb                               # dbt target db (views over the parquet)
    ├── tournament_summary.parquet                # aggregates only, all runs,
    ├── matchup_matrix.parquet                    # one row per (run_id, ...)
    ├── behavioral_drift.parquet
    └── forgiveness_index.parquet
```

**Bronze rounds** (match-centric): `player_a`, `player_b`, `round`,
`action_a`, `action_b` (`"C"`/`"D"`), `payoff_a`, `payoff_b`,
`cumulative_a`, `cumulative_b`, `reasoning_a`, `reasoning_b` (LLM
justification, null for coded strategies). `run_id` and `match_id` come
from the Hive partition paths.

**Silver rounds** (player-centric, one row per player per round): keys
`match_id`, `round`, `player_slot`; identity `player`, `opponent`; facts
`action`, `opponent_action`, `payoff`, `cumulative_score`, `reasoning`;
derived features `cooperated`, `betrayed`, `mutual_cooperation`,
`mutual_defection`, `prev_action`, `prev_opponent_action` (1-round memory),
`coop_rate_so_far` (expanding), `rolling_coop_rate` (last 50 rounds),
`defections_so_far`, `forgave` (cooperated right after being betrayed),
`retaliated` (defected right after being betrayed), `round_bucket`
(1, 101, 201... — behavioural-drift grain).

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
