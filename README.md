# Convicts' Dilemma — Iterated Prisoner's Dilemma ETL Pipeline

A complete data-engineering pipeline around a simulation of the **iterated
prisoner's dilemma** (Axelrod's 1981 tournament), built for the Ynov M2
"Outils ETL (& ELT)" course.

Coded strategies and local LLM agents (via Ollama) play a round-robin
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

## Quickstart (full replication)

```bash
git clone <this repo> && cd convicts-dilemma
uv sync                                                          # 1. install everything
uv run pytest                                                    # 2. sanity check (29 tests)
uv run dagster asset materialize -m convicts_dilemma.defs \
  --select "bronze_tournament,silver_rounds,int_match_results,tournament_summary,matchup_matrix,behavioral_drift,forgiveness_index,run_catalog,cross_run_summary"   # 3. full pipeline
uv sync --group dashboard
uv run streamlit run app/dashboard.py                            # 4. explore the results
```

To fill the lake with a curated set of **heterogeneous** runs (varied seeds,
payoff matrices, horizons, rosters — what the comparative Gold models are
for), run `uv run python scripts/populate_lake.py` (~1 min) and see the
[data scientist guide](docs/data_scientist_guide.md).

(The explicit asset list is used instead of `--select "*"` because Click
expands `*` against the filesystem on Windows. On PowerShell, replace the
`\` line continuation with a backtick.)

Step 3 plays a complete 10-strategy tournament (55 matches × 2000 rounds),
writes Bronze, enriches Silver with Polars, and builds + tests the Gold dbt
models — about 30 seconds end to end. Re-run it (optionally with different
config, see below) to add more snapshots to the lake. LLM agents are
opt-in and need [Ollama](https://ollama.com) (step 5 below).

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

Through Dagster (recommended — also runs the 24 dbt data tests as asset
checks):

```bash
uv run dagster asset materialize -m convicts_dilemma.defs `
  --select "int_match_results,tournament_summary,matchup_matrix,behavioral_drift,forgiveness_index,run_catalog,cross_run_summary"
```

Or with dbt directly:

```bash
mkdir data/gold      # first time only: DuckDB doesn't create parent dirs
cd dbt
uv run dbt build     # models + data tests
uv run dbt docs generate   # optional: browsable docs + lineage graph
```

The Gold tables (`tournament_summary`, `matchup_matrix`,
`behavioral_drift`, `forgiveness_index`, plus the multi-run views
`run_catalog` and `cross_run_summary`) contain **aggregates only** — no
raw rows. Each is materialised twice by dbt-duckdb's `external` strategy:
as a Parquet file under `data/gold/` and as a view in
`data/gold/gold.duckdb`.

> Note: if you ever add or rename dbt models, refresh Dagster's cached dbt
> manifest with `cd dbt && uv run dbt parse` (or just use `dagster dev`,
> which re-parses automatically).

You can also materialize the **whole pipeline end-to-end** (new tournament
→ Silver → Gold + tests) in one command:

```bash
uv run dagster asset materialize -m convicts_dilemma.defs \
  --select "bronze_tournament,silver_rounds,int_match_results,tournament_summary,matchup_matrix,behavioral_drift,forgiveness_index,run_catalog,cross_run_summary"
```

(`--select "*"` would be shorter but Click expands `*` on Windows.)

### 5. Add LLM agents to the tournament (optional, needs Ollama)

Four LLM personas are available: `llm_empathetic`, `llm_calculating`,
`llm_vengeful`, `llm_opportunist`. They implement the same `Strategy`
interface as the coded roster — the engine doesn't know the difference.

One-time setup:

```bash
# install Ollama: https://ollama.com/download (or: winget install -e --id Ollama.Ollama)
ollama pull llama3.2:3b     # ~2 GB; a 3B q4 model fits in 4 GB of VRAM
ollama serve                # if the server isn't already running
```

Shortcut — a curated LLM experiment (all 4 personas vs `tit_for_tat` and
`always_defect`, fallback rate reported at the end):

```bash
uv run python scripts/populate_lake.py --llm-only
```

Or add personas to the roster of any run via the run config:

```bash
uv run dagster asset materialize --select bronze_tournament -m convicts_dilemma.defs `
  --config-json '{"ops": {"bronze_tournament": {"config": {"strategies": ["tit_for_tat", "always_defect", "grim_trigger", "llm_empathetic", "llm_calculating", "llm_vengeful", "llm_opportunist"], "llm_n_rounds": 200}}}}'
```

How it works:

- Decisions are forced into JSON `{"action": "C"|"D", "reason": "..."}` via
  Ollama's **structured outputs**; the `reason` fills the `reasoning_*`
  columns in Bronze.
- The prompt is **compact**: opponent's last 10 moves + cooperation rates +
  last round, never the full transcript, and never the match length (avoids
  end-game defection by backward induction).
- Matches involving an LLM play `llm_n_rounds` rounds (default 100) instead
  of `n_rounds` — a 3B model takes ~1-4 s per decision on a 4 GB GPU, so
  budget a few minutes per LLM match (start with one or two personas, or
  lower `llm_n_rounds`, before launching the full 4-persona roster).
- Every LLM call is logged to `data/bronze/llm_raw/.../decisions.jsonl`
  (prompt, raw response, latency, fallback flag): the raw provenance zone
  of the generative part.
- If the server is unreachable or replies garbage, the agent **falls back
  to tit-for-tat** for that round and flags it — a tournament never crashes
  mid-run.
- Set `ollama_model` in the run config (or `CONVICTS_OLLAMA_MODEL`) to use
  another model. LLM runs are reproducible best-effort only (a fixed seed
  is sent, but determinism across GPUs/Ollama versions is not guaranteed).

### 6. Explore: the Streamlit dashboard

```bash
uv sync --group dashboard         # adds streamlit + plotly + pandas
uv run streamlit run app/dashboard.py
```

The dashboard is the interactive analyst endpoint of the lake. Like the
notebook it consumes the **Gold layer only**: per-run leaderboard, matchup
heatmap, behavioural drift and forgiveness-vs-performance, plus a
**cross-run comparison** tab that plots how each strategy's rank moves
across runs (i.e. across payoff matrices, horizons, rosters and seeds).
It adapts live to whatever runs exist — materialise a new tournament and
hit *Refresh data*.

Comparisons are only interesting if the lake contains **heterogeneous**
runs. Populate it with the curated 12-experiment plan (replicates,
temptation sweep, horizon sweep, roster compositions...):

```bash
uv run python scripts/populate_lake.py
```

The [data scientist guide](docs/data_scientist_guide.md) documents every
tunable parameter, the validity rules of the payoff matrix, and a recipe
book for designing your own comparative experiments.

### 7. Explore: the EDA notebook

```bash
uv sync --group analysis          # adds matplotlib + jupyterlab
uv run jupyter lab notebooks/analysis.ipynb
```

The notebook consumes the **Gold layer only** (as a data analyst would):
leaderboard, matchup heatmap, behavioural drift, forgiveness-vs-performance,
cross-run rank comparison, and a discussion of the Nash equilibrium vs the
emergence of cooperation. It adapts to whatever runs exist in your lake.

### 8. Query the lake (analyst access)

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
│   │                                             # payoff matrix, model, timestamp
│   ├── rounds/
│   │   └── run_id=<ts>-<uuid>/
│   │       └── match_id=<n>/rounds.parquet       # 1 row per round
│   └── llm_raw/
│       └── run_id=<ts>-<uuid>/
│           └── match_id=<n>/decisions.jsonl      # raw LLM provenance (prompt,
│                                                 # response, latency, fallback)
├── silver/
│   └── rounds/
│       └── run_id=<ts>-<uuid>/rounds.parquet     # 2 rows per round (player-centric)
└── gold/
    ├── gold.duckdb                               # dbt target db (views over the parquet)
    ├── tournament_summary.parquet                # aggregates only, all runs,
    ├── matchup_matrix.parquet                    # one row per (run_id, ...)
    ├── behavioral_drift.parquet
    ├── forgiveness_index.parquet
    ├── run_catalog.parquet                       # 1 row per run: all parameters
    └── cross_run_summary.parquet                 # leaderboards joined with run params
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

## The versioned data lake ("aller plus loin")

The per-`run_id` partitioning answers the three questions of the spec:

**How is each run identified and isolated?** Every materialisation creates
a `run_id` = `<UTC timestamp>-<short uuid>` (sortable + collision-proof)
and writes under fresh `run_id=...` directories — append-only snapshots,
previous runs are never touched. The one-row **manifest** records the full
configuration (seed, rounds, roster, payoff matrix, LLM model, engine
schema version), exposed to analysts as the `run_catalog` Gold table.

**How are snapshots stored without duplication?** A run's data exists
exactly once, under its partition. Nothing is copied between layers either:
Silver reads Bronze and dbt reads Silver as zero-copy external Parquet
sources. The Silver transform is incremental (only runs missing from
Silver are processed), and Gold tables are cheap aggregates keyed by
`run_id`.

**How to query one run, or compare runs?** Hive partitioning makes
`run_id` a queryable column with partition pruning:

```sql
-- one specific run
SELECT * FROM 'data/gold/tournament_summary.parquet' WHERE run_id = '...';

-- find runs by their parameters, then compare them
SELECT * FROM 'data/gold/cross_run_summary.parquet'
WHERE run_id IN (
    SELECT run_id FROM 'data/gold/run_catalog.parquet'
    WHERE payoff_temptation IN (5, 10) AND n_rounds = 2000
)
ORDER BY player, created_at;
```

The `cross_run_summary` model pre-joins every leaderboard with its run's
parameters — "does tit_for_tat still win when betrayal pays double (T=10)?"
is a one-liner.

## Architecture choices

| Choice | Why |
|---|---|
| Medallion Bronze/Silver/Gold | Raw events are never mutated (replayable); enrichment and aggregates are rebuildable from below; Gold contains aggregates only, per the spec |
| Parquet + Hive partitioning | Columnar, compressed, partition pruning; `run_id` in the path is the snapshot mechanism |
| Dagster | Software-defined assets map 1:1 onto the medallion layers; lineage, run config and dbt tests visible in one UI |
| Polars (Silver) | Lazy, fast window expressions (`shift`, `cum_sum`, `rolling_mean` `.over(...)`) are exactly the derived-column workload |
| dbt-duckdb (Gold) | SQL aggregates with data tests + generated docs; DuckDB reads the Parquet lake in place, zero copies |
| DuckDB (serving) | The analyst endpoint: one `duckdb` import queries every layer |
| Strategy interface | Coded strategies and Ollama agents are interchangeable players; the pipeline is agnostic to who decides |

```
src/convicts_dilemma/
├── strategies/   # Strategy interface + 10 coded Axelrod strategies
├── agents/       # Ollama LLM personas (same interface)
├── simulation/   # match engine + seeded round-robin tournament
├── pipeline/     # bronze.py (write/scan), silver.py (Polars transform)
└── defs/         # Dagster assets incl. dagster-dbt integration
dbt/              # Gold models, data tests, sources over the Parquet lake
app/              # Streamlit dashboard (Gold-only consumption)
notebooks/        # EDA notebook (Gold-only consumption)
scripts/          # populate_lake.py: curated heterogeneous experiment plan
docs/             # data scientist guide (experiment design + recipe book)
tests/            # 29 pytest tests (engine, layers, agents, dbt end-to-end)
```

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
| `llm_empathetic` | LLM persona: trusting, seeks mutual benefit, quick to forgive |
| `llm_calculating` | LLM persona: cold expected-value maximiser |
| `llm_vengeful` | LLM persona: cooperative until crossed, punishes hard |
| `llm_opportunist` | LLM persona: exploits naive opponents when it looks safe |

## Reproducibility

- One master `seed` per run; every stochastic strategy gets its own RNG
  seeded with the stable string `"{seed}:{match_id}:{slot}"` (SHA-512
  string seeding — identical across processes and Python versions).
- All run parameters are recorded in the run's manifest.
- Dependencies are locked in `uv.lock`.
