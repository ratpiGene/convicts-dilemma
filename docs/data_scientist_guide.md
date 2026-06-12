# Data Scientist Guide — designing comparative experiments

This guide is for the **consumer** of the lake: how to launch tournaments
with deliberately varied parameters so the Bronze layer fills with
*heterogeneous* runs, and how to exploit the cross-run Gold models to answer
comparative questions ("does tit_for_tat still win when betrayal pays
double?"). For installation and pipeline mechanics, see the
[README](../README.md).

## TL;DR

```bash
uv run python scripts/populate_lake.py        # 12 curated runs: Bronze → Silver → Gold (~1 min)
uv run python scripts/populate_lake.py --llm-only   # + the LLM persona face-off (needs Ollama, ~10-25 min)
uv sync --group dashboard
uv run streamlit run app/dashboard.py         # interactive exploration of the Gold layer
```

`populate_lake.py` materialises the experiment plan described in the
[recipe book](#3-the-recipe-book) below. The rest of this guide explains how
to design and run **your own** experiments.

## 1. The mental model: an append-only lake of snapshots

Every tournament you launch becomes one **immutable snapshot** identified by
a `run_id` (`<UTC timestamp>-<short uuid>`). Nothing is ever overwritten:

- each run's raw rounds land under `data/bronze/rounds/run_id=…/`;
- a one-row **manifest** records every parameter of the run (seed, payoffs,
  roster, horizon, model…) — surfaced to you as the Gold `run_catalog` table;
- Silver and Gold rebuild incrementally on top, so generating a new run is
  always the same three steps: **Bronze → Silver → Gold**.

Two consequences for experiment design:

1. **The lake only answers the questions you stored contrasts for.** Ten
   runs with identical config give you variance of *nothing* (the engine is
   deterministic: same config ⇒ byte-identical data). Heterogeneity must be
   injected on purpose — that is what this guide is about.
2. **You never need to be afraid of running an experiment.** Bad idea? The
   run just sits in its own partition; filter it out with `run_catalog`.
   The whole `data/` directory is disposable and regenerable.

## 2. Every knob you can turn

All tournament parameters go through the `bronze_tournament` run config
(Dagster launchpad or `--config-json` on the CLI); Silver has two knobs of
its own.

### Tournament (`bronze_tournament` config)

| Parameter | Default | What it controls | Why vary it |
|---|---|---|---|
| `seed` | `42` | Master seed; every stochastic strategy derives its RNG from it | Replicates: same rules, different randomness → is a ranking robust or a fluke? |
| `n_rounds` | `2000` | Rounds per match | The "shadow of the future": short horizons reward defection, long ones reward cooperation |
| `strategies` | all 10 coded | Roster entering the round-robin | Population composition: cooperation is an *ecological* outcome — a strategy's rank depends on who else is in the pool |
| `include_self_play` | `true` | Each strategy also plays its own copy | Self-play inflates nice strategies' totals (they fully cooperate with themselves) |
| `payoff_reward` (R) | `3` | Both cooperate | Raise to make cooperation more attractive |
| `payoff_temptation` (T) | `5` | I defect, they cooperate | The greed dial — the most interesting one to sweep |
| `payoff_sucker` (S) | `0` | I cooperate, they defect | Lower (e.g. `-2`) to make being betrayed costly |
| `payoff_punishment` (P) | `1` | Both defect | Raise towards R to make mutual defection almost OK |
| `llm_n_rounds` | `100` | Horizon of LLM-involved matches | LLM latency budget (see README §5) |
| `ollama_model` | `llama3.2:3b` | Model behind the LLM personas | Compare personas across model sizes |

### Silver (`silver_rounds` config)

| Parameter | Default | What it controls |
|---|---|---|
| `rolling_window` | `50` | Window of `rolling_coop_rate` (short-term mood of a player) |
| `bucket_size` | `100` | Grain of `round_bucket`, i.e. the x-axis of `behavioral_drift` |

For short matches (`n_rounds=100`), drop `bucket_size` to ~10, otherwise the
drift table has a single bucket and the drift plot is flat by construction.

### Environment variables

| Variable | Effect |
|---|---|
| `CONVICTS_DATA_DIR` | Redirect the whole lake (use an **absolute** path — dbt runs with cwd `dbt/`, so a relative path would point Python and dbt at different places). Perfect for throwaway sandboxes: experiment in a temp dir, delete it, your main lake is untouched. |
| `CONVICTS_OLLAMA_MODEL` | Default Ollama model (overridden by the run config). |

### The rules of a valid dilemma

The canonical constraints on the payoff matrix:

- **T > R > P > S** — otherwise it is not a prisoner's dilemma at all;
- **2R > T + S** — specific to the *iterated* game: mutual cooperation must
  beat taking turns exploiting each other.

The engine does **not** enforce them — deliberately. Breaking the second
one (e.g. `T=10, R=3`: 2R=6 < T+S=10) creates a "greed trap" regime where
alternating exploitation out-earns steady cooperation, which is itself an
interesting comparison point. Just *know* when you are outside the valid
region and label your analysis accordingly (the manifest records the matrix,
so `run_catalog` lets you tell the regimes apart afterwards).

## 3. The recipe book

Each recipe states the **question**, the **command**, and **where to look**
in Gold. Commands are PowerShell-ready (backtick continuation; on bash use
`\`). They all follow the same skeleton — only the `--config-json` changes —
and every materialisation must be followed by the Silver + Gold rebuild
(recipe 0).

> Shortcut: `scripts/populate_lake.py` runs recipes 1–6 in one go, Gold
> included. Read on if you want to understand them or craft variations.

### Recipe 0 — the rebuild step (after any number of Bronze runs)

```powershell
uv run dagster asset materialize -m convicts_dilemma.defs `
  --select "silver_rounds,int_match_results,tournament_summary,matchup_matrix,behavioral_drift,forgiveness_index,run_catalog,cross_run_summary"
```

Silver is incremental (only new runs are processed) and Gold aggregates are
cheap: launch several Bronze runs back-to-back, then rebuild once.

### Recipe 1 — replicates: is the ranking seed-stable?

Three runs, identical rules, different seeds. Only the stochastic
strategies (`generous_tit_for_tat`, `joss`, `random`) change behaviour.

```powershell
foreach ($s in 41, 42, 43) {
  uv run dagster asset materialize --select bronze_tournament -m convicts_dilemma.defs `
    --config-json ('{"ops": {"bronze_tournament": {"config": {"seed": ' + $s + '}}}}')
}
```

**Look at**: `cross_run_summary` filtered on these run_ids — the dashboard's
cross-run tab plots rank per strategy per run. If a strategy's rank moves
by more than a place or two between seeds, treat any single-run conclusion
about it as noise.

### Recipe 2 — temptation sweep: how much greed kills cooperation?

Same seed and horizon, only the payoff matrix moves. Three regimes:

```powershell
# gentle world: defection barely pays (T=4, still T>R>P>S and 2R>T+S)
uv run dagster asset materialize --select bronze_tournament -m convicts_dilemma.defs `
  --config-json '{"ops": {"bronze_tournament": {"config": {"payoff_temptation": 4}}}}'

# high stakes but still a valid iterated dilemma: T=10 requires R=6 (2R > T+S)
uv run dagster asset materialize --select bronze_tournament -m convicts_dilemma.defs `
  --config-json '{"ops": {"bronze_tournament": {"config": {"payoff_temptation": 10, "payoff_reward": 6}}}}'

# greed trap: T=10 with R=3 violates 2R > T+S — exploitation cycles beat cooperation
uv run dagster asset materialize --select bronze_tournament -m convicts_dilemma.defs `
  --config-json '{"ops": {"bronze_tournament": {"config": {"payoff_temptation": 10}}}}'
```

**Look at**: `cross_run_summary` — does `tit_for_tat` keep its crown at
T=10? `matchup_matrix` — which pairings flip from green to red?
`forgiveness_index` — forgiveness should pay less as T grows.

### Recipe 3 — the shadow of the future (horizon sweep)

Cooperation is rational only if the relationship lasts. Sweep `n_rounds`:

```powershell
foreach ($n in 100, 500, 2000) {
  uv run dagster asset materialize --select bronze_tournament -m convicts_dilemma.defs `
    --config-json ('{"ops": {"bronze_tournament": {"config": {"n_rounds": ' + $n + '}}}}')
}
```

For the 100-round run, rebuild Silver with a finer drift grain
(`bucket_size=10`), otherwise `behavioral_drift` has one bucket:

```powershell
uv run dagster asset materialize --select silver_rounds -m convicts_dilemma.defs `
  --config-json '{"ops": {"silver_rounds": {"config": {"bucket_size": 10}}}}'
```

**Look at**: `avg_score_per_round` in `cross_run_summary` (totals are not
comparable across horizons — always use per-round metrics here), and the
relative rank of `always_defect` as the horizon shrinks.

### Recipe 4 — population composition: ecology, not duels

A strategy's success depends on the pool it swims in.

```powershell
# nice-only pool: who wins when nobody attacks first?
uv run dagster asset materialize --select bronze_tournament -m convicts_dilemma.defs `
  --config-json '{"ops": {"bronze_tournament": {"config": {"strategies": ["always_cooperate", "tit_for_tat", "generous_tit_for_tat", "tit_for_two_tats", "grim_trigger", "pavlov"]}}}}'

# hostile pool: can tit_for_tat survive surrounded by aggressors and noise?
uv run dagster asset materialize --select bronze_tournament -m convicts_dilemma.defs `
  --config-json '{"ops": {"bronze_tournament": {"config": {"strategies": ["always_defect", "suspicious_tit_for_tat", "grim_trigger", "joss", "random", "tit_for_tat"]}}}}'
```

**Look at**: `tournament_summary` of each run, and `behavioral_drift` in the
hostile pool — watch `grim_trigger` lock into permanent defection after the
first betrayal while `tit_for_tat` keeps re-testing cooperation.

### Recipe 5 — costly betrayal and near-indifferent punishment

Two more corners of the payoff space:

```powershell
# being suckered hurts (S = -2): does caution (suspicious_tit_for_tat) finally pay?
uv run dagster asset materialize --select bronze_tournament -m convicts_dilemma.defs `
  --config-json '{"ops": {"bronze_tournament": {"config": {"payoff_sucker": -2}}}}'

# mutual defection almost as good as cooperation (P = 2): why bother being nice?
uv run dagster asset materialize --select bronze_tournament -m convicts_dilemma.defs `
  --config-json '{"ops": {"bronze_tournament": {"config": {"payoff_punishment": 2}}}}'
```

### Recipe 6 — self-play off

```powershell
uv run dagster asset materialize --select bronze_tournament -m convicts_dilemma.defs `
  --config-json '{"ops": {"bronze_tournament": {"config": {"include_self_play": false}}}}'
```

**Look at**: compare with a baseline run in `cross_run_summary` — nice
strategies lose the guaranteed all-cooperate points they harvest against
their own copy; `n_matches` drops from 55 to 45.

### Recipe 7 — LLM personas (optional, needs Ollama)

Shortcut — the curated "persona face-off" (all 4 personas vs a nice and a
nasty anchor, 25 rounds per LLM match, ~10-25 min, fallback rate reported):

```powershell
uv run python scripts/populate_lake.py --llm-only   # or --llm for coded plan + face-off
```

Or mix language-model players into any of the above by hand (see README §5
for setup):

```powershell
uv run dagster asset materialize --select bronze_tournament -m convicts_dilemma.defs `
  --config-json '{"ops": {"bronze_tournament": {"config": {"strategies": ["tit_for_tat", "always_defect", "grim_trigger", "llm_empathetic", "llm_calculating"], "llm_n_rounds": 100}}}}'
```

Budget ~1–4 s per LLM decision. **Look at**: `reasoning` provenance in
`data/bronze/llm_raw/`, the personas' `forgiveness_index`, and whether
`llm_calculating` actually behaves differently from `llm_empathetic` —
on a 3B model it often doesn't, which is a finding in itself.

## 4. Exploiting the heterogeneous lake

### Start from `run_catalog`

It is the discovery index — one row per run with every parameter:

```python
import duckdb

duckdb.sql("""
    SELECT run_id, created_at, seed, n_rounds,
           payoff_temptation AS T, payoff_reward AS R, len(strategies) AS n_strats
    FROM 'data/gold/run_catalog.parquet'
    ORDER BY created_at
""").show()
```

Find runs by their parameters, then filter every other Gold table on the
resulting `run_id`s. Typical comparative one-liner (`cross_run_summary`
pre-joins leaderboards with run parameters):

```sql
-- does the temptation level reorder the podium?
SELECT payoff_temptation, payoff_reward, player, rank, avg_score_per_round
FROM 'data/gold/cross_run_summary.parquet'
WHERE n_rounds = 2000 AND seed = 42 AND rank <= 3
ORDER BY payoff_temptation, rank;
```

> When labelling runs in a chart, always include the `run_id` — several
> runs can share an identical configuration.

### The three consumption surfaces

| Surface | Use it for |
|---|---|
| **Streamlit dashboard** (`uv run streamlit run app/dashboard.py`) | Interactive: per-run leaderboard/heatmap/drift/forgiveness + the cross-run comparison tab |
| **EDA notebook** (`notebooks/analysis.ipynb`) | Narrated analysis, publication-style figures, the Nash-vs-cooperation discussion |
| **Raw DuckDB SQL** | Ad-hoc questions; all Gold tables are plain Parquet files under `data/gold/` |

All three consume **Gold only** — if a question needs round-level data, the
right move is a new Gold model in `dbt/models/gold/`, not a raw scan in the
analysis layer.

### Methodological checklists

Designing a comparison:

- **One factor at a time.** Vary the payoff matrix *or* the roster *or* the
  horizon between two runs, never several at once.
- **Replicate anything involving randomness** (3 seeds minimum) before
  claiming an effect — recipe 1.
- **Per-round metrics across horizons.** `total_score` is only comparable
  between runs with the same `n_rounds` and roster size; prefer
  `avg_score_per_round` and `coop_rate`.
- **Roster changes change the denominator.** Ranks from a 6-player pool and
  a 10-player pool are not comparable — compare *behaviour* (cooperation,
  forgiveness rates), not ranks, across different rosters.

Housekeeping:

- The lake is **append-only**: re-running an experiment adds a new copy.
  Identical-config duplicates are harmless but clutter cross-run charts.
- `data/` is **disposable**: `rm -rf data` + `populate_lake.py` gives you a
  clean curated lake in ~1 minute. Never commit it.
- Want a scratch space? Set `CONVICTS_DATA_DIR` to an **absolute** path
  before any command — it redirects everything (Dagster assets, dbt, the
  dashboard): `$env:CONVICTS_DATA_DIR = "C:\tmp\scratch_lake"` (PowerShell).
