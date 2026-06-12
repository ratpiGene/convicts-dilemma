# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Ynov M2 "Outils ETL" course project (spec: `Expérimentation prisonnier - ETL-ELT.pdf` at repo root). An iterated prisoner's dilemma tournament (Axelrod) feeding a medallion pipeline: simulation → Bronze (raw Parquet) → Silver (Polars enrichment) → Gold (dbt-duckdb aggregates), orchestrated by Dagster.

Hard deliverable constraints from the spec: the repo must be **replicable from README.md alone**, `data/` must **never be committed**, and Gold must contain **aggregates only** (no raw rows).

## Commands

```bash
uv sync                          # install (uv manages Python 3.13 + venv)
uv run pytest                    # full suite
uv run pytest tests/test_silver.py -k forgiveness   # single test
uv run dagster dev               # UI at http://127.0.0.1:3000

# Full pipeline (new tournament snapshot -> Silver -> Gold + dbt tests):
uv run dagster asset materialize -m convicts_dilemma.defs \
  --select "bronze_tournament,silver_rounds,int_match_results,tournament_summary,matchup_matrix,behavioral_drift,forgiveness_index,run_catalog,cross_run_summary"

# dbt directly (cwd must be dbt/ — profiles.yml lives there):
cd dbt && uv run dbt build

# Populate the lake with 12 heterogeneous runs (Bronze+Silver+Gold, ~1 min):
uv run python scripts/populate_lake.py        # --dry-run to preview the plan
uv run python scripts/populate_lake.py --llm-only   # LLM persona face-off (needs Ollama, ~10-25 min)

# Streamlit dashboard (Gold-only consumer, like the notebook):
uv sync --group dashboard
uv run streamlit run app/dashboard.py

# EDA notebook:
uv sync --group analysis
uv run jupyter lab notebooks/analysis.ipynb
```

Asset configs (seed, rounds, roster, payoff matrix, LLM settings) are passed via `--config-json '{"ops": {"bronze_tournament": {"config": {...}}}}'` or the Dagster launchpad.

## Architecture

**Data flow**: `simulation/` plays a seeded round-robin and returns a `TournamentResult`; `pipeline/bronze.py` writes it as Hive-partitioned Parquet (`data/bronze/rounds/run_id=…/match_id=…/`); `pipeline/silver.py` (Polars) unpivots match-centric rows into **two player-centric rows per round** and adds windowed features (`shift`, `cum_sum`, `rolling_mean` `.over(match_id, player_slot)`); the dbt project in `dbt/` reads Silver as **zero-copy external sources** (`external_location` over Parquet globs) and materialises Gold models both as Parquet under `data/gold/` and as views in `data/gold/gold.duckdb`.

**Versioned lake**: every Bronze materialisation creates a new immutable `run_id` partition plus a one-row manifest (all run parameters). Partition columns (`run_id`, `match_id`) live **only in the directory paths**, never inside the Parquet files — readers re-materialise them via `hive_partitioning=true`. Silver is incremental/idempotent: it processes exactly the Bronze runs missing a Silver partition (`pending_run_ids`).

**Strategy abstraction**: `strategies/base.py` defines the one interface (`decide(my_history, their_history) -> "C"|"D"`, plus `last_reasoning` and `is_llm`). Coded strategies (`strategies/classic.py`) and Ollama LLM personas (`agents/ollama_agent.py`) are interchangeable players; the engine and pipeline never distinguish them. The tournament shortens LLM-involved matches to `llm_n_rounds` and collects `raw_records` from agents into Bronze JSONL (`bronze/llm_raw/`).

**Determinism contract**: all randomness goes through per-player `random.Random` instances seeded with the string `"{seed}:{match_id}:{slot}"` (string seeding is stable across processes/versions; tuples are not accepted and `hash()` is not stable). Same config ⇒ byte-identical dataset. LLM runs are best-effort only (a fixed Ollama seed is sent).

**Gold consumers**: the Streamlit dashboard (`app/dashboard.py`), the EDA notebook and ad-hoc DuckDB SQL all read **Gold aggregates only** — never Bronze/Silver rows. Round-level questions get a new dbt model, not a raw scan in a consumer. `scripts/populate_lake.py` holds the curated heterogeneous experiment plan (documented in `docs/data_scientist_guide.md`); it writes Bronze/Silver via direct function calls and shells out to `dbt build` for Gold.

**Dagster wiring**: `defs/assets.py` holds `bronze_tournament` and `silver_rounds`; `defs/dbt.py` exposes the dbt models via `@dbt_assets` with a custom `DagsterDbtTranslator` mapping dbt *sources* back onto the upstream asset keys (`silver_rounds`, `bronze_tournament`) so the UI shows one unbroken lineage. dbt data tests surface as Dagster asset checks. `[tool.dagster]` in pyproject points at `convicts_dilemma.defs`.

**Lazy LLM registration**: `strategies/__init__.py` must NOT import `agents` at module top (circular import — agents import `strategies.base`, which executes the package init). Personas are merged into `REGISTRY` lazily via `_register_llm_agents()` / a PEP 562 `__getattr__` for `LLM_ROSTER`.

## Gotchas (each one cost a debugging round)

- **No `from __future__ import annotations` in Dagster definition modules** (`defs/*.py`): Dagster resolves the `config:` parameter annotation at runtime and fails on stringified hints.
- **Never use `--select "*"` with the dagster CLI**: Click expands `*` against the filesystem on Windows regardless of quoting. Use explicit asset lists.
- **Stale dbt manifest**: after adding/renaming dbt models, run `cd dbt && uv run dbt parse` (or use `dagster dev`). `defs/dbt.py` only auto-generates the manifest when it is missing, via `dbt_project.preparer.prepare()` — not via `DbtCliResource.cli(["parse"])`, which writes to a per-invocation target path.
- **DuckDB does not create parent directories** for its database file; the `gold_dbt_assets` asset mkdirs `data/gold/` first, manual `dbt build` needs it pre-created once.
- **Polars three-valued logic**: boolean expressions over nullable columns propagate null (e.g. round 1 `prev_opponent_action`); wrap comparisons with `.fill_null(False)` before `&` (see `forgave`/`retaliated` in `silver.py`).
- Tests never require a live Ollama server: agents take a `client_factory` seam and tests stub it (`tests/test_agents.py`). Keep it that way so the grader's `uv run pytest` passes without Ollama.
- Manifest schema changes must bump `ENGINE_VERSION` in `simulation/tournament.py`; old runs with a different manifest schema break cross-run scans (regenerate `data/` — it is disposable by design).
- Windows console: set `PYTHONIOENCODING=utf-8` before printing DuckDB tables (cp1252 can't encode the box-drawing characters).
- **Ollama must NOT use this laptop's AMD iGPU** (hybrid Ryzen 9 6900HS + RTX 3050 4 GB). Ollama 0.30.7 ships a Vulkan backend that discovers the Radeon iGPU and the scheduler *prefers it* over CUDA because its shared memory looks bigger (≈16 GB vs 4 GB) — `ollama ps` then shows a reassuring but misleading "100% GPU". On that Vulkan/iGPU path `llama3.2:3b` generates `@@@@…` garbage (surfacing as `Unexpected empty grammar stack` errors in structured-output mode and as tit-for-tat fallbacks in tournaments) while some other models (qwen2.5:3b, llama3.2:1b) happen to work. **Fix applied 2026-06-12**: user env var `OLLAMA_VULKAN=0` (via `setx`) + Ollama app restart — CUDA/RTX 3050 is then the only GPU and llama3.2:3b runs clean at ~80% offload. To verify the device, don't trust `ollama ps` alone: check `nvidia-smi` memory or `selecting single GPU` lines in `%LOCALAPPDATA%\Ollama\server.log`. Always sanity-check one decision (fallback rate 0%) before a long LLM run; the populate script's Ollama check catches an unreachable server, but garbage output only shows up as fallbacks.
- pytest gets a **fresh per-session basetemp** via `tests/conftest.py` (`mkdtemp`). Any *shared* basetemp — the default `%TEMP%\pytest-of-<user>` or a static `--basetemp` path — causes `PermissionError` on this machine when the directory was created by a different security context (Claude's sandboxed shell vs the user's shell), because pytest scans/wipes it at session start. Never reintroduce a static basetemp.
- Anything pivoting Gold data per run must build labels that include the **run_id**, not just the config (seed/payoff): several runs can share an identical configuration (see the `run_label` cell in `notebooks/analysis.ipynb`).
