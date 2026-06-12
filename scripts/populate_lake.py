"""Populate the lake with a heterogeneous set of tournament runs.

The default lake quickly fills with runs that all share the same
configuration, which makes the cross-run Gold models (``run_catalog``,
``cross_run_summary``) and the dashboard's comparison tab pointless. This
script materialises a curated experiment plan that varies **one factor at a
time** — seed, payoff matrix, match horizon, roster composition, self-play —
so comparative analysis has real contrasts to work with. The rationale for
each experiment lives in ``docs/data_scientist_guide.md``.

Coded strategies only by default (no Ollama needed); the whole plan runs in
~1 minute. ``--llm`` appends an LLM persona experiment — it needs a running
Ollama server with the configured model pulled and takes ~10-25 minutes
(hundreds of LLM decisions at ~1-4 s each).

Usage::

    uv run python scripts/populate_lake.py             # Bronze + Silver + Gold
    uv run python scripts/populate_lake.py --dry-run   # print the plan only
    uv run python scripts/populate_lake.py --skip-gold # no dbt build at the end
    uv run python scripts/populate_lake.py --llm       # coded plan + LLM face-off
    uv run python scripts/populate_lake.py --llm-only  # LLM face-off alone

Re-running appends a fresh copy of every run (the lake is append-only by
design); wipe ``data/`` first if you want a clean comparative baseline.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from dataclasses import replace
from pathlib import Path

from convicts_dilemma.config import PayoffMatrix, data_root
from convicts_dilemma.pipeline.bronze import write_bronze
from convicts_dilemma.pipeline.silver import transform_pending
from convicts_dilemma.simulation.tournament import TournamentConfig, run_tournament
from convicts_dilemma.strategies import DEFAULT_ROSTER

REPO_ROOT = Path(__file__).resolve().parents[1]

NICE_ROSTER = (
    "always_cooperate",
    "tit_for_tat",
    "generous_tit_for_tat",
    "tit_for_two_tats",
    "grim_trigger",
    "pavlov",
)
HOSTILE_ROSTER = (
    "always_defect",
    "suspicious_tit_for_tat",
    "grim_trigger",
    "joss",
    "random",
    "tit_for_tat",
)

#: (label, config) — the labels are printed only; the lake identifies runs
#: by run_id + manifest parameters, never by a name.
EXPERIMENTS: tuple[tuple[str, TournamentConfig], ...] = (
    # -- replicates: same rules, three seeds (is the ranking seed-stable?)
    ("baseline seed 41", TournamentConfig(seed=41)),
    ("baseline seed 42", TournamentConfig(seed=42)),
    ("baseline seed 43", TournamentConfig(seed=43)),
    # -- payoff sweep (same seed/horizon/roster, only the matrix moves)
    (
        "gentle world T=4",
        TournamentConfig(payoff=PayoffMatrix(temptation=4)),
    ),
    (
        "high temptation T=10 R=6 (still a valid iterated dilemma)",
        TournamentConfig(payoff=PayoffMatrix(temptation=10, reward=6)),
    ),
    (
        "greed trap T=10 R=3 (violates 2R > T+S on purpose)",
        TournamentConfig(payoff=PayoffMatrix(temptation=10)),
    ),
    (
        "costly betrayal S=-2",
        TournamentConfig(payoff=PayoffMatrix(sucker=-2)),
    ),
    # -- horizon sweep: the shadow of the future
    ("short horizon 100 rounds", TournamentConfig(n_rounds=100)),
    ("medium horizon 500 rounds", TournamentConfig(n_rounds=500)),
    # -- population composition
    ("nice-only roster", TournamentConfig(strategies=NICE_ROSTER)),
    ("hostile roster", TournamentConfig(strategies=HOSTILE_ROSTER)),
    # -- scheduling variant
    ("no self-play", TournamentConfig(include_self_play=False)),
)

#: Opt-in (--llm): the four Ollama personas against a nice and a nasty
#: anchor. Self-play off and no coded filler keeps the LLM-decision budget
#: focused on persona-vs-persona and persona-vs-archetype contrasts.
LLM_EXPERIMENTS: tuple[tuple[str, TournamentConfig], ...] = (
    (
        "LLM persona face-off (Ollama)",
        TournamentConfig(
            strategies=(
                "tit_for_tat",
                "always_defect",
                "llm_empathetic",
                "llm_calculating",
                "llm_vengeful",
                "llm_opportunist",
            ),
            include_self_play=False,
            llm_n_rounds=25,
        ),
    ),
)


def check_ollama(model: str) -> None:
    """Fail fast (with a readable message) if the LLM runs cannot work.

    Without this check an unreachable server would not crash the run — the
    agents would silently fall back to tit-for-tat and poison the lake with
    100% fallback data.
    """
    import ollama

    try:
        client = ollama.Client(host=os.environ.get("OLLAMA_HOST"))
        client.show(model)
    except Exception as exc:  # noqa: BLE001 — single readable abort point
        raise SystemExit(
            f"Ollama not ready for model {model!r} ({type(exc).__name__}: {exc}).\n"
            "Start it with `ollama serve` and pull the model: `ollama pull "
            f"{model}` — see README §5."
        ) from exc


def llm_fallback_stats(result) -> tuple[int, int]:
    """(total LLM decisions, fallback decisions) of one tournament result."""
    records = [
        record
        for match in result.matches
        if match.llm_raw
        for slot_records in match.llm_raw.values()
        for record in slot_records
    ]
    return len(records), sum(bool(r["fallback"]) for r in records)


def build_gold() -> None:
    """Build + test the Gold dbt models over whatever Silver now contains."""
    (data_root() / "gold").mkdir(parents=True, exist_ok=True)  # DuckDB won't
    subprocess.run(
        ["uv", "run", "dbt", "build"],
        cwd=REPO_ROOT / "dbt",
        check=True,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--dry-run", action="store_true", help="print the plan, run nothing")
    parser.add_argument("--skip-gold", action="store_true", help="stop after Silver")
    parser.add_argument(
        "--llm", action="store_true",
        help="append the LLM persona experiment (needs Ollama; ~10-25 min)",
    )
    parser.add_argument(
        "--llm-only", action="store_true",
        help="run only the LLM persona experiment (implies --llm)",
    )
    parser.add_argument(
        "--model", default=None, metavar="TAG",
        help="Ollama model tag for the LLM experiments (recorded in the "
        "manifest; default: the engine default)",
    )
    args = parser.parse_args()

    llm_experiments = LLM_EXPERIMENTS
    if args.model:
        llm_experiments = tuple(
            (label, replace(config, ollama_model=args.model))
            for label, config in llm_experiments
        )
    plan = llm_experiments if args.llm_only else (
        EXPERIMENTS + llm_experiments if args.llm else EXPERIMENTS
    )
    if (args.llm or args.llm_only) and not args.dry_run:
        check_ollama(llm_experiments[-1][1].ollama_model)

    width = max(len(label) for label, _ in plan)
    print(f"Experiment plan ({len(plan)} runs):")
    for label, config in plan:
        p = config.payoff
        has_llm = any(s.startswith("llm_") for s in config.strategies)
        print(
            f"  {label:<{width}}  seed={config.seed} rounds={config.n_rounds} "
            f"T{p.temptation}/R{p.reward}/P{p.punishment}/S{p.sucker} "
            f"roster={len(config.strategies)} self_play={config.include_self_play}"
            + (f" model={config.ollama_model}" if has_llm else "")
        )
    if args.dry_run:
        return 0

    print("\nBronze:")
    t0 = time.perf_counter()
    for label, config in plan:
        result = run_tournament(config)
        summary = write_bronze(result)
        print(f"  {label:<{width}}  -> run_id={summary['run_id']} ({summary['n_rows']} rows)")
        n_llm, n_fallback = llm_fallback_stats(result)
        if n_llm:
            print(
                f"  {'':<{width}}     {n_llm} LLM decisions, "
                f"{n_fallback} fallback(s) ({n_fallback / n_llm:.1%})"
            )

    print("\nSilver:")
    summaries = transform_pending()
    print(f"  {len(summaries)} run(s) enriched.")

    if args.skip_gold:
        print("\nGold skipped (--skip-gold). Build later with: cd dbt && uv run dbt build")
    else:
        print("\nGold (dbt build):")
        build_gold()

    print(f"\nDone in {time.perf_counter() - t0:.1f}s.")
    print("Explore: uv run streamlit run app/dashboard.py")
    return 0


if __name__ == "__main__":
    sys.exit(main())
