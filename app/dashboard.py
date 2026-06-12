"""Streamlit dashboard over the Gold layer — the interactive analyst endpoint.

Like the EDA notebook, it consumes **Gold aggregates only** (never Bronze or
Silver rows), so it respects the spec constraint and stays fast whatever the
lake size. It adapts to whatever runs exist: materialise more tournaments
(see ``docs/data_scientist_guide.md``) and hit "Refresh data".

Run with::

    uv sync --group dashboard
    uv run streamlit run app/dashboard.py
"""

from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

from convicts_dilemma.config import data_root

GOLD_DIR = data_root() / "gold"

GOLD_TABLES = (
    "run_catalog",
    "tournament_summary",
    "matchup_matrix",
    "behavioral_drift",
    "forgiveness_index",
    "cross_run_summary",
)

st.set_page_config(
    page_title="Convicts' Dilemma — Gold dashboard",
    page_icon="🔁",
    layout="wide",
)


def _gold_path(table: str) -> Path:
    return GOLD_DIR / f"{table}.parquet"


@st.cache_data(show_spinner=False)
def load_gold(table: str, mtime_ns: int) -> pd.DataFrame:
    """Read one Gold parquet into pandas.

    ``mtime_ns`` only serves as cache key: a dbt rebuild rewrites the file,
    bumps the mtime and thus invalidates the cached frame.
    """
    return duckdb.sql(
        f"select * from '{_gold_path(table).as_posix()}'"
    ).df()


def gold(table: str) -> pd.DataFrame:
    return load_gold(table, _gold_path(table).stat().st_mtime_ns)


def run_label(row: pd.Series) -> str:
    """Human-readable but unambiguous run label.

    Several runs can share an identical configuration, so the label always
    starts with the run_id (see CLAUDE.md).
    """
    payoffs = (
        f"T{row.payoff_temptation}/R{row.payoff_reward}"
        f"/P{row.payoff_punishment}/S{row.payoff_sucker}"
    )
    return (
        f"{row.run_id} · seed {row.seed} · {row.n_rounds} rounds"
        f" · {payoffs} · {len(row.strategies)} players"
    )


missing = [t for t in GOLD_TABLES if not _gold_path(t).exists()]
if missing:
    st.title("Convicts' Dilemma")
    st.warning(
        f"Gold layer incomplete under `{GOLD_DIR.as_posix()}` "
        f"(missing: {', '.join(missing)}). Build it first:"
    )
    st.code(
        "uv run dagster asset materialize -m convicts_dilemma.defs "
        '--select "bronze_tournament,silver_rounds,int_match_results,'
        "tournament_summary,matchup_matrix,behavioral_drift,"
        'forgiveness_index,run_catalog,cross_run_summary"',
        language="bash",
    )
    st.stop()

catalog = gold("run_catalog").sort_values("created_at", ascending=False)
catalog["label"] = catalog.apply(run_label, axis=1)

# ---------------------------------------------------------------- sidebar
with st.sidebar:
    st.title("🔁 Convicts' Dilemma")
    st.caption(
        f"{len(catalog)} tournament run(s) in the lake — Gold aggregates only."
    )
    focus_label = st.selectbox(
        "Focus run", catalog["label"], index=0,
        help="Most tabs show this single run; newest first.",
    )
    focus = catalog.loc[catalog["label"] == focus_label].iloc[0]
    run_id = focus.run_id

    st.divider()
    if st.button("🔄 Refresh data", width="stretch"):
        st.cache_data.clear()
        st.rerun()
    st.caption(
        "Generate more runs to compare "
        "(see `docs/data_scientist_guide.md` for the recipe book):"
    )
    st.code("uv run python scripts/populate_lake.py", language="bash")

# ---------------------------------------------------------------- header
st.title("Iterated prisoner's dilemma — tournament analytics")
cols = st.columns(7)
cols[0].metric("Seed", int(focus.seed))
cols[1].metric("Rounds/match", int(focus.n_rounds))
cols[2].metric("Matches", int(focus.n_matches))
cols[3].metric("Temptation (T)", int(focus.payoff_temptation))
cols[4].metric("Reward (R)", int(focus.payoff_reward))
cols[5].metric("Punishment (P)", int(focus.payoff_punishment))
cols[6].metric("Sucker (S)", int(focus.payoff_sucker))
with st.expander(f"Roster of `{run_id}` ({len(focus.strategies)} strategies)"):
    st.write(" · ".join(f"`{s}`" for s in focus.strategies))
    if not focus.include_self_play:
        st.caption("Self-play disabled for this run.")

tab_board, tab_matrix, tab_drift, tab_forgive, tab_runs = st.tabs(
    [
        "🏆 Leaderboard",
        "⚔️ Matchup matrix",
        "📉 Behavioural drift",
        "🕊️ Forgiveness",
        "🧪 Cross-run comparison",
    ]
)

# ------------------------------------------------------------ leaderboard
with tab_board:
    summary = (
        gold("tournament_summary")
        .query("run_id == @run_id")
        .sort_values("rank")
    )
    left, right = st.columns([3, 2], gap="large")
    with left:
        fig = px.bar(
            summary,
            x="total_score",
            y="player",
            orientation="h",
            color="coop_rate",
            color_continuous_scale="RdYlGn",
            range_color=(0, 1),
            labels={"total_score": "Total score", "player": "",
                    "coop_rate": "Cooperation rate"},
            title="Final scores (colour = cooperation rate)",
        )
        fig.update_layout(yaxis={"categoryorder": "total ascending"}, height=480)
        st.plotly_chart(fig, width="stretch")
    with right:
        st.dataframe(
            summary.drop(columns=["run_id"]).set_index("rank"),
            width="stretch",
            height=480,
            column_config={
                "coop_rate": st.column_config.ProgressColumn(
                    "coop_rate", min_value=0.0, max_value=1.0, format="%.2f"
                ),
            },
        )
    st.caption(
        "Axelrod's classic result: *nice* but *retaliatory* strategies "
        "(tit-for-tat family) dominate, even though always_defect is the "
        "single-round Nash equilibrium."
    )

# ---------------------------------------------------------- matchup matrix
with tab_matrix:
    matrix = gold("matchup_matrix").query("run_id == @run_id")
    metric = st.radio(
        "Cell value",
        ["player_score", "player_coop_rate"],
        format_func=lambda m: {
            "player_score": "Average score of the row player",
            "player_coop_rate": "Cooperation rate of the row player",
        }[m],
        horizontal=True,
    )
    pivot = matrix.pivot(index="player", columns="opponent", values=metric)
    fig = px.imshow(
        pivot,
        text_auto=".2f" if metric == "player_coop_rate" else ".0f",
        color_continuous_scale="RdYlGn",
        aspect="auto",
        labels={"x": "Opponent", "y": "Player", "color": metric},
        title="Row strategy vs column strategy",
    )
    fig.update_layout(height=620)
    st.plotly_chart(fig, width="stretch")

# -------------------------------------------------------- behavioural drift
with tab_drift:
    drift = gold("behavioral_drift").query("run_id == @run_id")
    metric = st.radio(
        "Metric",
        ["coop_rate", "mutual_coop_rate", "avg_payoff"],
        format_func=lambda m: {
            "coop_rate": "Cooperation rate",
            "mutual_coop_rate": "Mutual cooperation rate",
            "avg_payoff": "Average payoff",
        }[m],
        horizontal=True,
        key="drift_metric",
    )
    players = st.multiselect(
        "Strategies",
        sorted(drift["player"].unique()),
        default=sorted(drift["player"].unique()),
    )
    fig = px.line(
        drift[drift["player"].isin(players)].sort_values("round_bucket"),
        x="round_bucket",
        y=metric,
        color="player",
        markers=True,
        labels={"round_bucket": "Round bucket (start of bucket)"},
        title="Behaviour over the course of a match (bucketed)",
    )
    fig.update_layout(height=520)
    st.plotly_chart(fig, width="stretch")
    st.caption(
        "Drops reveal grim-trigger lock-ins and echo feuds; the bucket size "
        "is a Silver run-config parameter (default 100 rounds)."
    )

# -------------------------------------------------------------- forgiveness
with tab_forgive:
    forgiveness = gold("forgiveness_index").query("run_id == @run_id")
    summary = gold("tournament_summary").query("run_id == @run_id")
    merged = forgiveness.merge(
        summary[["player", "avg_score_per_round", "rank"]], on="player"
    )
    fig = px.scatter(
        merged,
        x="forgiveness_rate",
        y="avg_score_per_round",
        size="betrayal_responses",
        color="rank",
        color_continuous_scale="Viridis_r",
        text="player",
        labels={
            "forgiveness_rate": "Forgiveness rate (cooperate right after being betrayed)",
            "avg_score_per_round": "Average score per round",
        },
        title="Does forgiveness pay? (bubble size = betrayals faced)",
    )
    fig.update_traces(textposition="top center")
    fig.update_layout(height=560)
    st.plotly_chart(fig, width="stretch")
    st.dataframe(
        forgiveness.drop(columns=["run_id"]).sort_values(
            "forgiveness_rate", ascending=False
        ),
        width="stretch", hide_index=True,
    )

# --------------------------------------------------------------- cross-run
with tab_runs:
    if len(catalog) < 2:
        st.info(
            "Only one run in the lake — generate heterogeneous runs to "
            "compare (different seeds, payoff matrices, rosters, horizons):"
        )
        st.code("uv run python scripts/populate_lake.py", language="bash")
    else:
        chrono = catalog.sort_values("created_at")
        selected = st.multiselect(
            "Runs to compare (chronological)",
            chrono["label"].tolist(),
            default=chrono["label"].tolist(),
        )
        cross = gold("cross_run_summary")
        cross = cross.merge(catalog[["run_id", "label"]], on="run_id")
        cross = cross[cross["label"].isin(selected)].sort_values("created_at")

        metric = st.radio(
            "Metric",
            ["rank", "avg_score_per_round", "coop_rate", "total_score"],
            horizontal=True,
            key="cross_metric",
        )
        fig = px.line(
            cross,
            x="label",
            y=metric,
            color="player",
            markers=True,
            labels={"label": "Run", "rank": "Rank (1 = winner)"},
            title=f"{metric} across runs — does the winner survive a rule change?",
        )
        if metric == "rank":
            fig.update_yaxes(autorange="reversed", dtick=1)
        fig.update_layout(height=560, xaxis={"tickangle": -30})
        st.plotly_chart(fig, width="stretch")

        st.subheader("Run parameters (`run_catalog`)")
        params = catalog[catalog["label"].isin(selected)].sort_values("created_at")
        st.dataframe(
            params[
                [
                    "run_id", "created_at", "seed", "n_rounds",
                    "payoff_temptation", "payoff_reward",
                    "payoff_punishment", "payoff_sucker",
                    "include_self_play", "n_matches", "strategies",
                ]
            ],
            width="stretch", hide_index=True,
        )
