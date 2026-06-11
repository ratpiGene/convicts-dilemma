-- Strategy performance ACROSS runs: tournament_summary joined with the
-- run parameters, so one query answers "how does tit_for_tat's rank move
-- when the payoff matrix / roster / round count changes?".

select
    summary.run_id,
    catalog.created_at,
    catalog.seed,
    catalog.n_rounds,
    catalog.payoff_reward,
    catalog.payoff_temptation,
    catalog.payoff_sucker,
    catalog.payoff_punishment,
    summary.player,
    summary.rank,
    summary.total_score,
    summary.avg_score_per_round,
    summary.coop_rate,
    summary.wins,
    summary.losses,
    summary.draws

from {{ ref('tournament_summary') }} as summary
inner join {{ ref('run_catalog') }} as catalog using (run_id)
order by summary.player, catalog.created_at
