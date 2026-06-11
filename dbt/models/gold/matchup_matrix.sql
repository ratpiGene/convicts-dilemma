-- Cross-confrontation table: strategy A vs strategy B -> average score on
-- both sides plus A's cooperation rate in those matches. One row per
-- (run, player, opponent) — both orientations are present, so the matrix
-- is directly pivotable into a heatmap.

select
    run_id,
    player,
    opponent,
    count(*)                  as n_matches,
    avg(final_score)          as player_score,
    avg(opponent_final_score) as opponent_score,
    avg(coop_rate)            as player_coop_rate

from {{ ref('int_match_results') }}
group by all
order by run_id, player, opponent
