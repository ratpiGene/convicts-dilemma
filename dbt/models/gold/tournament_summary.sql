-- Final leaderboard of each run: total score, per-round average,
-- cooperation rate, win/loss record and rank. One row per (run, strategy).

select
    run_id,
    player,
    count(*)                                  as n_matches,
    sum(final_score)                          as total_score,
    avg(final_score / n_rounds)               as avg_score_per_round,
    avg(coop_rate)                            as coop_rate,
    sum((outcome = 'win')::int)               as wins,
    sum((outcome = 'loss')::int)              as losses,
    sum((outcome = 'draw')::int)              as draws,
    sum((outcome = 'win')::int)::double
        / nullif(sum((outcome = 'loss')::int), 0) as win_loss_ratio,
    rank() over (
        partition by run_id
        order by sum(final_score) desc
    ) as rank

from {{ ref('int_match_results') }}
group by run_id, player
order by run_id, rank
