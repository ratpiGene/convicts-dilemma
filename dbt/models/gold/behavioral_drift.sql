-- Cooperation rate per strategy per round bucket (1-100, 101-200, ...),
-- to detect behavioural shifts over the course of a match (end-game
-- defection, grim-trigger lock-ins, echo feuds...).

select
    run_id,
    player,
    round_bucket,
    avg(cooperated::int)        as coop_rate,
    avg(mutual_cooperation::int) as mutual_coop_rate,
    avg(payoff)                 as avg_payoff,
    count(*)                    as n_observations

from {{ source('lake', 'silver_rounds') }}
group by all
order by run_id, player, round_bucket
