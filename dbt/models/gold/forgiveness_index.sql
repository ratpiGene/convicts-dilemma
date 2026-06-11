-- How does each strategy respond right after being betrayed?
-- Every round following an opponent defection is either forgiven (return
-- to cooperation) or retaliated. forgiveness_rate = forgiven / responses.

select
    run_id,
    player,
    sum(forgave::int)                          as forgiveness_events,
    sum(retaliated::int)                       as retaliation_events,
    sum(forgave::int) + sum(retaliated::int)   as betrayal_responses,
    sum(forgave::int)::double
        / nullif(sum(forgave::int) + sum(retaliated::int), 0) as forgiveness_rate

from {{ source('lake', 'silver_rounds') }}
group by all
order by run_id, forgiveness_rate desc
