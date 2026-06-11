-- One row per (run, match, player perspective): final score, outcome and
-- in-match cooperation rate. Shared per-match grain for tournament_summary
-- and matchup_matrix. Self-play matches yield two rows of the same player
-- and always count as draws.

with finals as (

    select
        run_id,
        match_id,
        player_slot,
        player,
        opponent,
        max(cumulative_score) as final_score,
        avg(cooperated::int)  as coop_rate,
        count(*)              as n_rounds

    from {{ source('lake', 'silver_rounds') }}
    group by all

)

select
    mine.run_id,
    mine.match_id,
    mine.player_slot,
    mine.player,
    mine.opponent,
    mine.n_rounds,
    mine.final_score,
    theirs.final_score as opponent_final_score,
    mine.coop_rate,
    case
        when mine.final_score > theirs.final_score then 'win'
        when mine.final_score < theirs.final_score then 'loss'
        else 'draw'
    end as outcome

from finals as mine
inner join finals as theirs
    on mine.run_id = theirs.run_id
    and mine.match_id = theirs.match_id
    and mine.player_slot <> theirs.player_slot
