-- The discovery index of the versioned lake: one row per tournament run
-- with every parameter that defines it. This is how an analyst finds runs
-- ("all runs with temptation = 10", "the latest 2000-round run") before
-- filtering the other Gold tables on run_id.

select
    run_id,
    created_at,
    engine_version,
    seed,
    n_rounds,
    llm_n_rounds,
    ollama_model,
    n_matches,
    strategies,
    include_self_play,
    payoff_reward,
    payoff_temptation,
    payoff_sucker,
    payoff_punishment

from {{ source('lake', 'bronze_manifests') }}
order by created_at
