"""Dagster code location for the project (referenced by [tool.dagster])."""

import dagster as dg
from dagster_dbt import DbtCliResource

from convicts_dilemma.defs.assets import bronze_tournament, silver_rounds
from convicts_dilemma.defs.dbt import dbt_project, gold_dbt_assets

defs = dg.Definitions(
    assets=[bronze_tournament, silver_rounds, gold_dbt_assets],
    resources={"dbt": DbtCliResource(project_dir=dbt_project)},
)
