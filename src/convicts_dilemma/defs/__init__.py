"""Dagster code location for the project (referenced by [tool.dagster])."""

import dagster as dg

from convicts_dilemma.defs.assets import bronze_tournament

defs = dg.Definitions(assets=[bronze_tournament])
