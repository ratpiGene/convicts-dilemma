"""dagster-dbt integration: expose the Gold dbt models as Dagster assets.

The dbt sources are mapped back onto the upstream Dagster assets
(``silver_rounds``, ``bronze_tournament``) so the whole lineage —
simulation → Bronze → Silver → Gold models → dbt tests — appears as one
graph in the Dagster UI.

Note: no ``from __future__ import annotations`` in Dagster definition
modules (see assets.py).
"""

from pathlib import Path

import dagster as dg
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

from convicts_dilemma.config import data_root

#: src/convicts_dilemma/defs/dbt.py -> repo root (works because uv installs
#: the project in editable mode).
REPO_ROOT = Path(__file__).resolve().parents[3]

dbt_project = DbtProject(project_dir=REPO_ROOT / "dbt")
dbt_project.prepare_if_dev()

# `dagster dev` prepares the manifest automatically; for one-shot CLI
# materialisations from a cold clone, generate it on demand. (Not via
# DbtCliResource.cli(["parse"]): that writes to a unique per-invocation
# target path, not the manifest_path read by @dbt_assets.)
if not dbt_project.manifest_path.exists() and dbt_project.preparer:
    dbt_project.preparer.prepare(dbt_project)


class LakeTranslator(DagsterDbtTranslator):
    """Map dbt sources onto the Dagster assets that produce them."""

    _SOURCE_TO_ASSET = {
        "silver_rounds": "silver_rounds",
        "bronze_manifests": "bronze_tournament",
    }

    def get_asset_key(self, dbt_resource_props) -> dg.AssetKey:
        if dbt_resource_props["resource_type"] == "source":
            return dg.AssetKey(self._SOURCE_TO_ASSET[dbt_resource_props["name"]])
        return super().get_asset_key(dbt_resource_props)


@dbt_assets(manifest=dbt_project.manifest_path, dagster_dbt_translator=LakeTranslator())
def gold_dbt_assets(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    """Run ``dbt build`` (models + data tests) for the Gold layer."""
    # DuckDB creates the database file but not its parent directory.
    (data_root() / "gold").mkdir(parents=True, exist_ok=True)
    yield from dbt.cli(["build"], context=context).stream()
