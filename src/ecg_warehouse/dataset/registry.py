import datetime
import json
import uuid

import pyarrow as pa
from pyiceberg.expressions import And, EqualTo, Reference, literal
from pyiceberg.table import Table, TableProperties

from ecg_warehouse.service import Warehouse

from .spec import SEMVER_CORE
from .store import read_manifest

DATASET_ID_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "ecg-warehouse:datasets:v1")


def dataset_id(name: str, version: str) -> str:
    """Stable id derived from (name, version) — same identity across runs."""
    return str(uuid.uuid5(DATASET_ID_NAMESPACE, f"{name}:{version}"))


def load_registry(warehouse: Warehouse) -> Table:
    """Load the `datasets` table so that a lookup on it and a `register_dataset(..., registry=...)` form one CAS.

    An append commit asserts that `main` still points at the snapshot this Table was loaded at, so a registration
    by anyone else in between makes it fail with `CommitFailedException` instead of adding a second row. PyIceberg's
    own commit retry would undo that by rebasing the append onto the new head without validation, so it is turned
    off on this in-memory copy (commits send only the staged updates, so the property never reaches the catalog);
    callers reload and re-check instead.
    """
    table = warehouse.table("datasets")
    properties = {**table.metadata.properties, TableProperties.COMMIT_NUM_RETRIES: "0"}
    table.metadata = table.metadata.model_copy(update={"properties": properties})
    return table


def _registry_table(registry: Warehouse | Table) -> Table:
    return registry.table("datasets") if isinstance(registry, Warehouse) else registry


def existing_dataset(registry: Warehouse | Table, name: str, version: str) -> dict | None:
    """Return the most recent registry row for (name, version), or None.

    `registry` is a warehouse or a loaded `datasets` table, whose snapshot is then the one read.
    """
    rows = (
        _registry_table(registry)
        .scan(
            row_filter=And(
                EqualTo(term=Reference("name"), value=literal(name)),
                EqualTo(term=Reference("version"), value=literal(version)),
            )
        )
        .to_arrow()
        .to_pylist()
    )
    if not rows:
        return None
    rows.sort(key=lambda r: r.get("created_at", ""), reverse=True)
    return rows[0]


def latest_semantic_release(registry: Warehouse | Table, name: str) -> dict | None:
    """Return the highest SemVer release for a dataset, ignoring legacy hash versions."""
    rows = (
        _registry_table(registry)
        .scan(
            row_filter=And(
                EqualTo(term=Reference("name"), value=literal(name)),
                EqualTo(term=Reference("mode"), value=literal("release")),
            )
        )
        .to_arrow()
        .to_pylist()
    )
    semantic_rows = [row for row in rows if SEMVER_CORE.fullmatch(row["version"])]
    if not semantic_rows:
        return None
    return max(semantic_rows, key=lambda row: (tuple(map(int, row["version"].split("."))), str(row["created_at"])))


def register_dataset(warehouse: Warehouse, manifest: dict, registry: Table | None = None) -> str:
    """Append a row describing a built dataset to the datasets registry.

    The append is unconditional. Pass the `load_registry` table the caller checked the registry on to make it
    conditional: it then raises `CommitFailedException` if anything was registered since that table was loaded.
    """
    name = manifest["name"]
    version = manifest["version"]
    ds_id = dataset_id(name, version)

    now = datetime.datetime.now(datetime.UTC)
    arrow_table = pa.table(
        {
            "dataset_id": pa.array([ds_id], type=pa.string()),
            "name": pa.array([name], type=pa.string()),
            "version": pa.array([version], type=pa.string()),
            "mode": pa.array([manifest["mode"]], type=pa.string()),
            "content_hash": pa.array([manifest["content_hash"]], type=pa.string()),
            "spec_json": pa.array(
                [json.dumps(manifest["spec"], sort_keys=True, separators=(",", ":"))],
                type=pa.string(),
            ),
            "recordings_snapshot_id": pa.array([manifest["snapshots"]["recordings"]], type=pa.int64()),
            "diagnoses_snapshot_id": pa.array([manifest["snapshots"]["diagnoses"]], type=pa.int64()),
            "manifest_uri": pa.array([manifest["manifest_uri"]], type=pa.string()),
            "recordings_uri": pa.array([manifest["artifacts"]["recordings"]["uri"]], type=pa.string()),
            "diagnoses_uri": pa.array([manifest["artifacts"]["diagnoses"]["uri"]], type=pa.string()),
            "recording_count": pa.array([manifest["artifacts"]["recordings"]["rows"]], type=pa.int64()),
            "diagnosis_count": pa.array([manifest["artifacts"]["diagnoses"]["rows"]], type=pa.int64()),
            "created_at": pa.array([now], type=pa.timestamp("us", tz="UTC")),
            "release_type": pa.array([manifest.get("release_type")], type=pa.string()),
            "signals_uri": pa.array([manifest["artifacts"].get("signals", {}).get("uri")], type=pa.string()),
        }
    )
    arrow_table = arrow_table.cast(warehouse.arrow_schema("datasets"))
    (warehouse.table("datasets") if registry is None else registry).append(arrow_table)
    return ds_id


def registered_manifest(row: dict) -> dict:
    """Return the stored manifest.json of a registry row, equal to the manifest its build returned.

    The row alone can't stand in for it: it lacks e.g. the signals storage config and the split counts.
    """
    label = f"{row['name']}@{row['version']}"
    try:
        manifest = read_manifest(row["manifest_uri"])
    except (OSError, ValueError) as exc:
        raise ValueError(
            f"Dataset {label} is registered, but its manifest {row['manifest_uri']} cannot be read: {exc}"
        ) from exc
    if manifest.get("content_hash") != row["content_hash"]:
        # Something else was written at the same location (e.g. another warehouse sharing the store).
        raise ValueError(
            f"Dataset {label} manifest {row['manifest_uri']} has content_hash {manifest.get('content_hash')}, "
            f"but the registry has {row['content_hash']}"
        )
    return manifest
