import datetime
import hashlib
import json
import random
import re
import time
import uuid
from typing import Any, Literal

import pyarrow as pa
import pyarrow.compute as _pc
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.expressions import And, EqualTo, In, Reference, literal
from pyiceberg.table import Table

from ecg_warehouse.service import Warehouse

from .registry import existing_dataset, latest_semantic_release, load_registry, register_dataset, registered_manifest
from .signals import SIGNAL_COLUMNS, signals_artifact
from .spec import DatasetSpec
from .split import SPLIT_COLUMN, assign_splits
from .store import DatasetStore

# pyarrow.compute kernels are generated at runtime and not visible to static
# type checkers; alias to Any to keep call sites typed.
pc: Any = _pc

Mode = Literal["dev", "release"]
ReleaseType = Literal["major", "minor", "patch"]

REGISTER_ATTEMPTS = 5
REGISTER_RETRY_WAIT_SEC = 0.1


def _and(parts: list[Any]) -> Any | None:
    if not parts:
        return None
    expr = parts[0]
    for x in parts[1:]:
        expr = And(expr, x)
    return expr


def _recordings_filter(spec: DatasetSpec) -> Any | None:
    parts: list[Any] = []
    if spec.num_leads is not None:
        parts.append(EqualTo(term=Reference("num_leads"), value=literal(spec.num_leads)))
    if spec.sampling_rate_hz is not None:
        parts.append(EqualTo(term=Reference("sampling_rate_hz"), value=literal(spec.sampling_rate_hz)))
    if spec.sources:
        parts.append(
            EqualTo(term=Reference("source"), value=literal(spec.sources[0]))
            if len(spec.sources) == 1
            else In(term=Reference("source"), values={literal(s) for s in spec.sources})
        )
    return _and(parts)


def _diagnoses_filter(spec: DatasetSpec) -> Any | None:
    if not spec.code_systems:
        return None
    if len(spec.code_systems) == 1:
        return EqualTo(term=Reference("code_system"), value=literal(spec.code_systems[0]))
    return In(term=Reference("code_system"), values={literal(c) for c in spec.code_systems})


def _content_hash(spec: DatasetSpec, snapshots: dict[str, int | None]) -> str:
    payload = json.dumps(
        {"spec": json.loads(spec.content_payload()), "snapshots": snapshots},
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def _bump_version(previous: str | None, release_type: ReleaseType) -> str:
    if release_type not in ("major", "minor", "patch"):
        raise ValueError("release_type must be major, minor, or patch")
    major, minor, patch = map(int, (previous or "0.0.0").split("."))
    if release_type == "major":
        return f"{major + 1}.0.0"
    if release_type == "minor":
        return f"{major}.{minor + 1}.0"
    return f"{major}.{minor}.{patch + 1}"


def _resolve_version(
    spec: DatasetSpec,
    mode: Mode,
    content_hash: str,
    dev_tag: str | None,
    release_type: ReleaseType | None = None,
    previous_release: str | None = None,
) -> str:
    if mode == "release":
        if release_type is not None:
            if spec.version is not None:
                raise ValueError("remove version from the dataset spec when using release_type")
            return _bump_version(previous_release, release_type)
        if spec.version is None:
            raise ValueError("release builds require release_type or a MAJOR.MINOR.PATCH version in the spec")
        return spec.version

    base = spec.version or "0.0.0"
    if dev_tag is None:
        return f"{base}-dev.h{content_hash}"
    tag = re.sub(r"[^0-9A-Za-z-]+", "-", dev_tag).strip("-")
    if not tag:
        raise ValueError("dev_tag must contain a letter or digit")
    return f"{base}-dev.t{tag}.h{content_hash}"


def _snapshot_id(table) -> int | None:
    snap = table.current_snapshot()
    return None if snap is None else snap.snapshot_id


def _build_key(name: str, version: str) -> str:
    """Store prefix of one build's artifacts: `<name>/<version>/<build id>`.

    Builds never share a prefix, so concurrent builds of one version cannot overwrite each other's files; the registry
    row, appended only after all of them are written, decides which build `name@version` resolves to.
    """
    return f"{name}/{version}/{uuid.uuid4().hex}"


def _skip(row: dict, content_hash: str) -> dict:
    manifest = registered_manifest(row)
    print(f"Dataset {row['name']}@{row['version']} already built (content_hash={content_hash}), skipping.")
    return manifest


def _repeated_release(latest_release: dict | None, content_hash: str, skip_if_exists: bool) -> dict | None:
    """Stored manifest of the latest release when an automatic release would repeat its content."""
    if latest_release is not None and latest_release["content_hash"] == content_hash and skip_if_exists:
        return _skip(latest_release, content_hash)
    return None


def _existing_release(
    registry: Table, name: str, version: str, content_hash: str, skip_if_exists: bool
) -> dict | None:
    """Stored manifest of an identical release registered as `name@version`, or None when the version is free.

    Raises ValueError when the version is taken by other content.
    """
    prev = existing_dataset(registry, name, version)
    if prev is None:
        return None
    if prev.get("mode") != "release":
        raise ValueError(f"Dataset {name}@{version} is already registered as a development build")
    if prev.get("content_hash") != content_hash:
        raise ValueError(f"Dataset {name}@{version} already exists with different content; bump the semantic version")
    if skip_if_exists:
        return _skip(prev, content_hash)
    raise ValueError(f"Dataset {name}@{version} already exists; release versions cannot be overwritten")


def _registered_release(
    registry: Table, manifest: dict, previous_release: str | None, skip_if_exists: bool
) -> dict | None:
    """Repeat the release checks of `build_dataset` on the registry snapshot the build is registered against.

    Returns the stored manifest of an identical release that a concurrent build registered first, or None when this
    build may be registered.
    """
    name, version, content_hash = manifest["name"], manifest["version"], manifest["content_hash"]
    if manifest["release_type"] is not None:
        latest_release = latest_semantic_release(registry, name)
        repeated = _repeated_release(latest_release, content_hash, skip_if_exists)
        if repeated is not None:
            return repeated
        latest_version = None if latest_release is None else latest_release["version"]
        if latest_version != previous_release:
            # The version was bumped from a release that is no longer the latest, and the manifest already carries it.
            raise ValueError(
                f"A concurrent build released {name}@{latest_version} while this build was releasing {version}; "
                "retry the build to release after it"
            )
    return _existing_release(registry, name, version, content_hash, skip_if_exists)


def _register(warehouse: Warehouse, manifest: dict, previous_release: str | None, skip_if_exists: bool) -> dict:
    """Register a built dataset unless a concurrent build registered its release version first.

    Returns the registered manifest: this build's, or the stored one of an identical concurrent release. Checking the
    registry and appending to it form one compare-and-swap (see `load_registry`); when it fails, the registry is
    reloaded and checked again.
    """
    error: CommitFailedException | None = None
    for attempt in range(REGISTER_ATTEMPTS):
        if attempt:
            # Jittered backoff keeps the builds that lost the race from colliding again on the retry.
            time.sleep(REGISTER_RETRY_WAIT_SEC * 2 ** (attempt - 1) * random.uniform(1, 2))
        registry = load_registry(warehouse)
        if manifest["mode"] == "release":
            registered = _registered_release(registry, manifest, previous_release, skip_if_exists)
            if registered is not None:
                return registered
        try:
            register_dataset(warehouse, manifest, registry=registry)
            return manifest
        except CommitFailedException as exc:
            error = exc
    raise ValueError(
        f"Could not register {manifest['name']}@{manifest['version']}: the datasets registry kept changing; "
        "retry the build"
    ) from error


def build_dataset(
    warehouse: Warehouse,
    spec: DatasetSpec,
    store: DatasetStore,
    *,
    mode: Mode | None = None,
    release_type: ReleaseType | None = None,
    dev_tag: str | None = None,
    skip_if_exists: bool = True,
) -> dict:
    """Materialize a dataset (recordings + diagnoses parquet, optional signals reference) and register it.

    The dataset is identified by (name, version). A release type increments
    the latest semantic version; an explicit version in the spec is also
    supported. Development builds use a prerelease version containing the
    content hash so they cannot collide with release artifacts.

    Artifacts are written under a prefix unique to this build and registered
    afterwards with a compare-and-swap, so of concurrent builds of one release
    version exactly one is registered. The others raise ValueError, or return
    the registered manifest when their content is identical; their own
    artifacts stay unreferenced in the store.
    """
    mode = mode or ("release" if release_type is not None else "dev")
    if release_type is not None and mode != "release":
        raise ValueError("release_type can only be used with release mode")
    if release_type is not None and spec.version is not None:
        raise ValueError("remove version from the dataset spec when using release_type")

    recordings_table = warehouse.table("recordings")
    diagnoses_table = warehouse.table("diagnoses")
    # A never-written diagnoses table has no snapshot (None) and scans as empty with the full schema, so unlabeled
    # datasets build before any diagnoses are ingested. Hashes of builds over two snapshots are unchanged.
    snapshots = {
        "recordings": _snapshot_id(recordings_table),
        "diagnoses": _snapshot_id(diagnoses_table),
    }
    if snapshots["recordings"] is None:
        raise RuntimeError("Cannot build dataset over empty recordings table")
    if snapshots["diagnoses"] is None and spec.require_diagnosis:
        # Every recording would be dropped; most likely diagnoses were not ingested yet.
        raise ValueError(
            "Cannot build dataset with require_diagnosis over empty diagnoses table: "
            "ingest diagnoses or set require_diagnosis to false"
        )
    content_hash = _content_hash(spec, snapshots)
    # Checked again when registering (see _register); checking first avoids building what would be rejected.
    registry = warehouse.table("datasets")
    latest_release = latest_semantic_release(registry, spec.name) if release_type is not None else None
    repeated = _repeated_release(latest_release, content_hash, skip_if_exists)
    if repeated is not None:
        return repeated
    previous_release = None if latest_release is None else latest_release["version"]
    version = _resolve_version(
        spec,
        mode,
        content_hash,
        dev_tag,
        release_type=release_type,
        previous_release=previous_release,
    )

    if mode == "release":
        existing = _existing_release(registry, spec.name, version, content_hash, skip_if_exists)
        if existing is not None:
            return existing

    rec_filter = _recordings_filter(spec)
    rec_arrow = (
        recordings_table.scan(row_filter=rec_filter).to_arrow() if rec_filter else recordings_table.scan().to_arrow()
    )
    if spec.min_duration_sec is not None:
        rec_arrow = rec_arrow.filter(pc.greater_equal(rec_arrow["duration_sec"], spec.min_duration_sec))
    if spec.max_duration_sec is not None:
        rec_arrow = rec_arrow.filter(pc.less_equal(rec_arrow["duration_sec"], spec.max_duration_sec))

    diag_filter = _diagnoses_filter(spec)
    diag_arrow = (
        diagnoses_table.scan(row_filter=diag_filter).to_arrow() if diag_filter else diagnoses_table.scan().to_arrow()
    )

    rec_id_set = pa.array(rec_arrow["recording_id"].to_pylist(), type=pa.string())
    diag_arrow = diag_arrow.filter(pc.is_in(diag_arrow["recording_id"], value_set=rec_id_set))

    if spec.codes and spec.drop_recordings_with_other_codes:
        # Recordings with ANY label outside the allowed set are excluded
        # entirely (computed before the `codes` filter is applied).
        allowed_codes = pa.array(spec.codes, type=pa.string())
        offending = diag_arrow.filter(pc.invert(pc.is_in(diag_arrow["code"], value_set=allowed_codes)))
        offending_ids = pa.array(offending["recording_id"].to_pylist(), type=pa.string())
        if len(offending_ids) > 0:
            keep_mask = pc.invert(pc.is_in(rec_arrow["recording_id"], value_set=offending_ids))
            rec_arrow = rec_arrow.filter(keep_mask)
            diag_keep = pc.invert(pc.is_in(diag_arrow["recording_id"], value_set=offending_ids))
            diag_arrow = diag_arrow.filter(diag_keep)

    if spec.codes:
        allowed_codes = pa.array(spec.codes, type=pa.string())
        diag_arrow = diag_arrow.filter(pc.is_in(diag_arrow["code"], value_set=allowed_codes))

    if spec.require_diagnosis:
        labelled = pa.array(diag_arrow["recording_id"].to_pylist(), type=pa.string())
        rec_arrow = rec_arrow.filter(pc.is_in(rec_arrow["recording_id"], value_set=labelled))

    # Split keys (source, patient_id, source_recording_id) may be projected away.
    split_arrow, split_summary = assign_splits(rec_arrow, spec.split) if spec.split is not None else (None, None)

    if spec.recording_columns:
        columns = list(spec.recording_columns)
        if spec.signals_format is not None:
            columns += [c for c in SIGNAL_COLUMNS if c not in columns]
        rec_arrow = rec_arrow.select(columns)
    if split_arrow is not None:
        rec_arrow = rec_arrow.append_column(SPLIT_COLUMN, split_arrow)
    if spec.diagnosis_columns:
        diag_arrow = diag_arrow.select(spec.diagnosis_columns)

    base_key = _build_key(spec.name, version)
    rec_uri = store.write_parquet(f"{base_key}/recordings.parquet", rec_arrow)
    diag_uri = store.write_parquet(f"{base_key}/diagnoses.parquet", diag_arrow)
    artifacts: dict = {
        "recordings": {"uri": rec_uri, "rows": rec_arrow.num_rows},
        "diagnoses": {"uri": diag_uri, "rows": diag_arrow.num_rows},
    }
    if split_summary is not None:
        artifacts["recordings"]["splits"] = split_summary

    if spec.signals_format is not None:
        # Signals stay in the warehouse repository; the dataset only records where its signal_uris resolve.
        artifacts["signals"] = signals_artifact(warehouse.signals_storage, rec_arrow.num_rows)

    manifest: dict = {
        "name": spec.name,
        "version": version,
        "mode": mode,
        "release_type": release_type,
        "content_hash": content_hash,
        "spec": spec.model_dump(exclude_none=True),
        "snapshots": snapshots,
        "artifacts": artifacts,
        "created_at": datetime.datetime.now(datetime.UTC).isoformat(),
    }
    manifest_uri = store.write_json(f"{base_key}/manifest.json", manifest)
    manifest["manifest_uri"] = manifest_uri

    # A build that loses a concurrent release leaves its artifacts unreferenced; nothing resolves to them.
    unregistered = f"Artifacts of this build under {manifest_uri.rsplit('/', 1)[0]} are not registered."
    try:
        registered = _register(warehouse, manifest, previous_release, skip_if_exists)
    except ValueError:
        print(unregistered)
        raise
    if registered is not manifest:
        print(unregistered)
        return registered
    print(
        f"Built dataset {spec.name}@{version} ({mode}): "
        f"{rec_arrow.num_rows} recordings, {diag_arrow.num_rows} diagnoses"
    )
    return manifest
