from pathlib import Path
from typing import Any, Literal

import numpy as np
import pyarrow as pa
import pyarrow.compute as _pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
from tqdm import tqdm

from ecg_warehouse.recordings_store import SIGNAL_DTYPE
from ecg_warehouse.service import Warehouse

from .registry import existing_dataset, registered_manifest
from .signals import read_dataset_signals
from .split import SPLIT_COLUMN
from .store import resolve_uri

pc: Any = _pc

ExportFormat = Literal["csv", "npz"]
RECORDINGS_CSV = "recordings.csv"
DIAGNOSES_CSV = "diagnoses.csv"
META_CSV = "meta.csv"
NPZ_FILE = "signals.npz"


def parse_dataset_ref(ref: str) -> tuple[str, str]:
    """Split a `<name>@<version>` reference."""
    name, sep, version = ref.rpartition("@")
    if not sep or not name or not version:
        raise ValueError(f"Dataset reference {ref!r} is not <name>@<version>")
    return name, version


def _read_parquet(uri: str) -> pa.Table:
    filesystem, path = resolve_uri(uri)
    return pq.read_table(path, filesystem=filesystem)


def load_dataset(warehouse: Warehouse, name: str, version: str) -> tuple[dict, pa.Table, pa.Table]:
    """Return the manifest, recordings and diagnoses of a registered dataset."""
    row = existing_dataset(warehouse, name, version)
    if row is None:
        raise ValueError(f"Dataset {name}@{version} is not registered")
    return registered_manifest(row), _read_parquet(row["recordings_uri"]), _read_parquet(row["diagnoses_uri"])


def _require_columns(table: pa.Table, label: str, columns: list[str]) -> None:
    missing = [c for c in columns if c not in table.column_names]
    if missing:
        raise ValueError(f"Dataset {label} have no {', '.join(missing)} column(s)")


def select_subset(
    recordings: pa.Table,
    diagnoses: pa.Table,
    *,
    split: str | None = None,
    limit: int | None = None,
) -> tuple[pa.Table, pa.Table]:
    """Narrow a dataset to a subset of its recordings, keeping dataset order, plus their diagnoses."""
    _require_columns(recordings, "recordings", ["recording_id"])
    if split is not None:
        _require_columns(recordings, "recordings", [SPLIT_COLUMN])
        recordings = recordings.filter(pc.equal(recordings[SPLIT_COLUMN], split))
    if limit is not None:
        recordings = recordings.slice(0, limit)

    _require_columns(diagnoses, "diagnoses", ["recording_id"])
    rec_id_set = pa.array(recordings["recording_id"].to_pylist(), type=pa.string())
    diagnoses = diagnoses.filter(pc.is_in(diagnoses["recording_id"], value_set=rec_id_set))
    return recordings, diagnoses


def label_classes(manifest: dict, diagnoses: pa.Table) -> list[str]:
    """Class order of `y`: the spec's `codes` when set, otherwise every code of the dataset, sorted."""
    codes = manifest["spec"].get("codes")
    if codes:
        return list(codes)
    _require_columns(diagnoses, "diagnoses", ["code"])
    return sorted(set(diagnoses["code"].to_pylist()))


def dataset_arrays(
    manifest: dict, recordings: pa.Table, diagnoses: pa.Table, classes: list[str]
) -> tuple[np.ndarray, np.ndarray]:
    """Stack signals into X `(recording, lead, sample)` float32 and labels into multi-hot y `(recording, class)`.

    Rows follow `recordings`. X needs every signal to have the same shape.
    """
    _require_columns(diagnoses, "diagnoses", ["recording_id", "code"])
    row_of = {recording_id: i for i, recording_id in enumerate(recordings["recording_id"].to_pylist())}
    class_of = {code: j for j, code in enumerate(classes)}
    y = np.zeros((recordings.num_rows, len(classes)), dtype=np.uint8)
    for recording_id, code in zip(diagnoses["recording_id"].to_pylist(), diagnoses["code"].to_pylist()):
        if recording_id in row_of and code in class_of:
            y[row_of[recording_id], class_of[code]] = 1

    x: np.ndarray | None = None
    signals = read_dataset_signals(manifest, recordings)
    for i, signal in enumerate(tqdm(signals, total=recordings.num_rows, ncols=100)):
        if x is None:
            # Preallocated so a large export holds the signals only once. A fixed dtype, not the first signal's:
            # older ingests stored float16, and widening them to float32 is exact.
            x = np.empty((recordings.num_rows, *signal.shape), dtype=SIGNAL_DTYPE)
        elif signal.shape != x.shape[1:]:
            raise ValueError(
                f"Signal {i} has shape {signal.shape}, expected {x.shape[1:]}: "
                "export a dataset whose recordings share num_leads, sampling_rate_hz and duration_sec"
            )
        x[i] = signal
    if x is None:
        x = np.empty((0, 0, 0), dtype=SIGNAL_DTYPE)
    return x, y


def export_csv(output: Path, recordings: pa.Table, diagnoses: pa.Table) -> dict[str, dict]:
    """Write recordings.csv and diagnoses.csv."""
    output.mkdir(parents=True, exist_ok=True)
    pacsv.write_csv(recordings, output / RECORDINGS_CSV)
    pacsv.write_csv(diagnoses, output / DIAGNOSES_CSV)
    return {RECORDINGS_CSV: {"rows": recordings.num_rows}, DIAGNOSES_CSV: {"rows": diagnoses.num_rows}}


def export_npz(
    output: Path, manifest: dict, recordings: pa.Table, diagnoses: pa.Table, classes: list[str]
) -> dict[str, dict]:
    """Write signals.npz, meta.csv (the recordings) and diagnoses.csv.

    signals.npz holds X, y, classes, recording_id and, when the dataset is split, split.
    """
    x, y = dataset_arrays(manifest, recordings, diagnoses, classes)
    # Fixed-width unicode arrays load without allow_pickle.
    arrays = {
        "X": x,
        "y": y,
        "classes": np.array(classes, dtype=str),
        "recording_id": np.array(recordings["recording_id"].to_pylist(), dtype=str),
    }
    if SPLIT_COLUMN in recordings.column_names:
        arrays[SPLIT_COLUMN] = np.array(recordings[SPLIT_COLUMN].to_pylist(), dtype=str)
    output.mkdir(parents=True, exist_ok=True)
    np.savez(output / NPZ_FILE, **arrays)
    pacsv.write_csv(recordings, output / META_CSV)
    pacsv.write_csv(diagnoses, output / DIAGNOSES_CSV)
    return {
        NPZ_FILE: {key: list(value.shape) for key, value in arrays.items()} | {"classes": classes},
        META_CSV: {"rows": recordings.num_rows},
        DIAGNOSES_CSV: {"rows": diagnoses.num_rows},
    }


def export_dataset(
    warehouse: Warehouse,
    name: str,
    version: str,
    export_format: ExportFormat,
    output: Path,
    *,
    split: str | None = None,
    limit: int | None = None,
) -> dict:
    """Export a registered dataset, or a subset of it, into `output` and describe what was written."""
    manifest, recordings, diagnoses = load_dataset(warehouse, name, version)
    subset, subset_diagnoses = select_subset(recordings, diagnoses, split=split, limit=limit)
    if export_format == "csv":
        files = export_csv(output, subset, subset_diagnoses)
    elif export_format == "npz":
        # Classes come from the whole dataset so every subset shares the same label columns.
        files = export_npz(output, manifest, subset, subset_diagnoses, label_classes(manifest, diagnoses))
    else:
        raise ValueError(f"Unsupported export format {export_format!r}")
    return {
        "name": name,
        "version": version,
        "format": export_format,
        "output": str(output.resolve()),
        "subset": {"split": split, "limit": limit},
        "recordings": {"dataset": recordings.num_rows, "exported": subset.num_rows},
        "diagnoses": {"dataset": diagnoses.num_rows, "exported": subset_diagnoses.num_rows},
        "files": files,
    }
