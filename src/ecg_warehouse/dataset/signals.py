from collections.abc import Iterator
from pathlib import Path

import icechunk
import numpy as np
import pyarrow as pa

from ecg_warehouse.recordings_store import (
    IcechunkSignalsStore,
    LocalSignalsStorage,
    S3SignalsStorage,
    SignalsStore,
    parse_signals_storage,
    signal_checksum,
)

SIGNALS_FORMAT = "icechunk"
# Kept in recordings.parquet whatever `recording_columns` says: they are how a dataset reaches its signals.
SIGNAL_COLUMNS = ["signal_uri", "checksum_sha256"]


def signals_artifact(storage: LocalSignalsStorage | S3SignalsStorage, rows: int) -> dict:
    """Manifest entry pointing a dataset at the signals repository its `signal_uri`s resolve in."""
    if isinstance(storage, LocalSignalsStorage):
        # The manifest outlives the working directory the build ran in.
        storage = storage.model_copy(update={"path": str(Path(storage.path).resolve())})
        uri = f"file://{storage.path}"
    else:
        uri = f"s3://{storage.bucket}/{storage.prefix.strip('/')}"
    return {"format": SIGNALS_FORMAT, "uri": uri, "storage": storage.model_dump(exclude_none=True), "rows": rows}


def dataset_signals_store(manifest: dict) -> SignalsStore:
    """Open the signals repository recorded in a dataset manifest (`manifest.json`)."""
    signals = manifest["artifacts"].get("signals")
    if signals is None or signals.get("format") != SIGNALS_FORMAT:
        raise ValueError(f"Dataset {manifest['name']}@{manifest['version']} has no {SIGNALS_FORMAT} signals")
    if "storage" not in signals:
        # Manifests rebuilt from registry rows only carry the repository URI.
        raise ValueError(f"Signals storage is recorded in the dataset's manifest.json ({manifest['manifest_uri']})")
    storage = parse_signals_storage(signals["storage"])
    # IcechunkSignalsStore would create a missing repository; a reader must not.
    if not icechunk.Repository.exists(storage.icechunk_storage()):
        raise FileNotFoundError(f"Signals repository {storage!r} does not exist")
    return IcechunkSignalsStore(storage)


def read_dataset_signals(manifest: dict, recordings: pa.Table, *, verify: bool = True) -> Iterator[np.ndarray]:
    """Yield the `(lead, sample)` signal of every `recordings` row, in row order, in its stored dtype
    (float32, or float16 for older ingests).

    `recordings` is the dataset's recordings.parquet, possibly filtered (e.g. by split). Signals are never
    rewritten, so reading from the branch tip returns the data the dataset was built over; `verify` checks
    each one against its `checksum_sha256`.
    """
    # Opened first: a dataset built without signals_format usually lacks the signal columns too, and the manifest
    # says why.
    store = dataset_signals_store(manifest)
    missing = [c for c in SIGNAL_COLUMNS if c not in recordings.column_names]
    if missing:
        raise ValueError(
            f"Dataset {manifest['name']}@{manifest['version']} recordings have no {', '.join(missing)} column(s)"
        )
    uris = recordings["signal_uri"].to_pylist()
    checksums = recordings["checksum_sha256"].to_pylist()
    stored = store.read(uris)
    for uri, checksum, signal in zip(uris, checksums, stored, strict=True):
        if verify and signal_checksum(signal) != checksum:
            raise ValueError(f"Signal {uri} does not match its checksum_sha256")
        yield signal
