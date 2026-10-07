import datetime
import hashlib
import io
from collections import defaultdict
from collections.abc import Iterable, Iterator, Sequence
from functools import cached_property
from itertools import batched
from typing import Annotated, Literal, Protocol

import icechunk
import numpy as np
import zarr
from pydantic import BaseModel, Field, TypeAdapter
from zarr.codecs import BloscCodec, BloscShuffle

DEFAULT_LOCAL_SIGNALS_PATH = "data/ECG/signals.icechunk"
SIGNAL_DTYPE = np.float32  # what ingest stores; older ingests stored float16
BRANCH = "main"
SHARD_SIGNALS = 256  # signals per stored object; an append rewrites only the array's last shard
READ_WINDOW = 512  # signals fetched ahead of the consumer
# Cleanup keeps newer objects: far longer than an append takes from writing chunks to committing them.
GC_GRACE = datetime.timedelta(minutes=10)


class LocalSignalsStorage(BaseModel):
    type: Literal["local"] = "local"
    path: str = DEFAULT_LOCAL_SIGNALS_PATH

    def icechunk_storage(self) -> icechunk.Storage:
        return icechunk.local_filesystem_storage(self.path)


class S3SignalsStorage(BaseModel):
    """S3 or S3-compatible storage; credentials come from the standard AWS chain."""

    type: Literal["s3"]
    bucket: str
    # Icechunk cannot create a repository at the bucket root.
    prefix: str = Field(min_length=1)
    region: str | None = None
    endpoint_url: str | None = None

    def icechunk_storage(self) -> icechunk.Storage:
        return icechunk.s3_storage(
            bucket=self.bucket,
            prefix=self.prefix,
            region=self.region,
            endpoint_url=self.endpoint_url,
            allow_http=self.endpoint_url is not None and self.endpoint_url.startswith("http://"),
            from_env=True,
        )


SignalsStorage = Annotated[LocalSignalsStorage | S3SignalsStorage, Field(discriminator="type")]
_SIGNALS_STORAGE = TypeAdapter(SignalsStorage)


def parse_signals_storage(raw: dict | None) -> LocalSignalsStorage | S3SignalsStorage:
    """Validate the `signals` section of the warehouse config; local storage when it is absent."""
    return LocalSignalsStorage() if raw is None else _SIGNALS_STORAGE.validate_python(raw)


def signal_checksum(signal: np.ndarray) -> str:
    """sha256 of the C-order `np.save` encoding, so the hash also covers shape and dtype."""
    buf = io.BytesIO()
    np.save(buf, np.ascontiguousarray(signal))
    return hashlib.sha256(buf.getvalue()).hexdigest()


class SignalsStore(Protocol):
    def append(self, signals: Sequence[tuple[str, np.ndarray]], message: str) -> list[str]: ...
    def read(self, uris: Iterable[str]) -> Iterator[np.ndarray]: ...
    def collect_garbage(self) -> int: ...


def _parse_uri(uri: str) -> tuple[str, int]:
    path, sep, index = uri.rpartition("#")
    if not sep or not index.isdigit():
        raise ValueError(f"Signal URI {uri!r} is not an <array path>#<index> reference")
    return path, int(index)


def _open_array(root: zarr.Group, path: str) -> zarr.Array:
    array = root[path]
    if not isinstance(array, zarr.Array):
        raise TypeError(f"Signals store path {path!r} is a group, expected an array")
    return array


def _create_array(root: zarr.Group, path: str, block: np.ndarray) -> zarr.Array:
    shape = block.shape[1:]
    return root.create_array(
        path,
        shape=(0, *shape),
        dtype=block.dtype,
        # One chunk per signal keeps single-signal reads cheap; shards keep the object count low.
        chunks=(1, *shape),
        shards=(SHARD_SIGNALS, *shape),
        compressors=BloscCodec(cname="zstd", clevel=5, shuffle=BloscShuffle.bitshuffle),
        dimension_names=("signal", "lead", "sample"),
    )


class IcechunkSignalsStore(SignalsStore):
    """Append-only signals in an icechunk repository: one `(signal, lead, sample)` zarr array per group, dtype and shape.

    A signal is addressed as `<group>/<dtype>/<leads>x<samples>#<index>` and never rewritten, so rows keep pointing at
    the data they were ingested with. Each `append` is a single commit, so a batch becomes visible atomically.
    The dtype is in the path because zarr silently casts appended data to the array's dtype, which would break the
    stored checksum. Arrays from before that (`<group>/<leads>x<samples>`) hold float16 and stay readable.
    """

    def __init__(self, storage: LocalSignalsStorage | S3SignalsStorage) -> None:
        self.storage = storage

    @cached_property
    def _repo(self) -> icechunk.Repository:
        # Opened lazily so commands that never touch signals don't need storage access.
        return icechunk.Repository.open_or_create(self.storage.icechunk_storage())

    def append(self, signals: Sequence[tuple[str, np.ndarray]], message: str) -> list[str]:
        """Append `(group, signal)` pairs and return each signal's URI, in input order."""
        session = self._repo.writable_session(BRANCH)
        root = zarr.open_group(session.store, mode="a")
        positions: dict[str, list[int]] = defaultdict(list)
        for pos, (group, signal) in enumerate(signals):
            positions[f"{group}/{signal.dtype.name}/{'x'.join(map(str, signal.shape))}"].append(pos)

        uris = [""] * len(signals)
        for path, rows in positions.items():
            block = np.stack([signals[pos][1] for pos in rows])
            try:
                array = _open_array(root, path)
            except KeyError:
                array = _create_array(root, path, block)
            start = array.shape[0]
            array.append(block, axis=0)
            for offset, pos in enumerate(rows):
                uris[pos] = f"{path}#{start + offset}"
        # Indices were allocated from this session's snapshot. If another writer appended to the same array
        # meanwhile, the rebase fails instead of reusing them; nothing is committed and the batch can be retried.
        session.commit(message, rebase_with=icechunk.ConflictDetector())
        return uris

    def read(self, uris: Iterable[str]) -> Iterator[np.ndarray]:
        """Yield signals in `uris` order, all from the same snapshot."""
        root = zarr.open_group(self._repo.readonly_session(BRANCH).store, mode="r")
        arrays: dict[str, zarr.Array] = {}
        for window in batched(uris, READ_WINDOW):
            positions: dict[str, list[tuple[int, int]]] = defaultdict(list)
            for pos, uri in enumerate(window):
                path, index = _parse_uri(uri)
                positions[path].append((pos, index))

            signals: dict[int, np.ndarray] = {}
            for path, rows in positions.items():
                if path not in arrays:
                    arrays[path] = _open_array(root, path)
                # One orthogonal selection per array lets zarr fetch the needed chunks concurrently.
                block = np.asarray(arrays[path].oindex[np.array([index for _, index in rows])])
                for (pos, _), signal in zip(rows, block):
                    signals[pos] = signal
            for pos in range(len(window)):
                yield signals[pos]

    def collect_garbage(self) -> int:
        """Delete data superseded by appends (rewritten last shards) and the history pointing at it.

        Only history is lost: appends never drop signals, so the branch tip still references every ingested signal.
        Returns the number of bytes deleted.
        """
        cutoff = datetime.datetime.now(datetime.UTC) - GC_GRACE
        self._repo.expire_snapshots(older_than=cutoff)
        return self._repo.garbage_collect(cutoff).bytes_deleted
