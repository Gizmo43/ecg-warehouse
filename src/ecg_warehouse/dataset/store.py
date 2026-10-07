import json
from pathlib import Path
from typing import Protocol

import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs

INVALID_KEY_SEGMENTS = frozenset({"", ".", ".."})


def _key_segments(key: str) -> list[str]:
    """Split a store key into `/`-separated segments, rejecting keys that are not canonical.

    Absolute keys and `.`/`..`/empty segments could escape the store root or alias another key once a filesystem
    or S3 tool normalizes them, letting one dataset overwrite another's artifacts.
    """
    segments = key.split("/")
    if "\\" in key or INVALID_KEY_SEGMENTS.intersection(segments):
        raise ValueError(
            f"Invalid dataset store key {key!r}: expected a relative '/'-separated key "
            "without backslashes or empty, '.' or '..' segments"
        )
    return segments


def resolve_uri(uri: str) -> tuple[fs.FileSystem, str]:
    """Filesystem and path of an artifact URI returned by a `DatasetStore` (`file://` or `s3://`)."""
    # Store URIs are not percent-encoded: pyarrow's URI parser rejects local paths with spaces or non-ASCII.
    if uri.startswith("file://"):
        return fs.LocalFileSystem(), uri.removeprefix("file://")
    return fs.FileSystem.from_uri(uri)


def read_manifest(uri: str) -> dict:
    """Read a stored manifest.json, with `manifest_uri` set as on the manifest `build_dataset` returns."""
    filesystem, path = resolve_uri(uri)
    with filesystem.open_input_stream(path) as f:
        manifest = json.loads(f.read())
    manifest["manifest_uri"] = uri
    return manifest


class DatasetStore(Protocol):
    """Sink for materialized dataset artifacts (parquet + manifest)."""

    def write_parquet(self, key: str, table: pa.Table) -> str: ...
    def write_json(self, key: str, payload: dict) -> str: ...
    def exists(self, key: str) -> bool: ...


class LocalDatasetStore(DatasetStore):
    """Filesystem-backed dataset store, used for local dev/test builds."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        # Resolving follows symlinks too, so a link inside the root cannot redirect writes outside it.
        root = self.root.resolve()
        path = root.joinpath(*_key_segments(key)).resolve()
        if not path.is_relative_to(root):
            raise ValueError(f"Dataset store key {key!r} resolves outside the store root {root}")
        return path

    def write_parquet(self, key: str, table: pa.Table) -> str:
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, path)
        return f"file://{path.resolve()}"

    def write_json(self, key: str, payload: dict) -> str:
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, sort_keys=True))
        return f"file://{path.resolve()}"

    def exists(self, key: str) -> bool:
        return self._path(key).exists()


class S3DatasetStore(DatasetStore):
    """S3-backed dataset store for CI/CD release builds.

    Uses pyarrow's built-in S3 filesystem so no extra dependency is required.
    Credentials are sourced from the standard AWS environment chain
    (env vars, instance/OIDC role, ~/.aws/credentials).
    """

    def __init__(self, bucket: str, prefix: str = "", region: str | None = None) -> None:
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        if self.prefix:
            _key_segments(self.prefix)
        kwargs: dict = {}
        if region:
            kwargs["region"] = region
        self._fs = fs.S3FileSystem(**kwargs)

    def _full_key(self, key: str) -> str:
        # S3 keys are not normalized, but other tools that read the bucket may normalize them.
        _key_segments(key)
        return f"{self.prefix}/{key}" if self.prefix else key

    def _path(self, key: str) -> str:
        # pyarrow.fs paths for S3 are "bucket/key" without the s3:// prefix.
        return f"{self.bucket}/{self._full_key(key)}"

    def write_parquet(self, key: str, table: pa.Table) -> str:
        path = self._path(key)
        with self._fs.open_output_stream(path) as out:
            pq.write_table(table, out)
        return f"s3://{path}"

    def write_json(self, key: str, payload: dict) -> str:
        path = self._path(key)
        body = json.dumps(payload, indent=2, sort_keys=True).encode()
        with self._fs.open_output_stream(path) as out:
            out.write(body)
        return f"s3://{path}"

    def exists(self, key: str) -> bool:
        info = self._fs.get_file_info(self._path(key))
        return info.type != fs.FileType.NotFound
