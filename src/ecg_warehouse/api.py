"""The CLI's scenarios as a Python API, for Jupyter notebooks and scripts.

Each `ecg-warehouse` command maps to one `WarehouseAPI` method; the CLI only parses arguments and prints what the
method returns, so both entry points run the same code. Relative paths, including the default catalog under `data/`
and `.env`, resolve against the current working directory.
"""

import json
import os
import sys
from pathlib import Path
from typing import get_args

import duckdb
import pandas as pd
import pyarrow as pa
from dotenv import load_dotenv

from ecg_warehouse.dataset import (
    DatasetSpec,
    DatasetStore,
    LocalDatasetStore,
    S3DatasetStore,
    build_dataset,
    export_dataset,
    parse_dataset_ref,
)
from ecg_warehouse.dataset.builder import Mode, ReleaseType
from ecg_warehouse.dataset.export import ExportFormat
from ecg_warehouse.dataset.split import SPLIT_NAMES
from ecg_warehouse.ingest.sources.ptb_xl import BATCH_SIZE, ingest_ptb_xl
from ecg_warehouse.service import Warehouse

DEFAULT_STORE_ROOT = Path("data/datasets")
DOTENV_PATH = Path(".env")
SQL_TABLES = ("recordings", "diagnoses")

SpecInput = str | Path | dict | DatasetSpec


def load_env() -> None:
    """Load credentials from `.env` into the environment, if the file exists; variables already set keep their values."""
    load_dotenv(DOTENV_PATH)


def load_config(path: str | Path) -> dict:
    """Read a warehouse config file: PyIceberg catalog properties plus the optional `signals` storage."""
    with open(path, "r") as f:
        return json.load(f)


def default_mode() -> Mode:
    return "release" if os.environ.get("CI", "").lower() in ("1", "true") else "dev"


def load_spec(spec: SpecInput) -> DatasetSpec:
    """Validate a dataset spec given as a JSON file path, a dict, or a `DatasetSpec`."""
    if isinstance(spec, DatasetSpec):
        return spec
    if isinstance(spec, (str, Path)):
        with open(spec, "r") as f:
            return DatasetSpec.model_validate(json.load(f))
    return DatasetSpec.model_validate(spec)


def dataset_store(
    store_root: str | Path | None = None,
    s3_bucket: str | None = None,
    s3_prefix: str = "",
    s3_region: str | None = None,
) -> DatasetStore:
    """S3 store when a bucket is given, otherwise a local store under `store_root` (default: data/datasets)."""
    if s3_bucket:
        return S3DatasetStore(bucket=s3_bucket, prefix=s3_prefix, region=s3_region)
    return LocalDatasetStore(root=store_root or DEFAULT_STORE_ROOT)


def registered_datasets(warehouse: Warehouse) -> pa.Table:
    """All rows of the datasets registry, ordered by name and creation time."""
    rows = warehouse.table("datasets").scan().to_arrow()
    return rows.sort_by([("name", "ascending"), ("created_at", "ascending")])


class WarehouseAPI:
    """Warehouse operations mirroring the `ecg-warehouse` CLI; tables come back as DataFrames, JSON output as dicts.

    >>> wh = WarehouseAPI("cfg/local.json")
    >>> wh.sql("SELECT source, count(*) FROM recordings GROUP BY source")
    """

    def __init__(self, config: str | Path | dict | Warehouse | None = None) -> None:
        """`config` is a config file path or dict (as `ecg-warehouse --config`) or an open `Warehouse`.

        Without it the default local warehouse under `data/` is used. `.env` is loaded first (see `load_env`).
        """
        load_env()
        if isinstance(config, Warehouse):
            self.warehouse = config
        elif config is None:
            self.warehouse = Warehouse()
        elif isinstance(config, (str, Path)):
            self.warehouse = Warehouse(load_config(config))
        else:
            self.warehouse = Warehouse(config)

    def duckdb_connection(self) -> duckdb.DuckDBPyConnection:
        """In-memory DuckDB connection with the current `recordings` and `diagnoses` tables registered.

        Every call scans both tables, so reuse the connection for a series of queries.
        """
        conn = duckdb.connect()
        for name in SQL_TABLES:
            self.warehouse.table(name).scan().to_duckdb(name, connection=conn)
        return conn

    def sql(self, query: str) -> pd.DataFrame | None:
        """`ecg-warehouse sql`: run a DuckDB query over `recordings` and `diagnoses`."""
        relation = self.duckdb_connection().sql(query)
        return None if relation is None else relation.df()

    def ingest_ptb_xl(
        self, data_dir: str | Path, rate: int, batch_size: int = BATCH_SIZE, *, cleanup: bool = True
    ) -> None:
        """`ecg-warehouse ingest ptb-xl`: ingest PTB-XL recordings at `rate` Hz (100 or 500) with their diagnoses.

        Already ingested `ecg_id`s are skipped. `cleanup` runs `collect_garbage` afterwards, as the CLI does.
        """
        ingest_ptb_xl(warehouse=self.warehouse, data_dir=str(data_dir), sampling_rate_hz=rate, batch_size=batch_size)
        if cleanup:
            self.collect_garbage()

    def collect_garbage(self) -> int:
        """Expire signals store history and delete data superseded by appends; returns the bytes freed."""
        freed = self.warehouse._signals_store.collect_garbage()
        print(f"Signals store cleanup: freed {freed / 2**20:.1f} MiB", file=sys.stderr)
        return freed

    def build_dataset(
        self,
        spec: SpecInput,
        *,
        mode: Mode | None = None,
        dev_tag: str | None = None,
        release_type: ReleaseType | None = None,
        store_root: str | Path | None = None,
        s3_bucket: str | None = None,
        s3_prefix: str = "",
        s3_region: str | None = None,
    ) -> dict:
        """`ecg-warehouse dataset build`: materialize and register a dataset, returning its manifest.

        `spec` is a spec file path, a dict or a `DatasetSpec`. `mode` defaults to release when `release_type` is
        given or `CI` is set, else dev.
        """
        if mode is not None and mode not in get_args(Mode):
            raise ValueError(f"mode must be one of {', '.join(get_args(Mode))}")
        dataset_spec = load_spec(spec)
        resolved_mode = mode or ("release" if release_type is not None else default_mode())
        store = dataset_store(store_root, s3_bucket, s3_prefix, s3_region)
        return build_dataset(
            self.warehouse, dataset_spec, store, mode=resolved_mode, release_type=release_type, dev_tag=dev_tag
        )

    def list_datasets(self) -> pd.DataFrame:
        """`ecg-warehouse dataset list`: registered datasets, ordered by name and creation time."""
        return registered_datasets(self.warehouse).to_pandas()

    def export_dataset(
        self,
        dataset_ref: str,
        format: ExportFormat,
        output: str | Path,
        *,
        split: str | None = None,
        limit: int | None = None,
    ) -> dict:
        """`ecg-warehouse dataset export`: write `<name>@<version>`, or a subset, as CSV or NPZ files.

        Returns the export summary. `split` keeps one split, `limit` the first recordings in dataset order.
        """
        if split is not None and split not in SPLIT_NAMES:
            raise ValueError(f"split must be one of {', '.join(SPLIT_NAMES)}")
        if limit is not None and limit < 0:
            raise ValueError("limit must not be negative")
        name, version = parse_dataset_ref(dataset_ref)
        return export_dataset(self.warehouse, name, version, format, Path(output), split=split, limit=limit)
