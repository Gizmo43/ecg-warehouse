import os
from pathlib import Path

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.table import Table
from sqlalchemy.engine import make_url

from .recordings_store import IcechunkSignalsStore, SignalsStore, parse_signals_storage
from .schemas import TABLE_REGISTRY

LOCAL_ENV = {"type": "sql", "uri": "sqlite:///data/catalog.db", "warehouse": "file://data"}

NAMESPACE = "ecg"
IN_MEMORY_SQLITE = ":memory:"
CATALOG_CREDENTIAL_ENV = "ICEBERG_CATALOG_CREDENTIAL"


def add_env_credential(catalog_config: dict) -> None:
    """Give a REST catalog the OAuth client credentials in ICEBERG_CATALOG_CREDENTIAL, unless its config has some.

    Keeps the secret out of config files; `.env` can set the variable.
    """
    credential = os.environ.get(CATALOG_CREDENTIAL_ENV)
    if catalog_config.get("type") == "rest" and credential and "credential" not in catalog_config:
        catalog_config["credential"] = credential


def ensure_sqlite_parent_dir(catalog_config: dict) -> None:
    """Create the parent directory of a SQLite catalog database: SQLite creates the file, but not its directory."""
    uri = catalog_config.get("uri")
    if not isinstance(uri, str) or not uri.startswith("sqlite"):
        return
    database = make_url(uri).database
    # `file:` names (with `?uri=true`) are SQLite URIs, not paths; in-memory databases have no file.
    if not database or database == IN_MEMORY_SQLITE or database.startswith("file:"):
        return
    Path(database).parent.mkdir(parents=True, exist_ok=True)


class Warehouse:
    def __init__(self, config=LOCAL_ENV) -> None:
        # `signals` configures the signals store; the remaining keys are PyIceberg catalog properties.
        catalog_config = dict(config)
        self.signals_storage = parse_signals_storage(catalog_config.pop("signals", None))
        ensure_sqlite_parent_dir(catalog_config)
        add_env_credential(catalog_config)
        self.catalog = load_catalog(name="warehouse-catalog", **catalog_config)
        self._ensure_schema()

        self._signals_store: SignalsStore = IcechunkSignalsStore(self.signals_storage)

    def _ensure_schema(self) -> None:
        self.catalog.create_namespace_if_not_exists(NAMESPACE)
        for table_name, (schema, partition_spec) in TABLE_REGISTRY.items():
            table = self.catalog.create_table_if_not_exists(
                identifier=f"{NAMESPACE}.{table_name}",
                schema=schema,
                partition_spec=partition_spec,
            )
            table.update_schema().union_by_name(schema).commit()

    def table(self, name: str) -> Table:
        return self.catalog.load_table(f"{NAMESPACE}.{name}")

    def arrow_schema(self, name: str) -> pa.Schema:
        """Get the PyArrow schema for a table"""
        iceberg_schema = TABLE_REGISTRY[name][0]
        return schema_to_pyarrow(iceberg_schema, include_field_ids=False)
