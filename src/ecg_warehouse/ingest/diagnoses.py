import datetime
import json
import random
import sys
import time
import uuid

import pyarrow as pa
from pydantic import BaseModel
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.expressions import EqualTo, In
from pyiceberg.table import Table, TableProperties
from tqdm import tqdm

from ecg_warehouse.service import Warehouse

DIAGNOSIS_KEY_FIELDS = ("recording_id", "annotator_id", "code_system", "code")
DIAGNOSIS_ID_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "ecg-warehouse:diagnoses:v1")
DiagnosisKey = tuple[str, str, str, str]

COMMIT_ATTEMPTS = 5
COMMIT_RETRY_WAIT_SEC = 0.1


class DiagnoseInput(BaseModel):
    recording_id: str
    annotator_id: str
    code: str
    code_system: str
    label_text: str | None


def _diagnosis_key(row: DiagnoseInput) -> DiagnosisKey:
    return (row.recording_id, row.annotator_id, row.code_system, row.code)


def _diagnosis_id_for_key(key: DiagnosisKey) -> str:
    key_json = json.dumps(key, separators=(",", ":"))
    return str(uuid.uuid5(DIAGNOSIS_ID_NAMESPACE, key_json))


def _load_diagnoses_table(warehouse: Warehouse) -> Table:
    table = warehouse.table("diagnoses")
    # Appending through the Table that was scanned is a compare-and-swap: the commit asserts that `main` still points
    # at the scanned snapshot, so a concurrent commit makes it fail instead of duplicating a diagnosis_id. PyIceberg's
    # own commit retry would undo that by rebasing the append onto the new head without validation, so it is turned
    # off on this in-memory copy (commits send only the staged updates, so the property never reaches the catalog)
    # and ingest_diagnoses_batch retries with a fresh scan instead.
    properties = {**table.metadata.properties, TableProperties.COMMIT_NUM_RETRIES: "0"}
    table.metadata = table.metadata.model_copy(update={"properties": properties})
    return table


def _existing_diagnosis_ids_by_key(table: Table, keys: set[DiagnosisKey]) -> dict[DiagnosisKey, str]:
    if not keys:
        return {}

    code_systems = sorted({key[2] for key in keys})
    row_filter = (
        EqualTo(term="code_system", value=code_systems[0])
        if len(code_systems) == 1
        else In(term="code_system", values=code_systems)
    )

    existing_rows = (
        table.scan(
            row_filter=row_filter,
            selected_fields=("diagnosis_id", *DIAGNOSIS_KEY_FIELDS),
        )
        .to_arrow()
        .to_pylist()
    )

    existing_ids: dict[DiagnosisKey, str] = {}
    for row in existing_rows:
        key = (row["recording_id"], row["annotator_id"], row["code_system"], row["code"])
        if key in keys and (key not in existing_ids or row["diagnosis_id"] < existing_ids[key]):
            existing_ids[key] = row["diagnosis_id"]

    return existing_ids


def _diagnoses_arrow_table(
    warehouse: Warehouse, new_rows: list[DiagnoseInput], new_ids: list[str], now: datetime.datetime
) -> pa.Table:
    arrow_table = pa.table(
        {
            "diagnosis_id": pa.array(new_ids, type=pa.string()),
            "recording_id": pa.array([row.recording_id for row in new_rows], type=pa.string()),
            "code": pa.array([row.code for row in new_rows], type=pa.string()),
            "code_system": pa.array([row.code_system for row in new_rows], type=pa.string()),
            "label_text": pa.array([row.label_text for row in new_rows], type=pa.string()),
            "annotator_id": pa.array([row.annotator_id for row in new_rows], type=pa.string()),
            "created_at": pa.array([now] * len(new_rows), type=pa.timestamp("us", tz="UTC")),
        }
    )
    return arrow_table.cast(warehouse.arrow_schema("diagnoses"))


def ingest_diagnoses_batch(
    warehouse: Warehouse, batch: list[DiagnoseInput], batch_id: str | None = None
) -> list[str]:
    if not batch:
        return []

    now = datetime.datetime.now(datetime.UTC)
    input_keys = [_diagnosis_key(row) for row in batch]
    rows_by_key: dict[DiagnosisKey, DiagnoseInput] = {}
    for key, row in zip(input_keys, batch):
        rows_by_key.setdefault(key, row)

    # The existence check and the append must see the same snapshot; see _load_diagnoses_table.
    for attempt in range(COMMIT_ATTEMPTS):
        table = _load_diagnoses_table(warehouse)
        existing_ids = _existing_diagnosis_ids_by_key(table, set(rows_by_key))
        new_keys = [key for key in rows_by_key if key not in existing_ids]
        new_ids = {key: _diagnosis_id_for_key(key) for key in new_keys}
        if not new_keys:
            break

        arrow_table = _diagnoses_arrow_table(
            warehouse, [rows_by_key[key] for key in new_keys], [new_ids[key] for key in new_keys], now
        )
        try:
            table.append(arrow_table)
            break
        except CommitFailedException:
            if attempt == COMMIT_ATTEMPTS - 1:
                raise
            # Jittered backoff keeps the writers that lost the race from colliding again on the retry.
            time.sleep(COMMIT_RETRY_WAIT_SEC * 2**attempt * random.uniform(1, 2))

    ids_by_key = existing_ids | new_ids
    tqdm.write(f"Ingested {len(new_keys)} rows to diagnoses table", file=sys.stderr)
    return [ids_by_key[key] for key in input_keys]
