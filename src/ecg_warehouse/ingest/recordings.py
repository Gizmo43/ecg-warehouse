# Должно содержать:
#  - Генерацию пути для записи
#  - Сохранение записи в warehouse
#  - Сохранение метаданных записи
#  - Сохранение диагнозов/сегментации(отдельно)

import datetime
import sys
import uuid

import numpy as np
import pyarrow as pa
from pydantic import BaseModel
from tqdm import tqdm

from ecg_warehouse.recordings_store import SIGNAL_DTYPE, signal_checksum
from ecg_warehouse.service import Warehouse


class RecordingsIngestorInput(BaseModel):
    model_config = {"arbitrary_types_allowed": True}

    signal_array: np.ndarray
    source_name: str
    source_recording_id: str
    patient_id: str | None
    leads_num: int
    sampling_rate_hz: int
    duration_sec: float
    device: str
    age_at_recording: int
    sex: int
    recording_date: datetime.date | None


def make_signal_group(metadata: RecordingsIngestorInput) -> str:
    # The store appends to one array per group, signal dtype and signal shape.
    return f"{metadata.source_name}/{metadata.sampling_rate_hz}"


def ingest_recordings_batch(warehouse: Warehouse, batch: list[RecordingsIngestorInput], batch_id: str | None = None) -> dict[str, str]:
    # TODO: Idempotence!
    if batch_id is None:
        batch_id = str(uuid.uuid4())

    now = datetime.datetime.now(datetime.UTC)

    recording_ids = []
    checksums = []

    ids_mapping: dict[str, str] = {}
    signals: list[tuple[str, np.ndarray]] = []

    for row in batch:
        signal = np.ascontiguousarray(row.signal_array, dtype=SIGNAL_DTYPE)

        recording_id = str(uuid.uuid4())

        checksums.append(signal_checksum(signal))
        recording_ids.append(recording_id)
        ids_mapping[row.source_recording_id] = recording_id
        signals.append((make_signal_group(row), signal))

    signal_uris = warehouse._signals_store.append(signals, message=f"Ingest batch {batch_id}")

    arrow_table = pa.table(
        {
            "recording_id": pa.array(recording_ids, type=pa.string()),
            "source": pa.array([m.source_name for m in batch], type=pa.string()),
            "source_recording_id": pa.array([m.source_recording_id for m in batch], type=pa.string()),
            "signal_uri": pa.array(signal_uris, type=pa.string()),
            "num_leads": pa.array([m.leads_num for m in batch], type=pa.int32()),
            "sampling_rate_hz": pa.array([m.sampling_rate_hz for m in batch], type=pa.int32()),
            "duration_sec": pa.array([m.duration_sec for m in batch], type=pa.float32()),
            "recording_date": pa.array([m.recording_date for m in batch], type=pa.date32()),
            "patient_id": pa.array([m.patient_id for m in batch], type=pa.string()),
            "age_at_recording": pa.array([m.age_at_recording for m in batch], type=pa.int32()),
            "sex": pa.array([m.sex for m in batch], type=pa.int32()),
            "device": pa.array([m.device for m in batch], type=pa.string()),
            "checksum_sha256": pa.array(checksums, type=pa.string()),
            "created_at": pa.array([now] * len(batch), type=pa.timestamp("us", tz="UTC")),
            "ingestion_batch_id": pa.array([batch_id] * len(batch), type=pa.string()),
        }
    )
    arrow_table = arrow_table.cast(warehouse.arrow_schema("recordings"))
    warehouse.table("recordings").append(arrow_table)
    tqdm.write(f"Ingested {len(arrow_table)} rows to recording table", file=sys.stderr)

    return ids_mapping
