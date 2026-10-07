import ast
import datetime
import itertools
import os
import sys
from collections import defaultdict
from collections.abc import Callable, Iterable, Iterator

import numpy as np
import pandas as pd
from pyiceberg.expressions import And, EqualTo
from tqdm import tqdm

from ecg_warehouse.ingest.diagnoses import DiagnoseInput, ingest_diagnoses_batch
from ecg_warehouse.ingest.recordings import RecordingsIngestorInput, ingest_recordings_batch
from ecg_warehouse.service import Warehouse

SOURCE_NAME = "ptb-xl-1.0.3"
SCP_CODE_SYSTEM = "SCP-ECG"
ANNOTATOR_ID = "ptbxl:scp_codes"

SEX_MAPPING = {0: 1, 1: 2}
SUPPORTED_SAMPLING_RATES = (100, 500)
FILENAME_COLUMN_BY_RATE = {100: "filename_lr", 500: "filename_hr"}
BATCH_SIZE = 1024

SignalLoader = Callable[[str], tuple[np.ndarray, int]]


def parse_scp_codes(raw: str) -> dict[str, float]:
    codes = ast.literal_eval(raw)
    return {str(code): float(probability) for code, probability in codes.items()}


def load_wfdb_signal(record_path: str) -> tuple[np.ndarray, int]:
    import wfdb

    signal, fields = wfdb.rdsamp(record_path)
    return signal.T, int(fields["fs"])


def _load_scp_labels(scp_statements_path: str) -> dict[str, str]:
    statements = pd.read_csv(scp_statements_path, index_col=0)
    return statements["description"].dropna().astype(str).to_dict()


def _existing_recording_ids(warehouse: Warehouse, sampling_rate_hz: int) -> dict[str, list[str]]:
    # Every ecg_id ships at both rates and each rate is a separate recording, so an ecg_id only counts as ingested at
    # the requested rate. Older, non-idempotent ingests may have left several recordings for one ecg_id and rate.
    row_filter = And(
        EqualTo(term="source", value=SOURCE_NAME), EqualTo(term="sampling_rate_hz", value=sampling_rate_hz)
    )
    rows = (
        warehouse.table("recordings")
        .scan(row_filter=row_filter, selected_fields=("source_recording_id", "recording_id"))
        .to_arrow()
        .to_pylist()
    )
    recording_ids: defaultdict[str, list[str]] = defaultdict(list)
    for row in rows:
        recording_ids[str(row["source_recording_id"])].append(str(row["recording_id"]))
    return dict(recording_ids)


def _recordings_without_diagnoses(warehouse: Warehouse, existing: dict[str, list[str]]) -> dict[str, list[str]]:
    # A batch's diagnoses are one append made after its recordings, so a recording with no rows from this annotator
    # comes from a run that failed in between.
    if not existing:
        return {}
    row_filter = And(
        EqualTo(term="code_system", value=SCP_CODE_SYSTEM), EqualTo(term="annotator_id", value=ANNOTATOR_ID)
    )
    diagnosed = set(
        warehouse.table("diagnoses")
        .scan(row_filter=row_filter, selected_fields=("recording_id",))
        .to_arrow()
        .column("recording_id")
        .to_pylist()
    )
    missing = {ecg_id: [rid for rid in rids if rid not in diagnosed] for ecg_id, rids in existing.items()}
    return {ecg_id: rids for ecg_id, rids in missing.items() if rids}


def _patient_id(raw) -> str | None:
    # ptbxl_database.csv stores patient_id as a float ("15709.0").
    if pd.isna(raw):
        return None
    return str(int(raw))


def _make_recording_input(row, ecg_id: str, signal_array: np.ndarray, sampling_rate_hz: int) -> RecordingsIngestorInput:
    leads_num, num_samples = signal_array.shape
    # PTB-XL timestamps carry no timezone and only the calendar date is kept.
    recording_date = datetime.datetime.strptime(str(row["recording_date"]), "%Y-%m-%d %H:%M:%S").date()  # noqa: DTZ007
    return RecordingsIngestorInput(
        signal_array=signal_array,
        source_name=SOURCE_NAME,
        source_recording_id=ecg_id,
        patient_id=_patient_id(row["patient_id"]),
        leads_num=leads_num,
        sampling_rate_hz=sampling_rate_hz,
        duration_sec=num_samples / sampling_rate_hz,
        device=str(row["device"]),
        age_at_recording=int(row["age"]),
        sex=SEX_MAPPING.get(int(row["sex"]), 0),
        recording_date=recording_date,
    )


def _make_diagnoses(recording_id: str, scp_codes: Iterable[str], scp_labels: dict[str, str]) -> list[DiagnoseInput]:
    return [
        DiagnoseInput(
            recording_id=recording_id,
            annotator_id=ANNOTATOR_ID,
            code=code,
            code_system=SCP_CODE_SYSTEM,
            label_text=scp_labels.get(code),
        )
        for code in scp_codes
    ]


def _ingest_recording_batch(
    warehouse: Warehouse, batch: list[tuple[RecordingsIngestorInput, dict[str, float]]], scp_labels: dict[str, str]
) -> None:
    ids_mapping = ingest_recordings_batch(warehouse=warehouse, batch=[recording for recording, _ in batch])
    codes_by_source_id = {recording.source_recording_id: scp_codes for recording, scp_codes in batch}
    diagnoses_batch = [
        diagnosis
        for source_recording_id, recording_id in ids_mapping.items()
        for diagnosis in _make_diagnoses(recording_id, codes_by_source_id[source_recording_id], scp_labels)
    ]
    ingest_diagnoses_batch(warehouse=warehouse, batch=diagnoses_batch)


def _missing_diagnoses(
    metadata: pd.DataFrame, missing: dict[str, list[str]], scp_labels: dict[str, str]
) -> Iterator[list[DiagnoseInput]]:
    for ecg_id, raw_codes in zip(metadata["ecg_id"], metadata["scp_codes"]):
        recording_ids = missing.get(str(ecg_id), [])
        scp_codes = parse_scp_codes(raw_codes) if recording_ids else {}
        if scp_codes:
            for recording_id in recording_ids:
                yield _make_diagnoses(recording_id, scp_codes, scp_labels)


def _restore_missing_diagnoses(
    warehouse: Warehouse,
    metadata: pd.DataFrame,
    existing: dict[str, list[str]],
    scp_labels: dict[str, str],
    batch_size: int,
) -> None:
    """Write the diagnoses of ingested recordings whose run failed before its diagnoses append; needs no signals."""
    missing = _recordings_without_diagnoses(warehouse, existing)
    if not missing:
        return
    restored = 0
    # Appends take whole recordings, so an interrupted restore never leaves a recording partially diagnosed.
    for recordings in itertools.batched(_missing_diagnoses(metadata, missing, scp_labels), batch_size):
        ingest_diagnoses_batch(warehouse=warehouse, batch=[row for diagnoses in recordings for row in diagnoses])
        restored += len(recordings)
    if restored:
        tqdm.write(f"Restored diagnoses of {restored} recordings ingested without them", file=sys.stderr)


def ingest_ptb_xl(
    warehouse: Warehouse,
    data_dir: str,
    sampling_rate_hz: int,
    scp_statements_path: str | None = None,
    batch_size: int = BATCH_SIZE,
    signal_loader: SignalLoader | None = None,
) -> None:
    """Ingest PTB-XL recordings at `sampling_rate_hz` with their SCP-ECG diagnoses.

    `ecg_id`s already ingested at `sampling_rate_hz` are skipped; the other rate's recordings are separate and don't
    count. A rerun first restores the diagnoses of this rate's recordings whose earlier run failed between the
    recordings and diagnoses appends, so rerunning resumes an incomplete ingest.
    """
    if sampling_rate_hz not in SUPPORTED_SAMPLING_RATES:
        raise ValueError(f"Unsupported sampling rate {sampling_rate_hz} Hz; expected one of {SUPPORTED_SAMPLING_RATES}")

    if signal_loader is None:
        signal_loader = load_wfdb_signal
    if scp_statements_path is None:
        candidate = os.path.join(data_dir, "scp_statements.csv")
        scp_statements_path = candidate if os.path.exists(candidate) else None
    scp_labels = _load_scp_labels(scp_statements_path) if scp_statements_path is not None else {}

    metadata = pd.read_csv(os.path.join(data_dir, "ptbxl_database.csv"))
    existing = _existing_recording_ids(warehouse, sampling_rate_hz)
    _restore_missing_diagnoses(warehouse, metadata, existing, scp_labels, batch_size)
    filename_column = FILENAME_COLUMN_BY_RATE[sampling_rate_hz]

    recording_batch: list[tuple[RecordingsIngestorInput, dict[str, float]]] = []
    for _, row in tqdm(metadata.iterrows(), total=len(metadata), ncols=100):
        ecg_id = str(row["ecg_id"])
        if ecg_id in existing:
            continue

        signal_array, actual_rate = signal_loader(os.path.join(data_dir, str(row[filename_column])))
        if actual_rate != sampling_rate_hz:
            raise ValueError(f"Signal for ecg {ecg_id} has rate {actual_rate} Hz, expected {sampling_rate_hz} Hz")

        recording_batch.append((_make_recording_input(row, ecg_id, signal_array, sampling_rate_hz), parse_scp_codes(row["scp_codes"])))

        if len(recording_batch) >= batch_size:
            _ingest_recording_batch(warehouse, recording_batch, scp_labels)
            recording_batch.clear()

    if recording_batch:
        _ingest_recording_batch(warehouse, recording_batch, scp_labels)
