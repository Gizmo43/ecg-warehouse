import hashlib
import json

import pyarrow as pa

from .spec import SplitSpec

SPLIT_COLUMN = "split"
TRAIN = "train"
TEST = "test"
SPLIT_NAMES = (TRAIN, TEST)
SPLIT_KEY_COLUMNS = ["source", "patient_id", "source_recording_id"]


def split_group_key(source: str, patient_id: str | None, source_recording_id: str) -> str:
    """Identity of the patient a recording belongs to.

    Patient ids are only unique within a source. A recording without one is
    its own patient. Source-native ids are used instead of the warehouse
    `recording_id` (a per-ingest uuid4) so re-ingesting a recording keeps its split.
    """
    if patient_id:
        return json.dumps([source, "patient", patient_id], separators=(",", ":"))
    return json.dumps([source, "recording", source_recording_id], separators=(",", ":"))


def split_bucket(seed: int, group_key: str) -> float:
    """Uniform value in [0, 1) that is stable across processes and machines."""
    digest = hashlib.sha256(f"{seed}:{group_key}".encode()).digest()
    return int.from_bytes(digest[:8], "big") / 2**64


def assign_splits(rec_arrow: pa.Table, split: SplitSpec) -> tuple[pa.Array, dict[str, dict[str, int]]]:
    """Return the split of every row plus per-split recording and patient counts."""
    group_keys = [
        split_group_key(source, patient_id, source_recording_id)
        for source, patient_id, source_recording_id in zip(
            *(rec_arrow[column].to_pylist() for column in SPLIT_KEY_COLUMNS)
        )
    ]
    split_by_key = {
        key: TEST if split_bucket(split.seed, key) < split.test_fraction else TRAIN for key in set(group_keys)
    }
    splits = [split_by_key[key] for key in group_keys]

    summary = {name: {"recordings": 0, "patients": 0} for name in SPLIT_NAMES}
    for name in splits:
        summary[name]["recordings"] += 1
    for name in split_by_key.values():
        summary[name]["patients"] += 1
    return pa.array(splits, type=pa.string()), summary
