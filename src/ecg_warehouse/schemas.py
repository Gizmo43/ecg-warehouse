from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import (
    DateType,
    FloatType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
)

RECORDING_SCHEMA = Schema(
    NestedField(field_id=1, name="recording_id", field_type=StringType(), required=True),
    NestedField(field_id=2, name="source", field_type=StringType(), required=True),
    NestedField(field_id=3, name="source_recording_id", field_type=StringType(), required=True),
    NestedField(field_id=4, name="signal_uri", field_type=StringType(), required=True),
    NestedField(field_id=5, name="num_leads", field_type=IntegerType(), required=True),
    NestedField(field_id=6, name="sampling_rate_hz", field_type=LongType(), required=True),
    NestedField(field_id=7, name="duration_sec", field_type=FloatType(), required=True),
    NestedField(field_id=8, name="recording_date", field_type=DateType(), required=False),
    NestedField(field_id=9, name="patient_id", field_type=StringType(), required=False),
    NestedField(field_id=10, name="age_at_recording", field_type=IntegerType(), required=True),
    NestedField(field_id=11, name="sex", field_type=IntegerType(), required=False),
    NestedField(field_id=12, name="device", field_type=StringType(), required=False),
    NestedField(field_id=13, name="checksum_sha256", field_type=StringType(), required=True),
    NestedField(field_id=14, name="created_at", field_type=StringType(), required=True),
    NestedField(field_id=15, name="ingestion_batch_id", field_type=StringType(), required=True),
)

RECORDINGS_PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=5, field_id=1001, transform=IdentityTransform(), name="num_leads"),
    PartitionField(
        source_id=6,
        field_id=1002,
        transform=IdentityTransform(),
        name="sampling_rate_hz",
    ),
    PartitionField(source_id=7, field_id=1003, transform=IdentityTransform(), name="duration_sec"),
    PartitionField(source_id=2, field_id=1004, transform=IdentityTransform(), name="source"),
)

DIAGNOSES_SCHEMA = Schema(
    NestedField(field_id=1, name="diagnosis_id", field_type=StringType(), required=True),
    NestedField(field_id=2, name="recording_id", field_type=StringType(), required=True),
    NestedField(field_id=3, name="code", field_type=StringType(), required=True),
    NestedField(field_id=4, name="code_system", field_type=StringType(), required=True),
    NestedField(field_id=5, name="label_text", field_type=StringType(), required=False),
    NestedField(field_id=6, name="annotator_id", field_type=StringType(), required=True),
    NestedField(field_id=7, name="created_at", field_type=StringType(), required=True),
)

DIAGNOSES_PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=4, field_id=1000, transform=IdentityTransform(), name="code_system"),
)

DATASET_SCHEMA = Schema(
    NestedField(field_id=1, name="dataset_id", field_type=StringType(), required=True),
    NestedField(field_id=2, name="name", field_type=StringType(), required=True),
    NestedField(field_id=3, name="version", field_type=StringType(), required=True),
    NestedField(field_id=4, name="mode", field_type=StringType(), required=True),
    NestedField(field_id=5, name="content_hash", field_type=StringType(), required=True),
    NestedField(field_id=6, name="spec_json", field_type=StringType(), required=True),
    NestedField(field_id=7, name="recordings_snapshot_id", field_type=LongType(), required=True),
    # Null when built before any diagnoses were ingested; startup's union_by_name relaxes older tables.
    NestedField(field_id=8, name="diagnoses_snapshot_id", field_type=LongType(), required=False),
    NestedField(field_id=9, name="manifest_uri", field_type=StringType(), required=True),
    NestedField(field_id=10, name="recordings_uri", field_type=StringType(), required=True),
    NestedField(field_id=11, name="diagnoses_uri", field_type=StringType(), required=True),
    NestedField(field_id=12, name="recording_count", field_type=LongType(), required=True),
    NestedField(field_id=13, name="diagnosis_count", field_type=LongType(), required=True),
    NestedField(field_id=14, name="created_at", field_type=StringType(), required=True),
    NestedField(field_id=15, name="release_type", field_type=StringType(), required=False),
    NestedField(field_id=16, name="signals_uri", field_type=StringType(), required=False),
)

DATASETS_PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="name"),
)


TABLE_REGISTRY = {
    "recordings": (RECORDING_SCHEMA, RECORDINGS_PARTITION_SPEC),
    "diagnoses": (DIAGNOSES_SCHEMA, DIAGNOSES_PARTITION_SPEC),
    "datasets": (DATASET_SCHEMA, DATASETS_PARTITION_SPEC),
}
