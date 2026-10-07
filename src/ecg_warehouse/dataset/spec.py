import json
import re
from typing import Literal

from pydantic import BaseModel, Field, field_validator

SEMVER_CORE = re.compile(r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\Z")
# The name is the first segment of the store key `<name>/<version>/...`, so it must be one canonical segment:
# no separators, no `.`/`..`, and lowercase ASCII so names that differ only in case cannot share a directory on
# case-insensitive filesystems.
DATASET_NAME = re.compile(r"[a-z0-9](?:[a-z0-9._-]*[a-z0-9])?\Z")


class SplitSpec(BaseModel):
    """Patient-level train/test split.

    Each patient is hashed with `seed` into [0, 1) and lands in test when the
    value is below `test_fraction`. Assignment depends only on the patient, so
    new recordings never move existing ones between splits.
    """

    test_fraction: float = Field(gt=0, lt=1)
    seed: int = 0


class DatasetSpec(BaseModel):
    """Declarative description of a dataset cut over the warehouse.

    A spec is the only input the builder needs to deterministically rebuild
    the same dataset against a given warehouse snapshot.
    """

    name: str
    description: str | None = None
    # Optional explicit release version; omit it when using a release type.
    version: str | None = None

    # Recordings filters (pushed down to Iceberg where possible).
    sources: list[str] | None = None
    num_leads: int | None = None
    sampling_rate_hz: int | None = None
    min_duration_sec: float | None = None
    max_duration_sec: float | None = None

    # Diagnoses filters.
    code_systems: list[str] | None = None
    # When set, only diagnoses whose `code` is in this list are kept in the
    # output. Useful for narrow classification cohorts (e.g. norm/afib only).
    codes: list[str] | None = None
    # When true (and `codes` is set), drop any recording that has a diagnosis
    # with a code outside `codes` — guarantees a "pure" cohort where every
    # included recording's labels fall entirely inside the allowed set.
    drop_recordings_with_other_codes: bool = False
    # When true, drop recordings that have no diagnosis matching the filters.
    require_diagnosis: bool = False

    # Output column projection (None = keep all warehouse columns).
    recording_columns: list[str] | None = None
    diagnosis_columns: list[str] | None = None

    # When set, the dataset reads raw signals from the warehouse's icechunk
    # repository: recordings.parquet always keeps signal_uri/checksum_sha256
    # and the manifest records the repository. Nothing is copied.
    signals_format: Literal["icechunk"] | None = "icechunk"

    # When set, recordings.parquet gets a `split` column ("train"/"test"),
    # assigned per patient; a recording without patient_id is its own patient.
    split: SplitSpec | None = None

    @field_validator("name")
    @classmethod
    def validate_name(cls, name: str) -> str:
        if DATASET_NAME.fullmatch(name) is None:
            raise ValueError(
                "name must use lowercase letters, digits, '.', '_' and '-', and start and end with a letter or digit "
                "(for example, afib_binary)"
            )
        return name

    @field_validator("version")
    @classmethod
    def validate_version(cls, version: str | None) -> str | None:
        if version is not None and SEMVER_CORE.fullmatch(version) is None:
            raise ValueError("version must be MAJOR.MINOR.PATCH (for example, 1.2.3)")
        return version

    def content_payload(self) -> str:
        """Stable JSON serialization used as input to the content hash.

        The version label is excluded so that re-labeling does not change
        the identity of the materialized content.
        """
        payload = self.model_dump(exclude={"version"}, exclude_none=True)
        return json.dumps(payload, sort_keys=True, separators=(",", ":"))
