# ECG Warehouse

A small data warehouse for 12-lead ECG recordings. It ingests public ECG datasets into versioned Apache Iceberg tables, stores the raw signals in an append-only Icechunk (Zarr) repository, and builds reproducible, versioned training datasets you can export to CSV or NumPy.

- **Tables** (`recordings`, `diagnoses`, `datasets`) live in an Iceberg catalog: SQLite locally, or a REST catalog on S3.
- **Signals** live in an Icechunk repository as `(signal, lead, sample)` float32 arrays. Each recording points at its signal by URI and stores a SHA-256 checksum of it.
- **Datasets** are declared in JSON specs. A build is pinned to the table snapshots it read, so the same spec over the same snapshots always produces the same content hash.
- **Querying** goes through DuckDB, using SQL over the current `recordings` and `diagnoses` tables.

The only ingest source at the moment is [PTB-XL 1.0.3](https://physionet.org/content/ptb-xl/1.0.3/).

## Requirements

- Python 3.12 or 3.13
- [uv](https://docs.astral.sh/uv/)
- Docker, only for the local S3 setup

## Installation

```bash
uv sync
```

This installs the package and the `ecg-warehouse` command into `.venv`. Run commands with `uv run ecg-warehouse ...`, or activate the environment first.

## Quick start

All paths are relative to the working directory, so run commands from the repository root.

**1. Download PTB-XL** from PhysioNet. The directory should contain `ptbxl_database.csv`, `scp_statements.csv`, `records100/` and `records500/`.

**2. Ingest it** at 100 Hz or 500 Hz. Each rate is ingested as its own set of recordings.

```bash
uv run ecg-warehouse ingest ptb-xl /path/to/ptb-xl-1.0.3 --rate 500
```

**3. Query the tables:**

```bash
uv run ecg-warehouse sql "SELECT source, sampling_rate_hz, count(*) FROM recordings GROUP BY ALL"
```

**4. Build a dataset** from a spec:

```bash
uv run ecg-warehouse dataset build cfg/datasets/afib_binary.json
```

**5. List and export it:**

```bash
uv run ecg-warehouse dataset list
```

```bash
uv run ecg-warehouse dataset export afib_binary@1.0.0 --format npz --output tmp/export --split train
```

Without `--config`, the CLI uses the local warehouse under `data/`, which is the same as `--config cfg/local.json`.

## Configuration

A config file holds PyIceberg catalog properties plus an optional `signals` section. Pass it with `--config`, before the subcommand:

```bash
uv run ecg-warehouse --config cfg/seaweedfs.json dataset list
```

| File | Catalog | Signals |
| --- | --- | --- |
| [cfg/local.json](cfg/local.json) | SQLite at `data/catalog.db`, tables under `data/` | Local repository at `data/ECG/signals.icechunk` |
| [cfg/seaweedfs.json](cfg/seaweedfs.json) | Iceberg REST catalog served by SeaweedFS | `s3://ecg-warehouse-signals/ECG/signals.icechunk` on SeaweedFS |

The `signals` section takes one of two forms:

```jsonc
{ "type": "local", "path": "data/ECG/signals.icechunk" }

{ "type": "s3", "bucket": "...", "prefix": "...", "region": "...", "endpoint_url": "..." }
```

Credentials stay out of config files. Put them in `.env` instead:

```bash
cp .env.example .env
```

The CLI and `WarehouseAPI` load `.env` from the working directory. Variables that are already set in the environment take precedence, so CI can set them directly.

| Variable | Used for |
| --- | --- |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | S3 access for the signals store and the S3 dataset store. They join the standard AWS chain, along with `~/.aws/credentials` and instance roles. |
| `ICEBERG_CATALOG_CREDENTIAL` | OAuth client credentials (`<client id>:<client secret>`) for a REST catalog. Used only when the config has no `credential` of its own. |

## CLI reference

```
ecg-warehouse [--config FILE] COMMAND
```

| Command | Description |
| --- | --- |
| `sql QUERY` | Run a DuckDB query over `recordings` and `diagnoses`. |
| `ingest ptb-xl DATA_DIR --rate {100,500} [--batch-size N]` | Ingest PTB-XL recordings and their SCP-ECG diagnoses. |
| `dataset build SPEC [--mode] [--dev-tag] [--release-type] [--store-root] [--s3-bucket] [--s3-prefix] [--s3-region]` | Materialize and register a dataset. Prints the manifest. |
| `dataset list` | List registered datasets. |
| `dataset export NAME@VERSION --format {csv,npz} --output DIR [--split {train,test}] [--limit N]` | Export a dataset, or a subset of it. |

Every ingest ends by garbage-collecting the signals store. This drops shard versions that later appends replaced, plus the history that pointed at them. No ingested signal is ever removed.

### Ingest behaviour

- Re-running an ingest is safe. `ecg_id`s that are already ingested at the requested rate are skipped.
- A run that failed between writing a batch's recordings and writing its diagnoses is repaired on the next run. The missing diagnoses are restored without re-reading the signals.
- Diagnoses get deterministic IDs derived from `(recording_id, annotator_id, code_system, code)`, so they are never duplicated.
- `sex` is stored using ISO/IEC 5218 codes: `1` male, `2` female, `0` unknown.

## Dataset specs

A spec is a JSON file that describes a slice of the warehouse. See [cfg/datasets/afib_binary.json](cfg/datasets/afib_binary.json) for an example.

| Field | Description |
| --- | --- |
| `name` | Lowercase letters, digits, `.`, `_` and `-`. **Required.** |
| `description` | Free text. |
| `version` | An explicit `MAJOR.MINOR.PATCH`. Leave it out when you use `--release-type`. |
| `sources` | Source names, e.g. `["ptb-xl-1.0.3"]`. |
| `num_leads`, `sampling_rate_hz` | Exact-match filters. |
| `min_duration_sec`, `max_duration_sec` | Inclusive duration bounds. |
| `code_systems` | Keep diagnoses from these code systems, e.g. `["SCP-ECG"]`. |
| `codes` | Keep only diagnoses with these codes. This also sets the label order of `y` in NPZ exports. |
| `drop_recordings_with_other_codes` | When `codes` is set, drop recordings that have any code outside the list. |
| `require_diagnosis` | Drop recordings that have no diagnosis left after filtering. |
| `recording_columns`, `diagnosis_columns` | Column projections. `signal_uri` and `checksum_sha256` are always kept when signals are enabled. |
| `signals_format` | `"icechunk"` (the default) records the signals repository in the manifest. Set it to `null` for a metadata-only dataset. |
| `split` | `{"test_fraction": 0.2, "seed": 0}` adds a `train`/`test` column. |

**Splits are assigned per patient.** Each patient is hashed with the seed, so all of a patient's recordings land in the same split. Adding new recordings never moves existing ones to the other split. A recording without a `patient_id` is treated as its own patient.

### Versions and modes

| Build | Version | How to run it |
| --- | --- | --- |
| Dev (the default) | `<version or 0.0.0>-dev.h<hash>`, or `...-dev.t<tag>.h<hash>` with `--dev-tag` | `dataset build SPEC` |
| Release, automatic bump | Increments the latest release | `dataset build SPEC --release-type minor` |
| Release, explicit | The `version` field in the spec | `dataset build SPEC --mode release` |

Release mode is the default when `CI=true`.

- Release versions are immutable. Rebuilding one with identical content returns the existing manifest; rebuilding it with different content is an error.
- An automatic bump is skipped when the content matches the latest release.
- Concurrent builds are registered with compare-and-swap, so exactly one build wins a given release version.

### Artifacts

Each build writes to `<store-root>/<name>/<version>/<build-id>/`:

- `recordings.parquet`
- `diagnoses.parquet`
- `manifest.json`: the spec, the table snapshot IDs, the content hash, the artifact URIs, the split counts and the signals location

Signals are not copied into the dataset. `recordings.parquet` references them by `signal_uri`.

`--store-root` defaults to `data/datasets`. With `--s3-bucket`, the artifacts go to S3 instead. That store has no endpoint setting, so it always talks to AWS.

### Export formats

- **`csv`** writes `recordings.csv` and `diagnoses.csv`.
- **`npz`** writes `signals.npz`, `meta.csv` (the recordings) and `diagnoses.csv`. `signals.npz` contains these arrays:
  - `X`: float32, shape `(recording, lead, sample)`
  - `y`: multi-hot uint8, shape `(recording, class)`
  - `classes`
  - `recording_id`
  - `split`, when the dataset has one

  Every signal is checked against its checksum as it is read. All signals must share one shape, so a dataset you export to NPZ should fix `num_leads`, `sampling_rate_hz` and duration.

## Python API

`WarehouseAPI` gives you the CLI's operations from Python. Tables come back as DataFrames and JSON output comes back as dicts:

```python
from ecg_warehouse.api import WarehouseAPI

wh = WarehouseAPI("cfg/local.json")  # or WarehouseAPI() for the default local warehouse
wh.sql("SELECT code, count(*) FROM diagnoses GROUP BY code ORDER BY 2 DESC")
manifest = wh.build_dataset("cfg/datasets/afib_binary.json", release_type="major")
wh.list_datasets()
wh.export_dataset("afib_binary@1.0.0", format="npz", output="tmp/export", split="train")
```

[notebooks/warehouse_api.ipynb](notebooks/warehouse_api.ipynb) walks through each scenario.

To read signals without exporting them, use the lower-level helpers in `ecg_warehouse.dataset`:

```python
from ecg_warehouse.dataset import load_dataset, read_dataset_signals

manifest, recordings, diagnoses = load_dataset(wh.warehouse, "afib_binary", "1.0.0")
for signal in read_dataset_signals(manifest, recordings):  # (lead, sample) arrays, checksum-verified
    ...
```

## Local S3 with SeaweedFS

[docker/docker-compose.yml](docker/docker-compose.yml) runs SeaweedFS with three endpoints:

- S3 API on port 8333
- Iceberg REST catalog on port 8181
- Filer UI on port 8888

It creates the `ecg-warehouse` and `ecg-warehouse-signals` buckets, plus the `ecg-tables` table bucket.

SeaweedFS takes its admin keys from `.env`, and `ecg-warehouse` reads the same file, so both sides use one set of keys. Any values work locally.

1. Copy `.env.example` to `.env` and fill in `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. Leave `ICEBERG_CATALOG_CREDENTIAL` as it is: it is built from those two keys, which is what the SeaweedFS catalog expects.

2. Start SeaweedFS:

   ```bash
   docker compose --env-file .env -f docker/docker-compose.yml up -d --wait
   ```

3. Run commands against it:

   ```bash
   uv run ecg-warehouse --config cfg/seaweedfs.json ingest ptb-xl /path/to/ptb-xl-1.0.3 --rate 500
   ```

Every compose command needs `--env-file .env`, because by default compose looks for `.env` in `docker/`.

The signals endpoint in `cfg/seaweedfs.json` is `127.0.0.1` on purpose. With a hostname, the AWS SDK used by Icechunk switches to virtual-hosted addressing, and SeaweedFS does not support it.

To stop the stack and delete everything stored in it, run `docker compose --env-file .env -f docker/docker-compose.yml down -v`.

## Project layout

```
src/ecg_warehouse/
  cli.py                CLI (click)
  api.py                WarehouseAPI: the CLI's operations as a Python API
  service.py            Warehouse: catalog and table setup
  schemas.py            Iceberg schemas and partition specs
  recordings_store.py   Icechunk signals store
  ingest/               Recording and diagnosis writers; sources/ptb_xl.py
  dataset/              Spec, builder, registry, splits, stores, signals, export
cfg/                    Warehouse configs and dataset specs
docker/                 SeaweedFS compose file
notebooks/              API walkthrough
data/                   Local warehouse (git-ignored)
```

## Development

```bash
uv run ruff check src
```

```bash
uv run ruff format src
```
