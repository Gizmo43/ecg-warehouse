import json
from dataclasses import dataclass
from pathlib import Path

import click

from ecg_warehouse.api import WarehouseAPI, registered_datasets
from ecg_warehouse.dataset.builder import Mode, ReleaseType
from ecg_warehouse.dataset.export import ExportFormat
from ecg_warehouse.dataset.split import SPLIT_NAMES


@dataclass
class Config:
    api: WarehouseAPI | None = None


@click.group()
@click.option("--config", type=click.Path(exists=True, dir_okay=False, file_okay=True, readable=True, path_type=Path))
@click.pass_context
def cli(ctx: click.Context, config):
    ctx.ensure_object(Config)
    ctx.obj.api = WarehouseAPI(config)


@cli.command()
@click.argument("query", type=str)
@click.pass_obj
def sql(ctx_obj: Config, query):
    """Make SQL queries"""

    if ctx_obj.api is None:
        raise RuntimeError("Failed to init data warehouse")

    conn = ctx_obj.api.duckdb_connection()
    print(conn.query(query=query))


@cli.group()
def ingest():
    """Ingest data"""


@ingest.result_callback()
@click.pass_obj
def ingest_cleanup(ctx_obj: Config, *_):
    """Runs after every successful ingest command."""
    if ctx_obj.api is None:
        raise RuntimeError("Failed to init data warehouse")

    ctx_obj.api.collect_garbage()


@ingest.command("ptb-xl")
@click.argument("data-dir", type=click.Path(exists=True, dir_okay=True, file_okay=False, readable=True, path_type=Path))
@click.option("--rate", type=click.Choice(["100", "500"]), required=True, help="Signal sampling rate to ingest.")
@click.option("--batch-size", type=int, default=1024, show_default=True)
@click.pass_obj
def ptb_xl(ctx_obj: Config, data_dir, rate, batch_size):
    """Ingest PTB-XL recordings with the selected signal sampling rate."""
    if ctx_obj.api is None:
        raise RuntimeError("Failed to init data warehouse")

    ctx_obj.api.ingest_ptb_xl(data_dir, rate=int(rate), batch_size=batch_size, cleanup=False)


@cli.group()
def dataset():
    """Manage materialized datasets."""


@dataset.command("build")
@click.argument(
    "spec_path",
    type=click.Path(exists=True, dir_okay=False, file_okay=True, readable=True, path_type=Path),
)
@click.option(
    "--mode",
    type=click.Choice(["dev", "release"]),
    default=None,
    help="Build mode. Defaults to release when CI=true, else dev.",
)
@click.option(
    "--dev-tag",
    type=str,
    default=None,
    help="Optional label in the dev prerelease version (e.g. user or branch).",
)
@click.option(
    "--release-type",
    type=click.Choice(["major", "minor", "patch"]),
    default=None,
    help="Increment the latest release version; implies release mode.",
)
@click.option(
    "--store-root",
    type=click.Path(file_okay=False, path_type=Path),
    default=None,
    help="Local store root (default: data/datasets).",
)
@click.option("--s3-bucket", type=str, default=None, help="S3 bucket to publish to.")
@click.option("--s3-prefix", type=str, default="", help="Optional S3 key prefix.")
@click.option("--s3-region", type=str, default=None, help="Optional AWS region for S3.")
@click.pass_obj
def dataset_build(
    ctx_obj: Config,
    spec_path: Path,
    mode: Mode | None,
    dev_tag: str | None,
    release_type: ReleaseType | None,
    store_root: Path | None,
    s3_bucket: str | None,
    s3_prefix: str,
    s3_region: str | None,
):
    """Build a dataset from a JSON spec file."""
    if ctx_obj.api is None:
        raise RuntimeError("Failed to init data warehouse")

    try:
        manifest = ctx_obj.api.build_dataset(
            spec_path,
            mode=mode,
            dev_tag=dev_tag,
            release_type=release_type,
            store_root=store_root,
            s3_bucket=s3_bucket,
            s3_prefix=s3_prefix,
            s3_region=s3_region,
        )
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(json.dumps(manifest, indent=2, default=str))


@dataset.command("list")
@click.pass_obj
def dataset_list(ctx_obj: Config):
    """List datasets registered in the warehouse."""
    if ctx_obj.api is None:
        raise RuntimeError("Failed to init data warehouse")
    rows = registered_datasets(ctx_obj.api.warehouse).to_pylist()
    click.echo(f"{'name':30s} {'version':30s} {'mode':8s} {'type':8s} rows_count")
    for row in rows:
        click.echo(
            f"{row['name']:30s} {row['version']:30s} {row['mode']:8s} {(row.get('release_type') or '-'):8s} "
            f"rec={row['recording_count']} diag={row['diagnosis_count']} "
            f"-> {row['manifest_uri']}"
        )


@dataset.command("export")
@click.argument("dataset_ref", metavar="NAME@VERSION")
@click.option(
    "--format",
    "export_format",
    type=click.Choice(["csv", "npz"]),
    required=True,
    help="csv: recordings.csv + diagnoses.csv; npz: signals.npz with X (signals), y (multi-hot labels), "
    "classes, recording_id and split, plus meta.csv (recordings) + diagnoses.csv.",
)
@click.option(
    "--output",
    type=click.Path(dir_okay=True, file_okay=False, path_type=Path),
    required=True,
    help="Directory to write the files to (created if missing).",
)
@click.option("--split", type=click.Choice(SPLIT_NAMES), default=None, help="Export only this split.")
@click.option("--limit", type=click.IntRange(min=0), default=None, help="Export at most this many recordings.")
@click.pass_obj
def dataset_export(
    ctx_obj: Config,
    dataset_ref: str,
    export_format: ExportFormat,
    output: Path,
    split: str | None,
    limit: int | None,
):
    """Export a registered dataset, or a subset of it, to CSV or NumPy files."""
    if ctx_obj.api is None:
        raise RuntimeError("Failed to init data warehouse")

    try:
        summary = ctx_obj.api.export_dataset(dataset_ref, export_format, output, split=split, limit=limit)
    except (ValueError, FileNotFoundError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    cli()
