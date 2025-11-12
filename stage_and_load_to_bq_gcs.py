# stage_and_load_to_bq.py
import os
import sys
import pathlib
from typing import List, Iterable

from google.cloud import storage
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

# ---- CONFIG ----
PROJECT_ID   = os.getenv("GCP_PROJECT_ID", "<your-gcp-project-id>")
LOCAL_DIR    = os.getenv("LOCAL_DIR", "downloads")
GCS_BUCKET   = os.getenv("GCS_BUCKET", "<your-gcs-bucket>")
GCS_PREFIX   = os.getenv("GCS_PREFIX", "events/")                   # a folder inside your bucket
BQ_DATASET   = os.getenv("BQ_DATASET", "optimizely_e3")
BQ_TABLE_RAW = os.getenv("BQ_TABLE_RAW", "event_data_with_user_agents")
LOCATION     = os.getenv("BQ_LOCATION", "EU")  # BigQuery dataset + GCS bucket should be the same region


# Max 10,000 URIs per BigQuery load job; stay well under the limit
CHUNK_SIZE = 9000

def ensure_bucket(client: storage.Client, bucket_name: str, location: str) -> storage.Bucket:
    try:
        return client.get_bucket(bucket_name)
    except NotFound:
        bucket = client.bucket(bucket_name)
        bucket.location = location
        return client.create_bucket(bucket)

def ensure_dataset(client: bigquery.Client, project_id: str, dataset_id: str, location: str) -> bigquery.Dataset:
    ds_ref = bigquery.Dataset(f"{project_id}.{dataset_id}")
    try:
        return client.get_dataset(ds_ref)
    except NotFound:
        ds_ref.location = location
        return client.create_dataset(ds_ref)

def iter_local_parquet(root: pathlib.Path) -> Iterable[pathlib.Path]:
    for p in root.rglob("*.parquet"):
        if p.is_file():
            yield p

def upload_to_gcs(local_root: pathlib.Path, bucket: storage.Bucket, gcs_prefix: str) -> List[str]:
    """
    Upload .parquet files under local_root to gs://bucket/gcs_prefix/<relative path>
    Returns the list of gs:// URIs uploaded (in the same order).
    """
    uris = []
    for idx, fpath in enumerate(iter_local_parquet(local_root), 1):
        rel = fpath.relative_to(local_root)
        # Always use '/' for GCS keys
        gcs_key = f"{gcs_prefix.rstrip('/')}/{str(rel).replace('\\', '/')}"
        blob = bucket.blob(gcs_key)

        # Skip if same size already there
        size_on_disk = fpath.stat().st_size
        needs_upload = True
        if blob.exists():
            blob.reload()
            if blob.size == size_on_disk:
                needs_upload = False

        if needs_upload:
            print(f"[{idx}] UP   {fpath}  -> gs://{bucket.name}/{gcs_key}")
            blob.upload_from_filename(str(fpath))
        else:
            print(f"[{idx}] SKIP {fpath}  (already in GCS with same size)")

        uris.append(f"gs://{bucket.name}/{gcs_key}")
    return uris

def chunks(lst: List[str], n: int) -> Iterable[List[str]]:
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def load_parquet_to_bq(uris: List[str], project_id: str, dataset_id: str, table_id: str, location: str = "EU"):
    client = bigquery.Client(project=project_id, location=location)
    table_fq = f"{project_id}.{dataset_id}.{table_id}"

    # Create table if not exists by running an empty load with WRITE_APPEND, or just let BigQuery create on first load
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        time_partitioning=bigquery.TimePartitioning(  # ingestion-time partitioning
            type_=bigquery.TimePartitioningType.DAY
        )
    )

    # BigQuery allows up to 10,000 URIs per load job
    total_files = len(uris)
    done = 0
    for i, batch in enumerate(chunks(uris, CHUNK_SIZE), 1):
        print(f"[BQ] Loading batch {i} with {len(batch)} files into {table_fq} ...")
        job = client.load_table_from_uri(
            batch,
            table_fq,
            job_config=job_config,
            location=location
        )
        job.result()  # wait
        done += len(batch)
        print(f"[BQ] Batch {i} complete. {done}/{total_files} files loaded.")

    table = client.get_table(table_fq)
    print(f"[BQ] Load finished. Table rows: {table.num_rows:,}  | Table: {table_fq}")

def main():
    # Validate local dir
    local_root = pathlib.Path(LOCAL_DIR)
    if not local_root.exists():
        print(f"[ERROR] Local directory not found: {local_root}", file=sys.stderr)
        sys.exit(1)

    # GCS
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = ensure_bucket(storage_client, GCS_BUCKET, LOCATION)
    print(f"[OK] Using GCS bucket: gs://{bucket.name} (location={bucket.location})")

    # BQ dataset
    bq_client = bigquery.Client(project=PROJECT_ID, location=LOCATION)
    ensure_dataset(bq_client, PROJECT_ID, BQ_DATASET, LOCATION)
    print(f"[OK] Using BigQuery dataset: {PROJECT_ID}.{BQ_DATASET} (location={LOCATION})")

    # Upload all parquet files
    print("[STEP] Uploading local Parquet files to GCS…")
    uris = upload_to_gcs(local_root, bucket, GCS_PREFIX)
    if not uris:
        print("[WARN] Found no .parquet files under the local directory.")
        sys.exit(0)

    # Load to BigQuery
    print("[STEP] Loading Parquet files into BigQuery…")
    load_parquet_to_bq(uris, PROJECT_ID, BQ_DATASET, BQ_TABLE_RAW, location=LOCATION)

if __name__ == "__main__":
    main()
