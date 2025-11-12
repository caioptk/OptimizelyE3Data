
import argparse
import os
from google.cloud import bigquery

# ---- CLI ARGUMENTS ----
parser = argparse.ArgumentParser(description="Load local Parquet files into BigQuery")
parser.add_argument("--source", required=True, help="Local directory containing Parquet files")
parser.add_argument("--project", required=True, help="GCP project ID")
parser.add_argument("--dataset", required=True, help="BigQuery dataset name")
parser.add_argument("--table", required=True, help="BigQuery table name")
parser.add_argument("--write-mode", default="append", choices=["append", "overwrite"], help="Write mode")
parser.add_argument("--partition-col", default=None, help="Partition column (optional)")
parser.add_argument("--batch-size", type=int, default=500, help="Number of files per batch")
args = parser.parse_args()

# ---- BIGQUERY LOAD FUNCTION ----
def load_parquet_to_bq(file_paths, project_id, dataset_id, table_id, location="EU"):
    client = bigquery.Client(project=project_id)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        autodetect=True,
        write_disposition=(bigquery.WriteDisposition.WRITE_APPEND if args.write_mode == "append" else bigquery.WriteDisposition.WRITE_TRUNCATE)
    )

    table_ref = f"{dataset_id}.{table_id}"
    print(f"[BQ] Loading batch with {len(file_paths)} files into {project_id}.{table_ref} ...")

    for file_path in file_paths:
        with open(file_path, "rb") as f:
            job = client.load_table_from_file(f, table_ref, job_config=job_config, location=location)
            job.result()  # Wait for each file to finish
            print(f"Loaded {file_path} into {table_ref}")

    print("[BQ] Batch load complete.")

# ---- MAIN ----
def main():
    all_files = []
    for root, _, files in os.walk(args.source):
        for file in files:
            if file.endswith(".parquet"):
                all_files.append(os.path.join(root, file))

    print(f"Found {len(all_files)} parquet files in {args.source}")

    # Batch loading
    for i in range(0, len(all_files), args.batch_size):
        batch = all_files[i:i + args.batch_size]
        load_parquet_to_bq(batch, args.project, args.dataset, args.table)

if __name__ == "__main__":
    main()
