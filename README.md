
# Optimizely Decision Events → BigQuery Loader

A step‑by‑step runbook to fetch Optimizely decision event files and load them into BigQuery. Written for rerunnable, long‑running jobs (with safe resume and de‑duplication).

> **Scope**: Fetch and load Optimizely decision event data into BigQuery. Supports any date range and dataset/project configuration.

---

## 0) Check `requirements.txt`

Ensure the required packages are pinned and installed:

```txt
# example – adjust to your actual versions
google-cloud-bigquery>=3.25.0
pandas>=2.2.2
python-dateutil>=2.9
requests>=2.32.0
pyarrow>=17.0.0
```

> If you change `requirements.txt`, reinstall in your venv (see next step).

---

## 1) Create & activate a virtual environment

**PowerShell (Windows):**
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -U pip
pip install -r requirements.txt
```

**bash (macOS/Linux):**
```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -U pip
pip install -r requirements.txt
```

---

## 2) Prevent your machine from sleeping (so the run doesn’t stall)

**Windows (quick UI):** Settings → System → Power & Sleep → **Set Sleep = Never** while plugged in.

**Windows (PowerShell):**
```powershell
# Disable sleep on AC (0 = never). Revert later to your preferred minutes.
powercfg /x standby-timeout-ac 0
```

**macOS:**
```bash
# Keep the Mac awake while the command runs
caffeinate -dimsu -- command_you_run_here
```

> Remember to restore your normal settings after the load finishes.

### 🧠 Understanding Events vs. Decisions

Optimizely exports multiple types of data to S3. This repo primarily focuses on **decision events**, which represent when a user is bucketed into an experiment or feature flag.

However, some analyses — such as bot detection using user agent strings — may require **event-level data** (e.g., `pageview`, `click`, `custom` events).

If you're working with traffic quality or user agent filtering, ensure you're referencing the correct S3 prefix and schema for **event data**, not just decisions.

---

## 3) Test your Optimizely Personal Access Token (PAT)

Set your environment variables before running scripts. This allows secure access to Optimizely and S3.

### **PowerShell (Windows):**

```powershell
$env:OPTIMIZELY_PAT = 'your_pat_here'
$env:OPTIMIZELY_ACCOUNT_ID = 'your_account_id'
$env:S3_BUCKET = 'optimizely-events-data'
$env:S3_PREFIX = 'v1/account_id=your_account_id/type=decisions/'
$env:OPTIMIZELY_EXPORT_CRED_DURATION = '1h'
$env:AWS_REGION = 'us-east-1'
```

### **bash (macOS/Linux):**

```bash
export OPTIMIZELY_PAT="your_pat_here"
export OPTIMIZELY_ACCOUNT_ID="your_account_id"
export S3_BUCKET="optimizely-events-data"
export S3_PREFIX="v1/account_id=your_account_id/type=decisions/"
export OPTIMIZELY_EXPORT_CRED_DURATION="1h"
export AWS_REGION="us-east-1"
```

Then test your PAT:

```powershell
python test_pat.py --pat $env:OPTIMIZELY_PAT
```

> If environment variables don’t work for your script, pass `--pat ABC...` explicitly when calling the downloader.

---

## 4) Authenticate to Google Cloud & set the project

Use either **user authentication** or a **service account** to access GCP resources.

### **User authentication:**

```powershell
gcloud auth login

# To check your current auth status:
gcloud auth list
```

### **Service account authentication:**

```powershell
gcloud auth activate-service-account --key-file "C:\path\to\your-key.json"
```

### **Set your project (replace with your own project ID):**

```powershell
gcloud config set project <your-gcp-project-id>
```

Ensure the BigQuery API is enabled and you have permissions to create datasets/tables and run load jobs.

---

## 5) Create (or verify) the BigQuery dataset

```powershell
bq --location=EU mk -d --description "Optimizely decision events" optimizely_e3
# Safe if it already exists (command will warn/exit).
```

> Adjust `--location` to match your project/data residency.

---

## 6) Download decision or event files from Optimizely

Use the downloader script `load_optimizely_decisions_v3.py` which supports both decision and event data.

This version flattens the folder structure and truncates long event names to avoid Windows file system errors.

```powershell
# Example CLI – replace args to match your account and date range
python load_optimizely_decisions_v3.py `
  --auth optimizely `
  --pat $env:OPTIMIZELY_PAT `
  --account-id <your-account-id> `
  --type events `  # or 'decisions' for decision event data
  --start-date 2025-10-01 `
  --end-date   2025-11-06 `
  --out-dir    downloads `
  --resume
```

> AWS authentication is handled automatically via temporary credentials from the Optimizely Auth API when using `--auth optimizely`.

> If your run was interrupted, simply re-run with the same dates and `--resume` to skip already-downloaded files.

---

## 7) (Optional) Stage to GCS before loading (if your loader expects GCS URIs)

If your `stage_and_load_to_bq_gcs.py` script stages to Cloud Storage first, create a bucket and sync the local files:

```powershell
# Create bucket once (example location)
gsutil mb -l EU gs://<your-gcs-bucket>

# Sync local files to GCS
gsutil -m rsync -r downloads gs://<your-gcs-bucket>/optimizely_events
```

---

## 8) Load to BigQuery

You have two paths:

### Option A: Stage to GCS and load to BigQuery (Recommended for large batches)

This script reads its configuration from **environment variables** (no CLI flags). It will:

1. Upload all local `.parquet` files from `LOCAL_DIR` to `gs://$GCS_BUCKET/$GCS_PREFIX/`  
   (skips files that already exist with the same size), and
2. Load those GCS URIs into BigQuery as append jobs.

**PowerShell example:**
```powershell
$env:GCP_PROJECT_ID = "<your-gcp-project-id>"
$env:BQ_DATASET     = "optimizely_e3"
$env:BQ_TABLE_RAW   = "event_data"
$env:BQ_LOCATION    = "EU"

$env:GCS_BUCKET     = "<your-gcs-bucket>"      # e.g. optly-e3-staging
$env:GCS_PREFIX     = "optimizely_events"      # folder/prefix in the bucket (no leading slash)

$env:LOCAL_DIR      = "downloads"              # where your .parquet files live

python .\stage_and_load_to_bq_gcs.py
```

> **Tip:** If you’ve already staged files to GCS, you can re-run the script; it will
> skip re-uploads (same size) and proceed to the BigQuery load phase.

### Option B: Load directly from local files (Best for small batches or few files)

```powershell
python stage_and_load_to_bq_local.py `
  --source        downloads `
  --project       <your-gcp-project-id> `
  --dataset       optimizely_e3 `
  --table         event_data `
  --write-mode    append `
  --partition-col event_date `
  --batch-size    500
```

> **Note:** Option B can quickly hit BigQuery’s per-table load-job quota on large datasets. Prefer Option A for bulk loads.
>
> Tables may not appear until the first batch finishes loading.
---

## 9) Monitor progress & job history

- **Console:** BigQuery → Job history → look for recent *Load* jobs.
- **CLI:**
  ```powershell
  bq ls -j --all true --max_results=50
  bq show --job <JOB_ID>
  ```
- **Script logs:** Enable `--log-level DEBUG` (if available) to see “Loaded batch N” markers.

---

## 10) Resume after interruption

- Re‑run the **download** step with `--resume` and the same date range (it should skip already existing files).
- Re‑run the **load** step. If your loader writes in batches with `WRITE_APPEND`, it will continue where it left off. If you’re concerned about duplicates, see **Step 11**.
- Consider adding a lightweight **checkpoint** file (e.g., `state.json`) that tracks the last processed filename/date to auto‑resume.

---

## 11) De‑duplicate in BigQuery (by UUID)

If your raw loads can contain duplicates, use a `MERGE` into a canonical table keyed by the event UUID.

```sql
-- Staging table: optimizely_e3.decision_events_raw
-- Canonical table: optimizely_e3.decision_events

MERGE `caio-sandbox-468412.optimizely_e3.decision_events` T
USING (
  SELECT * FROM `caio-sandbox-468412.optimizely_e3.decision_events_raw`
) S
ON T.event_uuid = S.event_uuid
WHEN NOT MATCHED THEN
  INSERT ROW;
```

> Optionally, partition by `event_date` (DATE(TIMESTAMP_MICROS(event_timestamp))) and cluster by experiment / flag key for performance.

---

## 12) Validate completeness

- **Row counts per day** vs. the Optimizely support file:

```sql
SELECT event_date, COUNT(*) AS rows
FROM `caio-sandbox-468412.optimizely_e3.decision_events`
GROUP BY 1
ORDER BY 1;
```

- **Experiment coverage:**

```sql
SELECT experiment_key, COUNT(*) AS rows
FROM `caio-sandbox-468412.optimizely_e3.decision_events`
GROUP BY 1
ORDER BY rows DESC;
```

- **Date range check:** ensure coverage from `2024-10-30` through `2025-10-29`.

---

## Troubleshooting

- **Dataset exists, but no tables:** The loader hasn’t committed a batch yet or the first load job failed. Check BigQuery Job History and script logs.
- **Permission errors:** Confirm `BigQuery Data Editor` and `BigQuery Job User` on the project/dataset (or appropriate custom roles).
- **Schema mismatch:** Ensure your JSON schema matches the files. If using newline‑delimited JSON, set source format accordingly.
- **Region mismatch:** Dataset location must match GCS bucket location if loading from GCS.
- **Gzip files:** When loading `.jsonl.gz`, use the compression flag in your loader (`source_format=NEWLINE_DELIMITED_JSON`, `autodetect` off if providing schema).
- **Quotas:** Very large numbers of small files are slower. Prefer batching (concatenate locally or use GCS compose) so each load job ingests larger chunks (e.g., 100–500 MB per job).

---

## Quick commands (copy/paste)

```powershell
# 1) Activate venv
.\.venv\Scripts\Activate.ps1

# 2) Re-run downloader with PAT via CLI
python get_events.py --pat "<YOUR_PAT>" --start-date 2024-10-30 --end-date 2025-10-29 --out-dir data/optimizely_decisions --resume

# 3) Load in batches so tables appear earlier
python stage_and_load_to_bq.py \
  --source data/optimizely_decisions \
  --project caio-sandbox-468412 \
  --dataset optimizely_e3 \
  --table decision_events_raw \
  --schema schemas/decision_events.json \
  --write-mode append \
  --batch-size 500

# 4) (Optional) De‑dup into canonical table
bq query --use_legacy_sql=false "\
MERGE `caio-sandbox-468412.optimizely_e3.decision_events` T\
USING (SELECT * FROM `caio-sandbox-468412.optimizely_e3.decision_events_raw`) S\
ON T.event_uuid = S.event_uuid\
WHEN NOT MATCHED THEN INSERT ROW;\
"
```

---

### Notes
- If your scripts use different names/flags, edit the examples above accordingly.
- Keep an eye on long runs; consider logging progress every N files and emitting a load job every N files or M megabytes.

