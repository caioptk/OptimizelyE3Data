
# Optimizely Decision Events → BigQuery Loader

A step‑by‑step runbook to fetch Optimizely decision event files and load them into BigQuery. Written for rerunnable, long‑running jobs (with safe resume and de‑duplication).

> **Scope**: Decision event data from **2024‑10‑30 → 2025‑10‑29** across multiple experiments. Target dataset: `optimizely_e3` in project `caio-sandbox-468412`.

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

---

## 3) Test your Optimizely Personal Access Token (PAT)

Some environments require passing the PAT via CLI args for our scripts.

```powershell
# Example
python test_pat.py --pat "$env:OPTLY_PAT"
```

> If environment variables don’t work for your script, pass `--pat ABC...` explicitly when calling the downloader.

---

## 4) Authenticate to Google Cloud & set the project

```powershell
# If using user auth
gcloud auth login

# To check your current auth status:
gcloud auth list

# Or if using a service account
# gcloud auth activate-service-account --key-file "C:\path\to\key.json"

# Set project
gcloud config set project caio-sandbox-468412
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

## 6) Download decision event files from Optimizely

Use the downloader script that accepts the PAT via CLI.

```powershell
# Example CLI – replace script & args to match your repo
python get_events.py \
  --pat "<YOUR_PAT>" \
  --start-date 2024-10-30 \
  --end-date   2025-10-29 \
  --out-dir    data/optimizely_decisions \
  --resume      # if supported – skips already-downloaded files
```

**Verify files landed:**
```powershell
Get-ChildItem data/optimizely_decisions -Recurse | Measure-Object
```

> If your run was interrupted, simply re‑run with the same dates and `--resume`/"skip existing" behaviour. If `--resume` isn’t implemented, the script should still be idempotent if it skips existing files by name.

---

## 7) (Optional) Stage to GCS before loading (if your loader expects GCS URIs)

If your `stage_and_load_to_bq.py` script stages to Cloud Storage first, create a bucket and sync the local files:

```powershell
# Create bucket once (example location)
gsutil mb -l EU gs://caio-optly-staging

# Sync local files to GCS
gsutil -m rsync -r data/optimizely_decisions gs://caio-optly-staging/optimizely_decisions
```

---

## 8) Load to BigQuery

Run your loader. It can read from local files or from GCS, depending on implementation.

```powershell
# Example – adjust args to your script
python stage_and_load_to_bq.py \
  --source        data/optimizely_decisions \
  --project       caio-sandbox-468412 \
  --dataset       optimizely_e3 \
  --table         decision_events \
  --schema        schemas/decision_events.json \
  --write-mode    append \
  --partition-col event_date \
  --batch-size    500      # if supported, triggers periodic loads (tables appear earlier)
```

> **Why you might see datasets but no tables yet:** if the script batches rows and only calls `load_table_from_uri()`/`load_table_from_file()` at the end, BigQuery tables will only appear after the first successful batch load. If the process stopped around *"file 300 of 22,000"* before any batch finished, the dataset would exist with no tables yet.

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

