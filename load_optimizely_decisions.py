# load_optimizely_decisions.py
import os
import sys
import math
import argparse
from datetime import date, timedelta, datetime, timezone
from typing import Dict, Iterable, List, Optional, Tuple

# Load .env for local dev
try:
    from dotenv import load_dotenv  # pip install python-dotenv
    load_dotenv(dotenv_path="C:/Users/caiopetelinkar/source/repos/OptimizelyE3Data/v1/.env")
except Exception:
    pass

import requests  # pip install requests

DEFAULT_START_DATE = date(2024, 10, 30)
DEFAULT_END_DATE   = date(2025, 10, 29)

DEFAULT_BUCKET = os.getenv("S3_BUCKET", "optimizely-events-data")
DEFAULT_PREFIX = os.getenv("S3_PREFIX", "v1/account_id=<REPLACE_ME>/type=decisions/")
DEFAULT_REGION = os.getenv("AWS_REGION", "us-east-1")
DEFAULT_DURATION = os.getenv("OPTIMIZELY_EXPORT_CRED_DURATION", "1h")  # 15m..1h

# ---------------------------
# Optimizely Auth → AWS creds
# ---------------------------

OPTLY_CRED_URL = "https://api.optimizely.com/v2/export/credentials"

def _isoformat_from_millis(ms: int) -> str:
    """Return ISO8601 UTC string for botocore expiry_time."""
    dt = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def fetch_optimizely_temp_creds(pat: str, duration: str, verbose: bool = False) -> Dict[str, str]:
    """Call Optimizely Authentication API to obtain temporary AWS credentials."""
    params = {"duration": duration} if duration else {}
    headers = {"Authorization": f"Bearer {pat}"}
    try:
        resp = requests.get(OPTLY_CRED_URL, headers=headers, params=params, timeout=30)
    except Exception as e:
        raise RuntimeError(f"Failed to call Optimizely credentials API: {e}")
    if resp.status_code != 200:
        if verbose:
            print(f"[DEBUG] Optimizely cred API {resp.status_code}: {resp.text[:300]}", file=sys.stderr)
        raise RuntimeError(f"Optimizely credentials API returned {resp.status_code}.")
    data = resp.json()
    creds = data.get("credentials") or {}
    if not all(k in creds for k in ("accessKeyId", "secretAccessKey", "sessionToken", "expiration")):
        raise RuntimeError("Optimizely credentials API response missing expected fields.")
    return {
        "access_key": creds["accessKeyId"],
        "secret_key": creds["secretAccessKey"],
        "token": creds["sessionToken"],
        "expiry_time": _isoformat_from_millis(int(creds["expiration"])),
    }

def s3_client_via_optimizely(pat: str, region_name: str, duration: str, verbose: bool = False):
    """
    Build a boto3 S3 client that auto-refreshes credentials using Optimizely Authentication API.
    """
    import boto3
    from botocore.credentials import RefreshableCredentials
    from botocore.session import get_session
    from botocore.config import Config

    def refresh():
        meta = fetch_optimizely_temp_creds(pat, duration, verbose=verbose)
        # Metadata keys must be named as below for botocore
        return {
            "access_key": meta["access_key"],
            "secret_key": meta["secret_key"],
            "token": meta["token"],
            "expiry_time": meta["expiry_time"],  # ISO8601 UTC
        }

    # Initial credentials + hook for refresh
    initial = refresh()
    botocore_sess = get_session()
    rc = RefreshableCredentials.create_from_metadata(
        metadata=initial,
        refresh_using=refresh,
        method="optimizely"
    )
    botocore_sess._credentials = rc
    botocore_sess.set_config_variable("region", region_name)

    # Signature V4 is required
    return boto3.Session(botocore_session=botocore_sess).client(
        "s3",
        region_name=region_name,
        config=Config(signature_version="s3v4")
    )

# ---------------------------
# Optional: static AWS creds
# ---------------------------

def s3_client_via_static(creds: Dict[str, Optional[str]]):
    import boto3
    from botocore.config import Config
    kwargs = {"region_name": creds.get("AWS_REGION")}
    if creds.get("AWS_ACCESS_KEY_ID") and creds.get("AWS_SECRET_ACCESS_KEY"):
        kwargs["aws_access_key_id"] = creds["AWS_ACCESS_KEY_ID"]
        kwargs["aws_secret_access_key"] = creds["AWS_SECRET_ACCESS_KEY"]
        if creds.get("AWS_SESSION_TOKEN"):
            kwargs["aws_session_token"] = creds["AWS_SESSION_TOKEN"]
    return boto3.client("s3", config=Config(signature_version="s3v4"), **kwargs)

def load_static_creds() -> Dict[str, Optional[str]]:
    return {
        "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
        "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
        "AWS_SESSION_TOKEN": os.getenv("AWS_SESSION_TOKEN"),
        "AWS_REGION": os.getenv("AWS_REGION", DEFAULT_REGION),
    }

# ---------------------------
# Extraction helpers
# ---------------------------

def daterange(start: date, end: date) -> Iterable[date]:
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)

def build_date_prefix(base_prefix: str, data_type: str, d: date) -> str:
    p = base_prefix if base_prefix.endswith("/") else base_prefix + "/"
    acceptable = ("type=decisions/", "type=events/", "type=decisions-rerun/")
     if not any(p.endswith(s) for s in acceptable):
        raise ValueError("S3_PREFIX must end with 'type=decisions/', 'type=events/', or 'type=decisions-rerun/'")
     if data_type == "decisions" and not (p.endswith("type=decisions/") or p.endswith("type=decisions-rerun/")):
         raise ValueError("You've set --type decisions but S3_PREFIX does not end with 'type=decisions/' or 'type=decisions-rerun/'")
     if data_type == "events" and not p.endswith("type=events/"):
         raise ValueError("You've set --type events but S3_PREFIX does not end with 'type=events/'")

    if data_type == "decisions" and not p.endswith("type=decisions/"):
        raise ValueError("You've set --type decisions but S3_PREFIX does not end with 'type=decisions/'")
    if data_type == "events" and not p.endswith("type=events/"):
        raise ValueError("You've set --type events but S3_PREFIX does not end with 'type=events/'")
    return f"{p}date={d.isoformat()}/"

def list_objects(client, bucket: str, prefix: str) -> Iterable[Dict]:
    token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        resp = client.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            yield obj
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break

def success_marker_exists(client, bucket: str, date_prefix: str, verbose=False) -> bool:
    key = date_prefix + "_SUCCESS"
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except Exception as e:
        if verbose:
            print(f"[DEBUG] HEAD s3://{bucket}/{key} failed: {e}", file=sys.stderr)
        return False

def ensure_local_path(root_dir: str, key: str) -> str:
    local_path = os.path.join(root_dir, *key.split("/"))
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    return local_path

def human_size(n: int) -> str:
    if not n:
        return "0B"
    units = ["B", "KB", "MB", "GB", "TB"]
    i = int(math.floor(math.log(n, 1024))) if n > 0 else 0
    return f"{n / (1024 ** i):.2f}{units[i]}"

def download_objects(client, bucket: str, objects: List[Dict], out_dir: str, dry_run: bool) -> Tuple[int, int, int]:
    ok = skipped = failed = 0
    total = len(objects)
    for idx, obj in enumerate(objects, 1):
        key = obj["Key"]
        size = obj.get("Size", 0)
        if key.endswith("/"):
            continue
        local_path = ensure_local_path(out_dir, key)
        if os.path.exists(local_path) and os.path.getsize(local_path) == size:
            skipped += 1
            print(f"[{idx}/{total}] SKIP  {key}  ({human_size(size)})")
            continue
        print(f"[{idx}/{total}] GET   {key}  -> {local_path} ({human_size(size)})")
        if dry_run:
            ok += 1
            continue
        try:
            client.download_file(bucket, key, local_path)
            ok += 1
        except Exception as e:
            failed += 1
            print(f"[ERROR] Failed to download {key}: {e}", file=sys.stderr)
    return ok, skipped, failed

def parse_args():
    p = argparse.ArgumentParser(description="Extract Optimizely Experimentation Events Export from S3 (via Optimizely temp creds)")
    p.add_argument("--auth", choices=["optimizely", "aws"], default="optimizely",
                   help="Use 'optimizely' temp creds (default) or static 'aws' creds from env")
    p.add_argument("--duration", default=DEFAULT_DURATION, help="Optimizely temp credential duration, e.g. 15m..1h")
    p.add_argument("--bucket", default=DEFAULT_BUCKET, help="S3 bucket (default: optimizely-events-data)")
    p.add_argument("--prefix", default=DEFAULT_PREFIX, help="Base prefix e.g. v1/account_id=223.../type=decisions/")
    p.add_argument("--type", choices=["decisions", "events"], default="decisions", help="Partition type to read")
    p.add_argument("--region", default=DEFAULT_REGION, help="AWS region (optimizely bucket is us-east-1)")
    p.add_argument("--out-dir", default="downloads", help="Local output directory")
    p.add_argument("--start-date", default=DEFAULT_START_DATE.isoformat(), help="YYYY-MM-DD")
    p.add_argument("--end-date", default=DEFAULT_END_DATE.isoformat(), help="YYYY-MM-DD")
    p.add_argument("--dry-run", action="store_true", help="List without downloading")
    p.add_argument("--require-success", dest="require_success", action="store_true", default=True,
                   help="Only process days that have a _SUCCESS marker (default)")
    p.add_argument("--ignore-success", dest="require_success", action="store_false",
                   help="Process days even if _SUCCESS is missing")
    p.add_argument("--verbose", action="store_true", help="Print debug details")
    p.add_argument("--pat", default=None, help="Optimizely Personal Access Token (overrides OPTIMIZELY_PAT from env)")
    return p.parse_args()

def main():
    args = parse_args()

    start = date.fromisoformat(args.start_date)
    end = date.fromisoformat(args.end_date)
    if end < start:
        raise ValueError("end-date cannot be earlier than start-date")

    if "<REPLACE_ME>" in args.prefix or "account_id=" not in args.prefix:
        raise RuntimeError(
            "Please set your S3 prefix to include your account id, e.g.\n"
            "  v1/account_id=22397541806/type=decisions/\n"
            "Put this in .env as S3_PREFIX=... or pass --prefix ..."
        )

    # Build S3 client

if args.auth == "optimizely":
    pat = args.pat or os.getenv("OPTIMIZELY_PAT")
    if not pat:
        parser.error("--pat is required for --auth optimizely (or set OPTIMIZELY_PAT)")

    if not pat:
        raise RuntimeError("Missing Optimizely PAT: provide via --pat or set OPTIMIZELY_PAT in environment or .env file")
        s3 = s3_client_via_optimizely(pat=pat, region_name=args.region, duration=args.duration, verbose=args.verbose)
        print("[OK] Using Optimizely temporary AWS credentials (auto-refresh).")
    else:
        static = load_static_creds()
        if not (static["AWS_ACCESS_KEY_ID"] and static["AWS_SECRET_ACCESS_KEY"]):
            raise RuntimeError("Missing AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY for --auth aws")
        s3 = s3_client_via_static(static)
        print("[OK] Using static AWS credentials from environment.")

    print("\nS3 extract plan:")
    print(f"  Bucket:   s3://{args.bucket}")
    print(f"  Base:     {args.prefix}")
    print(f"  Type:     {args.type}")
    print(f"  Dates:    {start} → {end}")
    print(f"  Out dir:  {os.path.abspath(args.out_dir)}")
    print(f"  Dry run:  {args.dry_run}")
    print(f"  Require _SUCCESS: {args.require_success}\n")

    all_objects: List[Dict] = []

    for d in daterange(start, end):
        date_prefix = build_date_prefix(args.prefix, args.type, d)
        has_success = success_marker_exists(s3, args.bucket, date_prefix, verbose=args.verbose)
        if not has_success and args.require_success:
            print(f"[INFO] {date_prefix} — no _SUCCESS, skipping")
            continue

        day_objs = [o for o in list_objects(s3, args.bucket, date_prefix)
                    if o["Key"].endswith(".parquet")]
        if not day_objs:
            print(f"[WARN] {date_prefix} — no parquet files found")
            continue
        print(f"[INFO] {date_prefix} — {len(day_objs)} parquet file(s)")
        all_objects.extend(day_objs)

    if not all_objects:
        print("[WARN] No files found to download in the selected range.")
        sys.exit(0)

    all_objects.sort(key=lambda o: o["Key"])
    total_bytes = sum(o.get("Size", 0) for o in all_objects)
    print(f"\n[INFO] Total files: {len(all_objects)} (~{human_size(total_bytes)})\n")

    ok, skipped, failed = download_objects(
        client=s3,
        bucket=args.bucket,
        objects=all_objects,
        out_dir=args.out_dir,
        dry_run=args.dry_run
    )

    print("\nSummary:")
    print(f"  Downloaded: {ok}")
    print(f"  Skipped:    {skipped}")
    print(f"  Failed:     {failed}")
    print(f"  Output:     {os.path.abspath(args.out_dir)}")
    if failed > 0:
        sys.exit(2)

if __name__ == "__main__":
    main()
