# load_optimizely_decisions.py
import os
import sys
import re
import math
import argparse
from datetime import date, timedelta
from typing import Dict, Iterable, List, Optional, Tuple

# 1) Load environment variables from .env (local dev)
try:
    from dotenv import load_dotenv  # pip install python-dotenv
    load_dotenv()
except Exception:
    pass

# --- Defaults for your window ---
DEFAULT_START_DATE = date(2024, 10, 30)
DEFAULT_END_DATE   = date(2025, 10, 29)  # "yesterday" relative to your ask

# --- Configure your S3 location (set here or via env/CLI) ---
DEFAULT_BUCKET = os.getenv("S3_BUCKET", "YOUR_OPTIMIZELY_EXPORT_BUCKET")
DEFAULT_PREFIX = os.getenv("S3_PREFIX", "decision_events/")  # must end with "/" if it's a folder


def require_env(name: str, optional: bool = False, hint: Optional[str] = None) -> Optional[str]:
    val = os.getenv(name)
    if not val and not optional:
        msg = f"Missing required environment variable: {name}"
        if hint:
            msg += f"\nHint: {hint}"
        raise RuntimeError(msg)
    return val


def load_credentials() -> Dict[str, Optional[str]]:
    creds = {}
    # Optional: PAT not required for S3 pulls, but we keep it loaded for future API steps.
    creds["OPTIMIZELY_PAT"] = require_env("OPTIMIZELY_PAT", optional=True)

    # AWS creds (optional if your environment has a default profile/SSO/role; we support both)
    creds["AWS_ACCESS_KEY_ID"] = require_env("AWS_ACCESS_KEY_ID", optional=True)
    creds["AWS_SECRET_ACCESS_KEY"] = require_env("AWS_SECRET_ACCESS_KEY", optional=True)
    creds["AWS_REGION"] = os.getenv("AWS_REGION", "eu-west-1")
    return creds


def s3_client(creds: Dict[str, Optional[str]]):
    import boto3
    kwargs = {"region_name": creds.get("AWS_REGION") or "eu-west-1"}
    if creds.get("AWS_ACCESS_KEY_ID") and creds.get("AWS_SECRET_ACCESS_KEY"):
        kwargs["aws_access_key_id"] = creds["AWS_ACCESS_KEY_ID"]
        kwargs["aws_secret_access_key"] = creds["AWS_SECRET_ACCESS_KEY"]
    # else: let boto3 resolve credentials from the environment/SSO/role
    return boto3.client("s3", **kwargs)


def daterange(start: date, end: date) -> Iterable[date]:
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def ymd_path_for(prefix: str, d: date) -> str:
    """Construct a 'prefix/YYYY/MM/DD/' style sub-prefix (common export layout)."""
    p = prefix if prefix.endswith("/") else prefix + "/"
    return f"{p}{d.year:04d}/{d.month:02d}/{d.day:02d}/"


DATE_IN_KEY_REGEX = re.compile(r'(?<!\d)(20\d{2})[-/_]?([01]\d)[-/_]?([0-3]\d)(?!\d)')

def key_has_date_in_range(key: str, start: date, end: date) -> bool:
    """
    Fallback filter: look for YYYY[-_/]MM[-_/]DD anywhere in the key/filename.
    If multiple matches exist, we accept if ANY falls in range.
    """
    for m in DATE_IN_KEY_REGEX.finditer(key):
        try:
            y, mth, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
            found = date(y, mth, d)
            if start <= found <= end:
                return True
        except ValueError:
            continue
    return False


def list_s3_objects(client, bucket: str, prefix: str) -> Iterable[Dict]:
    """Generator yielding objects under bucket/prefix (handles pagination)."""
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


def list_by_day_if_partitioned(client, bucket: str, base_prefix: str, start: date, end: date) -> Iterable[Dict]:
    """List objects by iterating date-partitioned subfolders (prefix/YYYY/MM/DD/)."""
    for d in daterange(start, end):
        day_prefix = ymd_path_for(base_prefix, d)
        for obj in list_s3_objects(client, bucket, day_prefix):
            yield obj


def try_detect_partitioning(client, bucket: str, base_prefix: str, sample_date: date) -> bool:
    """Heuristic: if we can find objects under prefix/YYYY/MM/DD/, assume date partitioning."""
    day_prefix = ymd_path_for(base_prefix, sample_date)
    resp = client.list_objects_v2(Bucket=bucket, Prefix=day_prefix, MaxKeys=1)
    return "Contents" in resp and len(resp["Contents"]) > 0


def ensure_local_path(root_dir: str, key: str) -> str:
    """Map S3 key to local path under root_dir, creating parent directories."""
    # Normalize Windows-safe path
    local_path = os.path.join(root_dir, *key.split("/"))
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    return local_path


def human_size(n: int) -> str:
    if n is None:
        return "0B"
    units = ["B", "KB", "MB", "GB", "TB"]
    if n == 0:
        return "0B"
    i = int(math.floor(math.log(n, 1024)))
    return f"{n / (1024 ** i):.2f}{units[i]}"


def download_objects(
    client,
    bucket: str,
    objects: List[Dict],
    out_dir: str,
    dry_run: bool = False
) -> Tuple[int, int, int]:
    ok = skipped = failed = 0
    total = len(objects)
    for idx, obj in enumerate(objects, 1):
        key = obj["Key"]
        size = obj.get("Size", 0)
        local_path = ensure_local_path(out_dir, key)
        # Skip if exists and sizes match
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
    p = argparse.ArgumentParser(description="Extract Optimizely S3 export for a date range")
    p.add_argument("--bucket", default=DEFAULT_BUCKET, help="S3 bucket name")
    p.add_argument("--prefix", default=DEFAULT_PREFIX, help="S3 prefix (folder) for the export")
    p.add_argument("--out-dir", default="downloads", help="Local output directory")
    p.add_argument("--start-date", default=DEFAULT_START_DATE.isoformat(), help="YYYY-MM-DD")
    p.add_argument("--end-date", default=DEFAULT_END_DATE.isoformat(), help="YYYY-MM-DD")
    p.add_argument("--dry-run", action="store_true", help="List and count only; no downloads")
    p.add_argument("--force-scan", action="store_true",
                   help="Force full scan (do not assume date-partitioned subfolders)")
    return p.parse_args()


def main():
    args = parse_args()

    start = date.fromisoformat(args.start_date)
    end = date.fromisoformat(args.end_date)
    if end < start:
        raise ValueError("end-date cannot be earlier than start-date")

    creds = load_credentials()
    s3 = s3_client(creds)

    bucket = args.bucket
    prefix = args.prefix if args.prefix else ""
    if not bucket or "YOUR_OPTIMIZELY_EXPORT_BUCKET" in bucket:
        raise RuntimeError(
            "Please provide your S3 bucket via --bucket or env S3_BUCKET, "
            "and the export prefix via --prefix or env S3_PREFIX."
        )

    print(f"\nS3 extract plan:")
    print(f"  Bucket:   s3://{bucket}")
    print(f"  Prefix:   {prefix}")
    print(f"  Dates:    {start} → {end}")
    print(f"  Out dir:  {os.path.abspath(args.out_dir)}")
    print(f"  Dry run:  {args.dry_run}")
    print()

    # Strategy: if keys are in prefix/YYYY/MM/DD/, iterate per day (fast).
    use_partitioned = False
    if not args.force_scan:
        try:
            use_partitioned = try_detect_partitioning(s3, bucket, prefix, start)
        except Exception as e:
            print(f"[WARN] Partition detection failed, will scan: {e}", file=sys.stderr)

    objects: List[Dict] = []
    if use_partitioned:
        print("[INFO] Detected date-partitioned layout (prefix/YYYY/MM/DD/). Listing per-day...")
        for obj in list_by_day_if_partitioned(s3, bucket, prefix, start, end):
            objects.append(obj)
    else:
        print("[INFO] Scanning under base prefix and filtering keys by date heuristic...")
        for obj in list_s3_objects(s3, bucket, prefix):
            if key_has_date_in_range(obj["Key"], start, end):
                objects.append(obj)

    if not objects:
        print("[WARN] No objects found for the given date range and prefix.")
        sys.exit(0)

    # Sort by key for stable ordering
    objects.sort(key=lambda o: o["Key"])

    total_bytes = sum(o.get("Size", 0) for o in objects)
    print(f"[INFO] Objects to process: {len(objects)} (~{human_size(total_bytes)})\n")

    ok, skipped, failed = download_objects(
        client=s3,
        bucket=bucket,
        objects=objects,
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
