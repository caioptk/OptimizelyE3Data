from __future__ import annotations
import os
import sys
import math
import argparse
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Tuple

# Optional: load a .env if present (no hard failure if missing)
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

import json
import re
import urllib.parse

import requests  # type: ignore

# -----------------------------
# Defaults
# -----------------------------
DEFAULT_START_DATE = date(2024, 10, 30)
DEFAULT_END_DATE = date(2025, 10, 29)
DEFAULT_REGION = os.getenv("AWS_REGION", "us-east-1")
DEFAULT_DURATION = os.getenv("OPTIMIZELY_EXPORT_CRED_DURATION", "1h")  # 15m..1h
DEFAULT_BUCKET = os.getenv("S3_BUCKET", "optimizely-events-data")

# -----------------------------
# Optimizely Auth API
# -----------------------------
OPTLY_CRED_URL = "https://api.optimizely.com/v2/export/credentials"


def _isoformat_from_millis(ms: int) -> str:
    dt = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass
class OptlyTempCredentials:
    access_key: str
    secret_key: str
    token: str
    expiry_time: str  # ISO8601 UTC string
    s3_path: Optional[str] = None  # e.g. s3://optimizely-events-data/v1/account_id=123


def fetch_optimizely_temp_creds(pat: str, duration: str, verbose: bool = False) -> OptlyTempCredentials:
    """Call Optimizely Authentication API to obtain temporary AWS credentials and s3Path."""
    params = {"duration": duration} if duration else {}
    headers = {"Authorization": f"Bearer {pat}"}
    try:
        resp = requests.get(OPTLY_CRED_URL, headers=headers, params=params, timeout=30)
    except Exception as e:
        raise RuntimeError(f"Failed to call Optimizely credentials API: {e}")

    if resp.status_code != 200:
        if verbose:
            sys.stderr.write(f"[DEBUG] Optimizely cred API {resp.status_code}: {resp.text[:300]}\n")
        raise RuntimeError(f"Optimizely credentials API returned {resp.status_code}.")

    try:
        data = resp.json()
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Invalid JSON from Optimizely credentials API: {e}\nBody: {resp.text[:200]}")

    creds = data.get("credentials") or {}
    for k in ("accessKeyId", "secretAccessKey", "sessionToken", "expiration"):
        if k not in creds:
            raise RuntimeError("Optimizely credentials API response missing expected fields.")

    s3_path = data.get("s3Path")
    return OptlyTempCredentials(
        access_key=creds["accessKeyId"],
        secret_key=creds["secretAccessKey"],
        token=creds["sessionToken"],
        expiry_time=_isoformat_from_millis(int(creds["expiration"])),
        s3_path=s3_path,
    )


# -----------------------------
# AWS / S3 helpers
# -----------------------------

def s3_client_via_optimizely(pat: str, region_name: str, duration: str, verbose: bool = False):
    """Create a boto3 S3 client that auto-refreshes creds via the Optimizely Auth API.
    Returns (s3_client, initial_s3_path)
    """
    import boto3  # type: ignore
    from botocore.credentials import RefreshableCredentials  # type: ignore
    from botocore.session import get_session  # type: ignore
    from botocore.config import Config  # type: ignore

    holder: Dict[str, Optional[str]] = {"s3_path": None}

    def refresh():
        meta = fetch_optimizely_temp_creds(pat, duration, verbose=verbose)
        if meta.s3_path and not holder.get("s3_path"):
            holder["s3_path"] = meta.s3_path
        # Botocore requires these exact keys
        return {
            "access_key": meta.access_key,
            "secret_key": meta.secret_key,
            "token": meta.token,
            "expiry_time": meta.expiry_time,  # ISO8601
        }

    initial = refresh()

    botocore_sess = get_session()
    rc = RefreshableCredentials.create_from_metadata(
        metadata=initial,
        refresh_using=refresh,
        method="optimizely",
    )
    botocore_sess._credentials = rc
    botocore_sess.set_config_variable("region", region_name)

    client = boto3.Session(botocore_session=botocore_sess).client(
        "s3", region_name=region_name, config=Config(signature_version="s3v4")
    )
    return client, holder.get("s3_path")


def s3_client_via_static(creds: Dict[str, Optional[str]]):
    import boto3  # type: ignore
    from botocore.config import Config  # type: ignore

    kwargs: Dict[str, Optional[str]] = {"region_name": creds.get("AWS_REGION")}
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


# -----------------------------
# Path utilities
# -----------------------------

def parse_s3_path(s3_path: str) -> Tuple[str, str]:
    """Return (bucket, key_prefix) from s3://bucket/key... . key_prefix may be empty."""
    if not s3_path.startswith("s3://"):
        raise ValueError(f"Expected s3:// URL, got: {s3_path}")
    parsed = urllib.parse.urlparse(s3_path)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")  # remove leading '/'
    # Ensure trailing slash on prefixes
    if key and not key.endswith("/"):
        key += "/"
    return bucket, key


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


def daterange(start: date, end: date) -> Iterable[date]:
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


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


def success_marker_exists(client, bucket: str, date_prefix: str, verbose: bool = False) -> bool:
    key = date_prefix + "_SUCCESS"
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except Exception as e:
        if verbose:
            sys.stderr.write(f"[DEBUG] HEAD s3://{bucket}/{key} failed: {e}\n")
        return False


# -----------------------------
# CLI & main
# -----------------------------

def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Extract Optimizely Experimentation Events Export from S3\n"
            "- Uses Optimizely Auth API (temporary AWS creds) or static AWS creds.\n"
        )
    )
    p.add_argument("--auth", choices=["optimizely", "aws"], default="optimizely",
                   help="Use 'optimizely' temp creds (default) or static 'aws' creds from env")
    p.add_argument("--pat", default=None, help="Optimizely Personal Access Token (or set OPTIMIZELY_PAT)")
    p.add_argument("--duration", default=DEFAULT_DURATION, help="Optimizely temp credential duration, e.g. 15m..1h")

    # Location controls
    p.add_argument("--bucket", default=None, help="S3 bucket; if omitted with --auth optimizely, taken from s3Path")
    p.add_argument("--prefix", default=None,
                   help=(
                       "Full base prefix including type=.../, e.g. 'v1/account_id=123/type=decisions/'.\n"
                       "If omitted, we construct it from s3Path or --account-id + --type."
                   ))
    p.add_argument("--account-id", default=None,
                   help="If --prefix is not provided, build it as v1/account_id=<ID>/type=<type>/")
    p.add_argument("--type", dest="partition_type", choices=["decisions", "events", "decisions-rerun"],
                   default="decisions", help="Partition type to read (default: decisions)")

    # Date range & output
    p.add_argument("--region", default=DEFAULT_REGION, help="AWS region (Optimizely bucket is us-east-1)")
    p.add_argument("--out-dir", default="downloads", help="Local output directory")
    p.add_argument("--start-date", default=DEFAULT_START_DATE.isoformat(), help="YYYY-MM-DD")
    p.add_argument("--end-date", default=DEFAULT_END_DATE.isoformat(), help="YYYY-MM-DD")

    # Behaviour
    p.add_argument("--dry-run", action="store_true", help="List without downloading")
    p.add_argument("--require-success", dest="require_success", action="store_true", default=True,
                   help="Only process days that have a _SUCCESS marker (default)")
    p.add_argument("--ignore-success", dest="require_success", action="store_false",
                   help="Process days even if _SUCCESS is missing")
    p.add_argument("--verbose", action="store_true", help="Print debug details")

    return p


def validate_prefix_endswith(prefix: str, partition_type: str) -> None:
    acceptable = ("type=decisions/", "type=events/", "type=decisions-rerun/")
    if not any(prefix.endswith(s) for s in acceptable):
        raise ValueError(
            "--prefix must end with one of 'type=decisions/', 'type=events/', or 'type=decisions-rerun/'"
        )
    if partition_type == "events" and not prefix.endswith("type=events/"):
        raise ValueError("--type events requires --prefix to end with 'type=events/'")
    if partition_type == "decisions" and not (prefix.endswith("type=decisions/") or prefix.endswith("type=decisions-rerun/")):
        raise ValueError("--type decisions requires --prefix to end with 'type=decisions/' or 'type=decisions-rerun/'")


def compute_bucket_and_prefix(args, s3_path_hint: Optional[str]) -> Tuple[str, str]:
    # If user supplied a full prefix, prefer it.
    if args.prefix:
        prefix = args.prefix if args.prefix.endswith("/") else args.prefix + "/"
        validate_prefix_endswith(prefix, args.partition_type)
        bucket = args.bucket or DEFAULT_BUCKET
        return bucket, prefix

    # No explicit prefix. Try to derive from Optimizely s3Path hint first.
    base_bucket = None
    base_key = None
    if s3_path_hint:
        try:
            base_bucket, base_key = parse_s3_path(s3_path_hint)
        except Exception:
            pass

    bucket = args.bucket or base_bucket or DEFAULT_BUCKET

    # Determine account base prefix (v1/account_id=.../)
    account_base = None
    if base_key and re.search(r"v1/account_id=\d+/?$", base_key):
        # base_key is exactly 'v1/account_id=123/'
        account_base = base_key
    elif base_key and re.search(r"v1/account_id=\d+/", base_key):
        # Trim to v1/account_id=.../
        m = re.match(r"(v1/account_id=\d+/)", base_key)
        if m:
            account_base = m.group(1)

    if not account_base and args.account_id:
        account_base = f"v1/account_id={args.account_id}/"

    if not account_base:
        raise RuntimeError(
            "Cannot determine account base prefix. Provide --prefix or --account-id, or use --auth optimizely to infer from s3Path."
        )

    # Build full prefix as base + type
    partition = f"type={args.partition_type}/"
    prefix = account_base + partition
    validate_prefix_endswith(prefix, args.partition_type)
    return bucket, prefix


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()

    # Parse dates
    try:
        start = date.fromisoformat(args.start_date)
        end = date.fromisoformat(args.end_date)
    except ValueError:
        sys.exit("ERROR: --start-date and --end-date must be in YYYY-MM-DD format")
    if end < start:
        sys.exit("ERROR: --end-date cannot be earlier than --start-date")

    # Build S3 client
    s3_path_hint: Optional[str] = None
    if args.auth == "optimizely":
        pat = args.pat or os.getenv("OPTIMIZELY_PAT")
        if not pat:
            sys.exit("ERROR: --pat is required for --auth optimizely (or set OPTIMIZELY_PAT)")
        s3, s3_path_hint = s3_client_via_optimizely(
            pat=pat, region_name=args.region, duration=args.duration, verbose=args.verbose
        )
        print("[OK] Using Optimizely temporary AWS credentials (auto-refresh).")
    else:
        static = load_static_creds()
        if not (static["AWS_ACCESS_KEY_ID"] and static["AWS_SECRET_ACCESS_KEY"]):
            sys.exit("ERROR: Missing AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY for --auth aws")
        s3 = s3_client_via_static(static)
        print("[OK] Using static AWS credentials from environment.")

    # Determine bucket/prefix
    try:
        bucket, prefix = compute_bucket_and_prefix(args, s3_path_hint)
    except Exception as e:
        sys.exit(f"ERROR: {e}")

    print("\nS3 extract plan:")
    print(f" Bucket: s3://{bucket}")
    print(f" Base: {prefix}")
    print(f" Type: {args.partition_type}")
    print(f" Dates: {start} → {end}")
    print(f" Out dir: {os.path.abspath(args.out_dir)}")
    print(f" Dry run: {args.dry_run}")
    print(f" Require _SUCCESS: {args.require_success}\n")

    all_objects: List[Dict] = []
    for d in daterange(start, end):
        date_prefix = prefix + f"date={d.isoformat()}/"
        has_success = success_marker_exists(s3, bucket, date_prefix, verbose=args.verbose)
        if not has_success and args.require_success:
            print(f"[INFO] {date_prefix} — no _SUCCESS, skipping")
            continue
        day_objs = [o for o in list_objects(s3, bucket, date_prefix) if o.get("Key", "").endswith(".parquet")]
        if not day_objs:
            print(f"[WARN] {date_prefix} — no parquet files found")
            continue
        print(f"[INFO] {date_prefix} — {len(day_objs)} parquet file(s)")
        all_objects.extend(day_objs)

    if not all_objects:
        print("[WARN] No files found to download in the selected range.")
        sys.exit(0)

    all_objects.sort(key=lambda o: o.get("Key", ""))
    total_bytes = sum(int(o.get("Size", 0)) for o in all_objects)
    print(f"\n[INFO] Total files: {len(all_objects)} (~{human_size(total_bytes)})\n")

    ok = skipped = failed = 0
    for idx, obj in enumerate(all_objects, 1):
        key = obj.get("Key", "")
        size = int(obj.get("Size", 0))
        if key.endswith("/") or not key:
            continue
        local_path = ensure_local_path(args.out_dir, key)
        if os.path.exists(local_path) and os.path.getsize(local_path) == size:
            skipped += 1
            print(f"[{idx}/{len(all_objects)}] SKIP {key} ({human_size(size)})")
            continue
        print(f"[{idx}/{len(all_objects)}] GET {key} -> {local_path} ({human_size(size)})")
        if args.dry_run:
            ok += 1
            continue
        try:
            s3.download_file(bucket, key, local_path)
            ok += 1
        except Exception as e:
            failed += 1
            sys.stderr.write(f"[ERROR] Failed to download {key}: {e}\n")

    print("\nSummary:")
    print(f" Downloaded: {ok}")
    print(f" Skipped:    {skipped}")
    print(f" Failed:     {failed}")
    print(f" Output:     {os.path.abspath(args.out_dir)}")

    if failed > 0:
        sys.exit(2)


if __name__ == "__main__":
    main()
