#!/usr/bin/env python3
"""
Download a range of ws-5d-reflexive parquet files from HuggingFace.
Usage: python download_ws5d.py --start 0 --end 99
"""

import argparse
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# ── Config ────────────────────────────────────────────────────────────────────

REPO_ID    = "calabi-yau-data/ws-5d"
REPO_TYPE  = "dataset"
SUBFOLDER  = "reflexive"
FILE_STEM  = "ws-5d-reflexive"
TOTAL_FILES = 4000
MAX_WORKERS = 8          # parallel file downloads; tune to your connection

# ── Args ──────────────────────────────────────────────────────────────────────

parser = argparse.ArgumentParser(description="Download ws-5d-reflexive parquet files.")
parser.add_argument("--start",    type=int, required=True, help="First file index (inclusive)")
parser.add_argument("--end",      type=int, required=True, help="Last file index (inclusive)")
parser.add_argument("--out-dir",  type=str, default="./ws-5d-reflexive", help="Local output directory")
parser.add_argument("--workers",  type=int, default=MAX_WORKERS, help="Parallel download workers")
args = parser.parse_args()

if not (0 <= args.start <= args.end < TOTAL_FILES):
    sys.exit(f"Error: indices must satisfy 0 <= start <= end < {TOTAL_FILES}")

# ── Environment ───────────────────────────────────────────────────────────────

load_dotenv()  # reads .env in the current directory

token = os.environ.get("HF_TOKEN")
if not token:
    sys.exit("Error: HF_TOKEN not found. Make sure it is set in your .env file.")

os.environ["HF_TOKEN"] = token
os.environ["HF_HUB_ENABLE_HF_TRANSFER"] = "1"  # use fast Rust-backed transfer

# ── Download ──────────────────────────────────────────────────────────────────

try:
    from huggingface_hub import hf_hub_download
    import hf_transfer  # noqa: F401 — imported to confirm it's installed
except ImportError as e:
    sys.exit(
        f"Missing dependency: {e}\n"
        "Run: pip install huggingface_hub hf-transfer python-dotenv"
    )

out_dir = Path(args.out_dir)
out_dir.mkdir(parents=True, exist_ok=True)

filenames = [
    f"{SUBFOLDER}/{FILE_STEM}-{i:04d}.parquet"
    for i in range(args.start, args.end + 1)
]

print(f"Downloading files {args.start:04d} – {args.end:04d}  ({len(filenames)} files)")
print(f"Output directory : {out_dir.resolve()}")
print(f"Workers          : {args.workers}")
print(f"hf-transfer      : enabled\n")

from concurrent.futures import ThreadPoolExecutor, as_completed

def download_one(filename):
    local_path = out_dir / Path(filename).name
    if local_path.exists():
        return filename, "skipped (already exists)"
    hf_hub_download(
        repo_id=REPO_ID,
        repo_type=REPO_TYPE,
        filename=filename,
        local_dir=str(out_dir),
        token=token,
    )
    return filename, "ok"

failed = []
with ThreadPoolExecutor(max_workers=args.workers) as pool:
    futures = {pool.submit(download_one, f): f for f in filenames}
    for i, future in enumerate(as_completed(futures), 1):
        fname = futures[future]
        short = Path(fname).name
        try:
            _, status = future.result()
            print(f"[{i:>{len(str(len(filenames)))}}/{len(filenames)}] {short}  — {status}")
        except Exception as exc:
            print(f"[{i:>{len(str(len(filenames)))}}/{len(filenames)}] {short}  — FAILED: {exc}")
            failed.append(fname)

print()
if failed:
    print(f"⚠  {len(failed)} file(s) failed:")
    for f in failed:
        print(f"   {f}")
    sys.exit(1)
else:
    print(f"✓  All {len(filenames)} files downloaded to {out_dir.resolve()}")
