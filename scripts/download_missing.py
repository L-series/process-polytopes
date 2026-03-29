#!/usr/bin/env python3
"""Download any missing parquet files from the 0-499 range."""
import os, sys
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from huggingface_hub import hf_hub_download

REFLEXIVE_DIR = Path(__file__).resolve().parent.parent / "samples" / "reflexive"
SAMPLES_DIR = REFLEXIVE_DIR.parent

missing = []
for i in range(500):
    f = REFLEXIVE_DIR / f"ws-5d-reflexive-{i:04d}.parquet"
    if not f.exists():
        missing.append(i)

print(f"{len(missing)} files missing")
if not missing:
    print("All 500 files present!")
    sys.exit(0)

print(f"Missing indices: {missing}")

def dl(idx):
    fname = f"reflexive/ws-5d-reflexive-{idx:04d}.parquet"
    hf_hub_download(
        repo_id="calabi-yau-data/ws-5d",
        repo_type="dataset",
        filename=fname,
        local_dir=str(SAMPLES_DIR),
    )
    return idx

failed = []
with ThreadPoolExecutor(max_workers=8) as pool:
    futs = {pool.submit(dl, i): i for i in missing}
    for fut in as_completed(futs):
        idx = futs[fut]
        try:
            fut.result()
            print(f"  {idx:04d} ok")
        except Exception as e:
            print(f"  {idx:04d} FAILED: {e}")
            failed.append(idx)

if failed:
    print(f"\n{len(failed)} files failed: {failed}")
    sys.exit(1)
else:
    print(f"\nAll {len(missing)} missing files downloaded successfully!")
