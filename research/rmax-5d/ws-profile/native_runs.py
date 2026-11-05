#!/usr/bin/env python3
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import time

OUT = Path(__file__).resolve().parent


def one(sample):
    ident = sample["id"]
    text = " ".join(map(str, [
        sample["point_count"], sample["vertex_count"], sample["facet_count"],
        int(sample["kind"] == "reflexive"), sample.get("dual_point_count", -1),
        *sample["weights"],
    ])) + "\n"
    command = [str(OUT / "native"), str(OUT / f"{ident}.points.bin"),
               str(OUT / f"{ident}.dual.bin"), "4"]
    t = time.perf_counter()
    try:
        p = subprocess.run(command, input=text, text=True, capture_output=True, timeout=30)
    except subprocess.TimeoutExpired:
        return {"id": ident, "status": "timeout", "timeout_seconds": 30,
                "process_seconds": time.perf_counter() - t}
    result = {"id": ident, "process_seconds": time.perf_counter() - t,
              "returncode": p.returncode}
    if p.returncode or p.stderr:
        result.update(status="error", stderr=p.stderr, stdout=p.stdout[-2000:])
    else:
        result.update(status="complete", **json.loads(p.stdout))
    return result


def main():
    samples = json.loads((OUT / "sample.json").read_text())["records"]
    provenance = {
        "platform": platform.platform(), "cpus": os.cpu_count(),
        "workers": 2, "native_timeout_seconds": 30, "repeats": 4,
        "note": "Exclude first native repetition from steady-state summaries; "
                "process time includes all four repetitions and file dumps.",
        "hashes": {name: hashlib.sha256((OUT / name).read_bytes()).hexdigest()
                   for name in ["native.c", "native", "sample.py"]},
        "palp_commit": subprocess.check_output(
            ["git", "-C", "/home/ahat01/repos/process-polytopes/PALP",
             "rev-parse", "HEAD"], text=True).strip(),
    }
    (OUT / "provenance.json").write_text(json.dumps(provenance, indent=2) + "\n")
    start = time.perf_counter()
    with (OUT / "native.jsonl").open("w") as stream, ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(one, sample) for sample in samples]
        for i, future in enumerate(as_completed(futures), 1):
            result = future.result()
            stream.write(json.dumps(result) + "\n")
            stream.flush()
            if result["status"] != "complete" or i % 20 == 0:
                print("PROGRESS", i, len(samples), result["id"], result["status"], flush=True)
    print("FINISHED", len(samples), "wall_seconds", time.perf_counter() - start, flush=True)


if __name__ == "__main__":
    main()
