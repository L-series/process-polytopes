#!/usr/bin/env python3
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
from pathlib import Path
import subprocess
import time

OUT = Path(__file__).resolve().parent


def one(ident):
    start = time.perf_counter()
    try:
        p = subprocess.run([str(OUT / "lattices"), str(OUT / f"{ident}.points.bin")],
                           capture_output=True, text=True, timeout=30)
    except subprocess.TimeoutExpired:
        return {"id": ident, "status": "timeout", "timeout_seconds": 30}
    result = {"id": ident, "process_seconds": time.perf_counter() - start}
    if p.returncode or p.stderr:
        result.update(status="error", returncode=p.returncode,
                      stdout=p.stdout[-2000:], stderr=p.stderr)
    else:
        result.update(status="complete", **json.loads(p.stdout))
        if len(result["states"]) != 152:
            raise ValueError("Unexpected prime-index lattice count")
    return result


def main():
    cases = [json.loads(line)["id"] for line in (OUT / "native.jsonl").read_text().splitlines()
             if json.loads(line)["status"] == "complete"]
    start = time.perf_counter()
    with (OUT / "lattices.jsonl").open("w") as stream, ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(one, ident) for ident in cases]
        for i, future in enumerate(as_completed(futures), 1):
            result = future.result()
            stream.write(json.dumps(result) + "\n")
            stream.flush()
            if result["status"] != "complete" or i % 20 == 0:
                print("PROGRESS", i, len(cases), result["id"], result["status"], flush=True)
    print("FINISHED", len(cases), "wall_seconds", time.perf_counter() - start, flush=True)


if __name__ == "__main__":
    main()
