#!/usr/bin/env python3
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import os
from pathlib import Path
import re
import time

import native_cover

OUT = Path(__file__).resolve().parent


def main():
    os.environ["WS_COVER_SECONDS"] = "2"
    os.environ["WS_COVER_NODES"] = "1000"
    samples = json.loads((OUT / "sample.json").read_text())["records"]
    cases = [r["id"] for r in samples if r["kind"] == "ip" and
             any(re.fullmatch(r"random-\d+", label) for label in r["labels"])]
    start = time.perf_counter()
    with (OUT / "random-cover.jsonl").open("w") as stream, ThreadPoolExecutor(max_workers=2) as pool:
        for f in as_completed([pool.submit(native_cover.one, (i, "partition")) for i in cases]):
            result = f.result()
            stream.write(json.dumps(result) + "\n")
            stream.flush()
    print("FINISHED", len(cases), "uniform IP roots", time.perf_counter() - start, "seconds")


if __name__ == "__main__":
    main()
