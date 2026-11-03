# CUDA front end

`make gpu` builds `build/cws-gpu.x` (set `CUDA_ARCH`, default `sm_120`, and
`NVCC`). It runs one historical structure, or one contiguous shard of it:

```sh
build/cws-gpu.x --structure-id 25 --np-cap 1336 \
  --accepted-output s25.cws --deferred-output s25.deferred
build/cws-gpu.x --structure-id 25 --count-only   # candidates only
build/cws-gpu.x --structure-id 25 --census       # exact largest point counts
```

Descriptors come from `src/cws/catalogue.c` and pools from the same
`--weights-dir` as `build/cws.x`, so both generate the same candidates.

| File | Stage |
| --- | --- |
| `generate.cuh` | Selection tuples, shared-prefix permutations, slot-order and shared-prefix pruning (as PALP `CWSGenerate`). |
| `join.cuh` | Per-pool tables of single-row points; exact point count and coordinate/pair certificates per candidate. |
| `lattice.cuh` | Integer kernel basis and LLL + Fincke-Pohst lattice-point walk. |
| `ip_check.cuh` | Port of PALP `IP_Check` with 64-entry lists. |
| `pipeline.cuh` | Chunks, join, survivor slices, outputs and accounting. |
| `join_type3.cuh` | Optional (`make gpu-type3`, `build/cws-gpu-type3.x`): structure 3 generated and joined in one kernel; about 2x faster on structure 3. |

Every candidate ends in exactly one outcome and each chunk checks that they
add up. **Accepted** rows are IP according to the GPU. **Deferred** rows have
no GPU verdict: more than `--np-cap` points, walk limits, a degenerate FP
solve or full IP-check lists. They must be decided on the CPU, for example
with PALP `cws-5d.x -i -f`. Both outputs are PALP CWS rows: per weight row the
degree, then the weights of all coordinates.

Rejections by rank deficiency and initial incidence failure carry no
certificate; they are counted separately (`rank_deficient`, `inci_fail`).
