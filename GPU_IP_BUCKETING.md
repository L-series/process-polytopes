# GPU IP bucketing: split point-enumeration / IP-check pipeline

`cuda_dim5_cws_scan --ip-bucketed` replaces the single fused IP kernel with two
smaller kernels and a compact, work-proportional point buffer. It targets the
three independent utilisation problems identified in `PIPELINE_PROFILING.md`:

| problem (fused kernel)                              | fix in `--ip-bucketed`                         |
|-----------------------------------------------------|------------------------------------------------|
| 80 MB/slot point buffer ⇒ VRAM caps grid to ~6 SMs  | compact `np_cap`-sized per-candidate buffer    |
| 153 registers/thread ⇒ ~37 % occupancy ceiling      | split kernels: point-enum drops to **78 regs** |
| heavy-np tail ⇒ warp divergence in the point walk   | bound the walk to `np_cap`; np-sort the IP pass |

Completeness is preserved: candidates whose point count exceeds `np_cap` are not
dropped — they are written to `--overflow-output` for the CPU (PALP) to finish.

## How it works

```
candidates ──► point_enum_kernel ──► np_out[]  (per-candidate point count or sentinel)
              (1 thread/candidate,    points[] (compact: candidate i at i*np_cap*5)
               grid-stride, full grid) overflow[] (np>np_cap → CPU)
                      │
                      ▼  D2H np_out
            host: classify + counting-sort valid (6..np_cap) candidates by np
                      │  H2D valid_index (np-ascending)
                      ▼
            ip_check_bucketed_kernel ──► accepted[]  (IP polytopes)
            (1 thread/candidate over the sorted index list; per-thread scratch)
```

* **Compact buffer decouples storage from the grid.** The point buffer is
  `candidate_count * np_cap * 5` longs — proportional to *work*, not to the
  launch grid — so the grid covers every SM (no VRAM cap). At `np_cap=64` the
  buffer is 2.5 KB/candidate instead of 80 MB/slot.
* **Bounding the walk to `np_cap` removes most tail divergence for free.** ~98 %
  of type-3 CWS have ≤64 points; the rare heavy-np candidates bail after
  `np_cap` appends instead of enumerating thousands of points in one lane.
* **Splitting cuts register pressure.** The point-enumeration kernel carries no
  IP scratch, so it compiles to 78 registers (vs 153 fused) → roughly double the
  achievable occupancy for the kernel that is ~99 % of the runtime.
* **np-sorting the IP pass** groups equal-`np` candidates so a warp's 32 lanes
  run near-identical IP loops.

## Correctness & completeness (verified on n31, structure 3)

`scripts/validate_ip_bucketed_correctness.sh` runs on a shard small enough to fit
without `--emit-capacity` truncation (the non-stream `--ip-check` path stores
candidates in nondeterministic atomic-arrival order, so a fair comparison needs
the whole generated set; the real complete run uses `--stream-ip` chunking which
never truncates). On 25,152 candidates:

* legacy serial (`--ip-max-points 4096`) — **deterministic** run-to-run.
* bucketed (`--np-cap 4096`, no overflow) — **deterministic** run-to-run.
* **accepted CWS sets byte-identical**: 258 == 258 (`diff` clean).
* completeness at `--np-cap 64`: `point_overflow == overflow rows written`
  (2955 == 2955); every accept@64 ⊆ accept@4096; and
  `accept@64 ∪ overflow@64` covers 100 % of accept@4096 (0 missed). I.e. the GPU
  accepts plus the CPU-handoff overflow together reproduce the full IP set.

(The earlier apparent "nondeterminism" was entirely the truncation artifact of
comparing different atomic-arrival subsets, not a kernel bug.)

## Throughput (n31, RTX PRO 6000 Blackwell, structure 3, shard 2000/4000)

300,000 candidates, no `--ip-stage-profile`. Overflow % is the share shipped to
the CPU. (`scripts/benchmark_ip_bucketed.sh`.)

| config                         | cand/s   | vs block | overflow→CPU |
|--------------------------------|----------|----------|--------------|
| legacy serial (ip-max 4096)    |   29,339 | 0.27×    | 0 %          |
| legacy block  (ip-max 4096)    |  106,866 | 1.00×    | 0 %          |
| **bucketed np_cap 32**         | **146,008** | **1.37×** | 11.3 %    |
| bucketed np_cap 48             |  127,399 | 1.19×    | 6.5 %        |
| bucketed np_cap 64             | ~100,000 | ~0.95×   | 4.4 %        |
| bucketed np_cap 96             |   79,436 | 0.74×    | 2.0 %        |
| bucketed np_cap 128            |   66,712 | 0.62×    | 1.1 %        |

Secondary sweeps at np_cap 64: `threads` 64/128/256 → 111k/105k/101k (64 best,
marginal); `blocks` 8/16/32/64×SM → 107k/100k/105k/101k — **flat**, confirming
the grid already saturates every SM and the ceiling is per-SM occupancy
(register- and local-memory-bound point walk), not SM coverage.

Single-GPU best (~146k/s ≈ 17 CPU cores) still trails one 128-core CPU node
(~1.1 M/s): the lattice point walk is branchy, integer-division-heavy and spills
~4 KB/thread to local memory, so it stays GPU-hostile even with the walk bounded.
The split kernels + compact buffer fix the buffer/grid scaling and registers; the
remaining lever is a block-cooperative point-enum (one block per candidate, as in
the legacy block kernel which is 3.7× the serial one at np_cap 4096) — left as
future work.

`np_cap` is a **GPU-throughput vs CPU-offload** knob: a smaller cap runs the GPU
faster but ships more np>cap candidates to the CPU. `meanSM` is nvidia-smi
"GPU busy" (kernel resident), not achieved occupancy — `ncu` is admin-gated
(`ERR_NVGPUCTRPERM`) on this cluster, so warp-occupancy can't be measured here.

## Usage

```
cuda_dim5_cws_scan --structure-id 3 --w5 results/cache/w5.ip \
    --shard-count N --shard-index K --emit-capacity M \
    --ip-check --ip-bucketed --np-cap 64 \
    --accepted-output accepted.cws --overflow-output overflow.cws
```

Then run PALP (`cws-5d.x`) over `overflow.cws` on the CPU to finish the np>cap
candidates. `--ip-bucketed` is incompatible with `--stream-ip` / `--block-ip`
(it manages its own split kernels); if any candidate exceeds `--np-cap` and no
`--overflow-output` is given, it errors rather than silently drop them.
