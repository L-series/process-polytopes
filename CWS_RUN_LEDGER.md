# CWS classification run ledger


> **IP check != reflexive (dim 5).** "IP-accepted" / `cws-5d.x -i` means the polytope
> is IP (has an interior lattice point). **Reflexivity is a stronger, separate test**
> requiring a later pass over these IP-accepted CWS. All "accepted" counts here are IP.
Records of GPU classification runs and the CPU overflow processing. All on the
`gpu-run-43types` branch, RTX6000BW (n31/n32), streaming FP pipeline
(`--stream-ip --ip-bucketed --fp-walk --vol-sort --np-cap 256 --emit-capacity 1.5M`).
Terms: **processed/seen** = candidates generated+IP-classified; **accepted** = passed
GPU IP check (IP only, np<=256); **overflow** = np>256, deferred to CPU.
Output dirs: `/home/ahat01/cws43run`, `/home/ahat01/cws12run`.

## Run 1 — 43 least-populated types (all except s3/s12/s13) ✅ COMPLETE

Jobs **66815** (5-GPU array, 40 structures) + **66822** (fix run for s38/s40/s42,
which crashed in 66815 on a stack-overflow bug — see CWS_TYPE_COUNTS / commit 16bb8fb).
Wall: ~79 min (66815, tail-limited by contiguous-shard imbalance) + ~1 min (66822).
**Coverage 100.000%** (every candidate processed).

| s | total candidates | processed | accepted | overflow |
|---:|---:|---:|---:|---:|
| 27 | 28,358,376,718 | 28,358,376,718 | 7,438,173 | 12,812,223 |
| 26 | 28,358,376,718 | 28,358,376,718 | 7,438,173 | 12,812,223 |
| 25 | 28,358,376,718 | 28,358,376,718 | 7,438,173 | 12,812,223 |
| 29 | 22,300,333,662 | 22,300,333,662 | 5,952,504 | 195,263 |
| 20 | 6,535,293,765 | 6,535,293,765 | 3,803,436 | 2,305,498 |
| 15 | 1,961,068,830 | 1,961,068,830 | 1,513,191 | 5,474,004 |
| 6 | 1,892,667,282 | 1,892,667,282 | 39,573,682 | 19,470,550 |
| 43 | 384,429,045 | 384,429,045 | 178,052 | 356,717 |
| 35 | 115,356,990 | 115,356,990 | 57,912 | 769,089 |
| 40 | 109,836,870 | 109,836,870 | 77,063 | 60,649 |
| 14 | 92,984,126 | 92,984,126 | 267,126 | 38 |
| 11 | 92,388,560 | 92,388,560 | 290,792 | 402,826 |
| 21 | 54,918,435 | 54,918,435 | 71,901 | 60,436 |
| 36 | 32,959,140 | 32,959,140 | 25,182 | 157,674 |
| 38 | 18,306,145 | 18,306,145 | 24,039 | 21,424 |
| 4 | 5,506,794 | 5,506,794 | 1,653,748 | 300,285 |
| 24 | 3,207,408 | 3,207,408 | 94,063 | 29,315 |
| 28 | 925,344 | 925,344 | 68,428 | 39 |
| 10 | 267,379 | 267,379 | 15,118 | 1,899 |
| 2 | 184,026 | 184,026 | 164,783 | 19,243 |
| 7 | 63,903 | 63,903 | 26,913 | 1,925 |
| 45 | 54,432 | 54,432 | 3,753 | 4 |
| 8 | 49,970 | 49,970 | 2,585 | 170 |
| 32 | 37,188 | 37,188 | 9,942 | 586 |
| 31 | 37,188 | 37,188 | 9,942 | 586 |
| 30 | 37,188 | 37,188 | 9,942 | 586 |
| 42 | 15,552 | 15,552 | 1,504 | 0 |
| 34 | 11,046 | 11,046 | 451 | 42 |
| 22 | 7,497 | 7,497 | 2,353 | 579 |
| 18 | 2,142 | 2,142 | 1,020 | 127 |
| 17 | 2,142 | 2,142 | 1,020 | 127 |
| 16 | 2,142 | 2,142 | 1,020 | 127 |
| 33 | 1,578 | 1,578 | 73 | 14 |
| 46 | 526 | 526 | 29 | 0 |
| 5 | 285 | 285 | 257 | 28 |
| 44 | 126 | 126 | 42 | 32 |
| 9 | 95 | 95 | 85 | 10 |
| 23 | 63 | 63 | 44 | 7 |
| 41 | 56 | 56 | 27 | 10 |
| 39 | 21 | 21 | 14 | 3 |
| 19 | 6 | 6 | 4 | 2 |
| 37 | 3 | 3 | 2 | 1 |
| 47 | 1 | 1 | 1 | 0 |
| **TOTAL (43)** | **118,676,087,105** | **118,676,087,105** | **76,216,562** | **68,066,584** |

- accept rate **0.0642%**, overflow rate **0.0574%** (of processed).
- accepted is pre-deduplication (passes IP; not yet reduced to distinct polytopes).

## Run 2 — CPU overflow IP-check ✅ COMPLETE

Job **66824** (std node n11, 128 cores, `split` + `xargs -P128`). Pipes all overflow
CWS rows through PALP `cws-5d.x -i -f` (full uncapped point enumeration + IP check;
the GPU overflow format is natively PALP-readable — no conversion). Recovers the
IP CWS the GPU deferred -> completes Run 1. Wall: **1244 s (~21 min)**.

- overflow rows fed: **68,066,584** (includes s38/s40/s42 fix overflow: 82,073).
- IP-accepted (passed -i) recovered: **3,871,042** (**5.687%** of overflow — far above the
  0.064% GPU accept rate, since np>256 polytopes are much more often IP).
- result file: `/home/ahat01/cws43run/overflow_ip_accepted.txt` (230 MB).

### Run 1 + Run 2 — the 43 minor types, FINAL
- candidates processed: **118,676,087,105** (100%)
- **total IP-accepted: 80,087,604** = 76,216,562 (GPU) + 3,871,042 (CPU overflow)
- overall IP-pass rate: **0.0675%** of processed (pre-dedup).

## Run 3 — s12 classification ✅ COMPLETE (GPU + overflow)

Job **66825** (5-GPU array, interleaved sharding: 40 shards, GPU g does g,g+5,…,g+35).
Output `/home/ahat01/cws12run/`. Wall **~7.75 h** (tasks 26307–27905 s, ~6% spread —
interleaving fixed the load imbalance). **Coverage 100.000%.**
- processed: **987,911,532,890**
- accepted (GPU IP, np≤256): **14,201,801** (0.0014% — far below the minor types,
  confirming bigger structure → lower IP rate)
- overflow (np>256): **14,121,722**; CPU IP pass (job 66875, ~24 min):
  **30,214 IP-accepted** (only **0.21%** of overflow — vs 5.69% for the minor types;
  overflow-IP fraction is strongly structure-dependent).
- **s12 final IP-accepted = 14,232,015** (14,201,801 GPU + 30,214 overflow).
- file: `/home/ahat01/cws12run/overflow_ip_accepted.txt`.

## Cumulative so far (45 of 46 structures: 43 minor + s12) — FINAL
- candidates processed: **119,663,998,637** (118.68B minor + 0.988T s12)
- **total IP-accepted: 94,319,619** = 80,087,604 (minor) + 14,232,015 (s12)

## s3 sample — IP-rate measurement (job 66880, 1.008B candidates, 5 spread shards)
Decision input before the full s3 run. s3 is nw=2 (size-5⊕size-5), the same family
as s6, so its rate sits between the nw=3 giants and little s6.
- GPU IP-accept rate **0.0126%** → projected s3 GPU-accept **~1.3B**
- overflow rate **0.163%** → projected s3 overflow **~16.4B (np>256)** ← the big one
- est. total s3 IP ≈ **1.3–2.2B** (GPU + overflow-IP at 0.2–5.7%; fraction uncertain)
- **storage/CPU planning:** at np_cap 256, s3 ≈ ~52 GB accepted + **~656 GB overflow**,
  and a 16.4B-row CPU overflow pass (~1–3 d). **Raise np_cap (512–1024) for s3** to
  shift overflow onto the (cheap) GPU before the production run.

## Not yet run
- **s13** (987,911,532,890) — twin of s12, ~7.75 h on 5 GPUs (expect ~same as s12).
- **s3** (10,046,036,135,619, 82.7% of all work) — ~3.2 d on 5 GPUs / ~1.4 d full fleet;
  dominates total IP (~95%). Decide np_cap from the sample above first.
