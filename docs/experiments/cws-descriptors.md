# CPU descriptor baseline

Measured locally on 2026-10-28, AMD Ryzen 9 7950X3D, one CPU process, no GPU.
Source base: PALP `968bb4b`; descriptor work adapted from `c4a565f`, with the
compact catalogue and conservative slot transpositions described in the
[descriptor guide](../CWS_DESCRIPTORS.md). GCC 16.2.1, GNUmakefile default
`-O3 -g -W -Wall`, dimension capacity 5. No CPU affinity or frequency policy was
set; these are workstation baseline measurements, not an isolated machine study.

The boundary includes pool loading, selections, generation, PALP point/IP/dual
work, and complete CWS output to a temporary file. It excludes normal forms.
Timings are startup-inclusive host wall time, with one warmup and seven measured
runs per descriptor. Both types use complete committed pools. The candidate
numerator counts assignments submitted to geometry after symmetry pruning.
Fixed-range elapsed time is the primary comparison when future pruning changes
that numerator. This record establishes a baseline and claims no speedup over
archival CPU/GPU stages with different work boundaries.

| Type | Candidates | IP-accepted rows | Median seconds | Candidate CWS/s |
| ---- | ---------- | ---------------- | -------------- | --------------- |
| 5    | 285        | 285              | 0.014512       | 19,639          |
| 7    | 63,903     | 28,221           | 3.129978       | 20,416          |

[Raw measurements](cws-descriptors-results.json) retain all seven wall times,
input/source/binary SHA-256 hashes, output hashes, CPU and platform information.
All repetitions produced identical output hashes and candidate/accept counts.
The absolute binary path records the isolated staging build used for measurement.

Reproduce from the repository root:

```sh
make
python3 scripts/benchmark-cws-descriptors.py
```

Validation for this source snapshot:

- Six descriptor suites pass: exact catalogue fields, all 46 types versus the
  unpruned rational oracle, a larger 5,994-class type-12 case, malformed inputs,
  direct slot-symmetry checks, and full type-5 product equivalence with the legacy path.
- All 20 existing CWS/NF regressions pass; no golden files were changed.
- Removing either pruning fix deliberately causes 11 bounded descriptor cases
  to fail, confirming that the tests detect the historical omissions.
  These are historical benchmark and bounded regression results, not an end-to-end
  implementation proof. Historical catalogue
  isomorphisms and interpretation gaps remain as documented in the theory audit.
