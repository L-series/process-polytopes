# Project sources

Root `make` builds `build/cws.x`. The descriptor command is:

```sh
ulimit -s unlimited
build/cws.x -n5 -s5
```

| Directory     | Purpose                                                            |
| ------------- | ------------------------------------------------------------------ |
| `cws/`        | Historical catalogue, project weight-file loading and CLI adapter. |
| `gpu/`        | CUDA generation, join, lattice points and IP check (`make gpu`).   |
| `benchmarks/` | Geometry-corpus harness and function-clock runtime.                |
| `reporting/`  | Performance dashboard template.                                    |

Scripts remain in `scripts/`, regression tests in `tests/`, and data/results in
`docs/`. Generated objects are excluded from Git.

PALP implements descriptor validation and enumeration directly in `cws.c`.
`cws_generate.h` declares the public descriptor and callback interface. The
project selects a historical descriptor and weight pools, then calls
`CWSGenerate`; its callback `EmitGeometry` invokes PALP's `PRINT_CWS`.

`CWSGenerate` handles prefix selections, permutations, symmetry pruning and row
assembly before calling the supplied callback. The descriptor is supplied by
the caller; neither the historical catalogue nor structural validation proves
type completeness or geometric realizability. The callback must copy a candidate
before retaining it. PALP still uses global input/output state, so execution is
serial within a process.

The project adapter delegates other commands to `CWSMain`. Standalone
`PALP/cws.x` retains PALP's legacy CLI; project `-n5 -sID` is provided by
`build/cws.x`. Compile PALP with `PALP_CWS_NO_MAIN` when linking the project front
end. The CUDA front end is described in [gpu/README.md](gpu/README.md); worker
scheduling and checkpointing are not implemented here.
