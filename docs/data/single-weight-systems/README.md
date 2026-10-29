# Single IP weight systems

Each text file contains one primitive positive integer weight system per line,
in PALP output order. Weights within a row are nondecreasing and separated by
one ASCII space. There are no headers, degrees, annotations, or blank lines.
The degree is the sum of the weights. Dimension means Newton polytope dimension,
so a dimension-d row has d+1 weights. These are single IP weight systems, not
combined weight systems or distinct polytope classes.

| File | Weights per row | Systems |
| ---- | --------------- | ------- |
| [2d.txt](2d.txt) | 3 | 3 |
| [3d.txt](3d.txt) | 4 | 95 |
| [4d.txt](4d.txt) | 5 | 184,026 |

## Provenance and checks

Generated from the pinned PALP source at
`968bb4b3c6c42a0893c95340e81718548e13f12a`, without modifying the submodule.
From the repository root, the generation commands were:

```sh
cc -O2 -DPOLY_Dmax=5 PALP/cws.c PALP/Coord.c PALP/Rat.c PALP/Vertex.c PALP/Polynf.c PALP/LG.c -lm -o /tmp/cws-single-weights.x
ulimit -s unlimited
/tmp/cws-single-weights.x -w2
/tmp/cws-single-weights.x -w3
/tmp/cws-single-weights.x -w4
```

Compiler: GCC 16.2.1. Generation used no degree-range restriction and no
transversality filter. For each output row, retain only the d+1 weights after
the leading degree and before any PALP annotation.

Validation checks row counts, widths, positivity, nondecreasing weights,
primitivity (gcd 1), uniqueness, and equality of the removed degree to the sum
of weights. The complete 2D and 3D lists and the existing first-1,000-row 4D
fixture are compared after removing their degree columns.

The counts agree with Kreuzer and Skarke,
[*Reflexive polyhedra, weights and toric Calabi–Yau fibrations*](https://arxiv.org/pdf/math/0001106),
Section 2.2, Lemma 5 (printed page 11): three systems with three weights,
95 with four weights, and 184,026 with five weights. The 2D rows are exactly
`1 1 1`, `1 1 2`, and `1 2 3`.
Skarke's earlier
[*Weight systems for toric Calabi-Yau varieties and reflexivity of Newton polyhedra*](https://arxiv.org/abs/alg-geom/9603007)
also reports the 3D and 4D counts. Count agreement is a cross-check, not a
formal proof of dataset completeness.
