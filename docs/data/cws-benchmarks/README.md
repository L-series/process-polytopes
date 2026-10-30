# Five-dimensional CWS testing corpus

30,000 distinct CWS, split by the number of lattice points in the primal Newton
polytope (PALP's **M** count). All entries have dimension five and the IP property.

| Group  | Input file               | CWS    | Points    | Median points | Reflexive      | IP but nonreflexive |
| ------ | ------------------------ | ------ | --------- | ------------- | -------------- | ------------------- |
| Small  | [small.cws](small.cws)   | 10,000 | 8–30      | 24            | 6,993 (69.93%) | 3,007 (30.07%)      |
| Medium | [medium.cws](medium.cws) | 10,000 | 50–150    | 86            | 5,478 (54.78%) | 4,522 (45.22%)      |
| Large  | [large.cws](large.cws)   | 10,000 | 500–3,846 | 634           | 8,880 (88.80%) | 1,120 (11.20%)      |

Overall: **21,351 reflexive (71.17%)** and **8,649 IP but nonreflexive (28.83%)**.
These percentages describe this selected corpus, not the full 5D census.
The large group includes all five available CWS with at least 3,000 points:
3,017, 3,091, 3,322, 3,423 and 3,846.

## Format

Each `.cws` file has one CWS per line, with no header or point-count annotation.
Each equation is a degree followed by its zero-padded ambient weights; two spaces
separate equations. This is standard PALP CWS input, for example:

```sh
ulimit -s unlimited
PALP/cws-5d.x -i -f < docs/data/cws-benchmarks/small.cws
```

The corresponding `.jsonl` file has one metadata record per input line, in the
same order: CWS ID, primal point count, vertex count, reflexivity flag, historical
descriptor ID, source filename and source line number. The ID is SHA-256 of the
canonical input line without its terminating newline.

Equation and ambient-coordinate permutations are identified using full CWS
matrices before selection. Distinct CWS may still describe equivalent polytopes;
polytope normal-form deduplication is outside this corpus's construction.

## Selection and provenance

Sources are the local archival outputs `cws3-d1-20-cwsx.txt`, `cws11.txt`,
`cws24.txt` and `cws28.txt` under `PALP/cws`, totalling 1,402,481 IP-labelled rows.
Their median point count is 94 and their 99th percentile is 685. This motivated
medium bounds of 50–150 and a large lower bound of 500. The small selection range
was 6–30; the selected source data start at 8 points.

Selection is deterministic by SHA-256 order of exact canonical CWS, retaining
all available 3,000+ point cases. The corpus covers historical descriptors 3,
11, 24 and 28; it is not a uniform sample of the complete descriptor universe.
[manifest.json](manifest.json) records source/output hashes, point histograms,
descriptor counts, selection rules and verification provenance.

PALP freshly re-enumerated all 30,000 point sets and checked their dimension, IP
status, point count, vertex count and reflexivity flag. Every result matched the
archived annotation. This is a PALP consistency check; it is not an independent
formal proof of the point enumeration or geometry algorithms.

The selection/rechecking tools are retained in [tools](tools). To recheck the
saved corpus from the repository root, without the archival source files:

```sh
cc -O3 -DPOLY_Dmax=5 -I PALP docs/data/cws-benchmarks/tools/verify.c PALP/Coord.c PALP/Rat.c PALP/Vertex.c PALP/Polynf.c PALP/LG.c -lm -o /tmp/cws-corpus-verify
python3 docs/data/cws-benchmarks/tools/build.py --verify-only --verifier /tmp/cws-corpus-verify
```

Local verification used GCC 16.2.1 and PALP `b148949`. PALP source and verifier
fingerprints are in the manifest. Regenerating the selection additionally
requires the four original source files, whose hashes are recorded there.
