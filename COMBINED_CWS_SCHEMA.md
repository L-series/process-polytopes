# Combined-CWS Classification Schema

The classifier now accepts two input schemas:

- Legacy single-weight rows with `weight0..weight5`.
- Combined-CWS rows with explicit PALP CWS matrices.

## Combined Input Columns

Required columns:

- `cws_schema_version`: `2`
- `structure_id`: canonical 5D overlap structure ID, `2..47` for combined CWS
- `profile_id`: profile bucket, derived from `structure_id` when omitted by tools
- `source_index`: row provenance within the generator stream
- `nw`: number of weight-system rows, `2..5`
- `N`: ambient homogeneous coordinates, always `nw + 5` for this build
- `degree0..degree4`: row degrees, unused rows zero-filled
- `weight0_0..weight4_9`: embedded CWS matrix, unused rows/columns zero-filled
- `vertex_count`, `facet_count`, `point_count`, `dual_point_count`, `h11`, `h12`, `h13`: optional metadata, zero-filled when absent

## Classifier Output Columns

`unique_polytopes.parquet` keeps the legacy `first_weight0..first_weight5` projection and adds replay columns:

- `schema_version`
- `first_structure_id`
- `first_profile_id`
- `first_nw`
- `first_N`
- `first_source_index`
- `first_degree0..first_degree4`
- `first_weight0_0..first_weight4_9`

These replay columns are used by `add_nf` to recompute normal forms for combined-CWS results exactly.

## Commands

Generate one structure shard:

```bash
scripts/generate_dim5_cws_parquet.sh --structure-id 15 --output-dir data/combined-cws
```

Generate a sharded structure:

```bash
scripts/generate_dim5_cws_parquet.sh --structure-id 12 --output-dir data/combined-cws --shards 8 --worker 1
```

Run the combined-CWS smoke test:

```bash
scripts/test_combined_cws_pipeline.sh
```

Select the classifier backend:

```bash
src/classify/build/classifier --input data/combined-cws --output results/combined-cws --backend cpu
src/classify/build-cuda/classifier --input data/combined-cws --output results/combined-cws-cuda --backend auto
```
