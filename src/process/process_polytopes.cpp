/**
 * process_polytopes.cpp — Reprocess unique_polytopes.parquet with CLEAN PALP.
 *
 * For each row (one candidate weight system per unique 5D reflexive polytope)
 * recomputes, with clean PALP v2.21:
 *   - the canonical normal-form vertex matrix      -> column `nf` (list<int32>)
 *   - overflow-safe Hodge numbers h11,h12,h13,h22  -> int32 columns
 *   - the Euler number chi                         -> int64 column
 *   - the M/N point & vertex counts from BaHo       -> bh_* int32 columns
 *
 * All original columns are passed through unchanged.  Output is one parquet
 * file (the SLURM shard) covering an input row-group range [rg-start, rg-end).
 * One output row group is written per input row group, so peak memory is one
 * row-group table plus the reused PALP workspace.
 *
 * Any per-row failure (non-IP / non-reflexive) is a hard error: the dataset is
 * known reflexive and unique, so a failure means a bug or an exceeded PALP
 * limit and must surface, not be skipped.
 *
 * Usage:
 *   process_polytopes --input F.parquet --output OUT.parquet \
 *                     --rg-start S --rg-end E [--computed-only] [--limit N]
 *   process_polytopes --input F.parquet --verify --rg-start 0 --rg-end 1 \
 *                     [--limit N]      # cross-check vs original columns, no write
 */
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/file_reader.h>

#define XXH_INLINE_ALL
#include "../classify/xxhash.h"

#include "palp_hodge.h"

#define THROW_NOT_OK(expr)                                                  \
    do {                                                                    \
        auto _s = (expr);                                                   \
        if (!_s.ok())                                                       \
            throw std::runtime_error(std::string(__FILE__) + ":" +         \
                std::to_string(__LINE__) + " " + _s.ToString());           \
    } while (0)

#define ASSIGN_OR_THROW(lhs, expr)                                          \
    do {                                                                    \
        auto _r = (expr);                                                   \
        if (!_r.ok())                                                       \
            throw std::runtime_error(std::string(__FILE__) + ":" +         \
                std::to_string(__LINE__) + " " + _r.status().ToString());  \
        lhs = std::move(_r).ValueOrDie();                                   \
    } while (0)

namespace {

struct Args {
    std::string input;
    std::string output;
    int  rg_start = -1;
    int  rg_end   = -1;     /* exclusive */
    int64_t limit = -1;     /* cap rows processed per row group (pilot)      */
    bool verify   = false;
    bool computed_only = false;
};

Args parse_args(int argc, char **argv) {
    Args a;
    for (int i = 1; i < argc; i++) {
        std::string k = argv[i];
        auto next = [&]() -> std::string {
            if (i + 1 >= argc) { std::cerr << "missing value for " << k << "\n"; exit(1); }
            return argv[++i];
        };
        if      (k == "--input")    a.input = next();
        else if (k == "--output")   a.output = next();
        else if (k == "--rg-start") a.rg_start = std::stoi(next());
        else if (k == "--rg-end")   a.rg_end = std::stoi(next());
        else if (k == "--limit")    a.limit = std::stoll(next());
        else if (k == "--verify")   a.verify = true;
        else if (k == "--computed-only") a.computed_only = true;
        else { std::cerr << "unknown arg: " << k << "\n"; exit(1); }
    }
    if (a.input.empty() || a.rg_start < 0 || a.rg_end < 0) {
        std::cerr << "usage: --input F --rg-start S --rg-end E "
                     "[--output OUT [--computed-only] | --verify] [--limit N]\n";
        exit(1);
    }
    if (!a.verify && a.output.empty()) {
        std::cerr << "either --output or --verify is required\n";
        exit(1);
    }
    return a;
}

int schema_column_index(const std::shared_ptr<arrow::Schema> &schema,
                        const std::string &name) {
    int idx = schema->GetFieldIndex(name);
    if (idx < 0) throw std::runtime_error("missing column: " + name);
    return idx;
}

/* Fetch a single-chunk column from a per-row-group table by name. */
std::shared_ptr<arrow::Array> col(const std::shared_ptr<arrow::Table> &t,
                                  const std::string &name) {
    auto c = t->GetColumnByName(name);
    if (!c) throw std::runtime_error("missing column: " + name);
    if (c->num_chunks() != 1)
        throw std::runtime_error("expected single chunk for " + name);
    return c->chunk(0);
}

/* xxHash128 of the NF, identical layout to classifier.cpp:hash_normal_form. */
void hash_nf(const Long nf[POLY_Dmax][VERT_Nmax], int dim, int nv,
             uint64_t *lo, uint64_t *hi) {
    Long buf[POLY_Dmax * VERT_Nmax];
    int k = 0;
    for (int i = 0; i < dim; i++)
        for (int j = 0; j < nv; j++)
            buf[k++] = nf[i][j];
    XXH128_hash_t h = XXH3_128bits(buf, (size_t)k * sizeof(Long));
    *lo = h.low64; *hi = h.high64;
}

[[noreturn]] void die_row(int rg, int64_t row, const int w[6]) {
    std::cerr << "FATAL: PALP failed (non-IP/non-reflexive) at row_group=" << rg
              << " row=" << row << " weights="
              << w[0] << "," << w[1] << "," << w[2] << ","
              << w[3] << "," << w[4] << "," << w[5] << "\n";
    std::exit(2);
}

} // namespace

int main(int argc, char **argv) try {
    Args args = parse_args(argc, argv);
    palp_hodge_init();
    ProcWorkspace *ws = proc_workspace_alloc();
    if (!ws) { std::cerr << "workspace alloc failed\n"; return 1; }

    arrow::MemoryPool *pool = arrow::default_memory_pool();

    std::shared_ptr<arrow::io::ReadableFile> infile;
    ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(args.input, pool));
    std::unique_ptr<parquet::arrow::FileReader> reader;
    THROW_NOT_OK(parquet::arrow::OpenFile(infile, pool, &reader));

    std::shared_ptr<arrow::Schema> input_schema;
    THROW_NOT_OK(reader->GetSchema(&input_schema));
    const std::vector<int> weight_cols = {
        schema_column_index(input_schema, "first_weight0"),
        schema_column_index(input_schema, "first_weight1"),
        schema_column_index(input_schema, "first_weight2"),
        schema_column_index(input_schema, "first_weight3"),
        schema_column_index(input_schema, "first_weight4"),
        schema_column_index(input_schema, "first_weight5"),
    };

    const int nrg = reader->num_row_groups();
    if (args.rg_end > nrg) args.rg_end = nrg;
    if (args.rg_start >= args.rg_end) {
        std::cerr << "empty row-group range [" << args.rg_start << ","
                  << args.rg_end << ")\n";
        return 1;
    }

    /* Output writer (lazy: opened once we know the augmented schema). */
    std::shared_ptr<arrow::io::FileOutputStream> out;
    std::unique_ptr<parquet::arrow::FileWriter> writer;

    /* Verify-mode counters. */
    int64_t v_rows = 0, v_count_mismatch = 0, v_h_overflow = 0,
            v_h_mismatch_inrange = 0, v_hash_mismatch = 0;

    int64_t total_rows = 0;
    for (int rg = args.rg_start; rg < args.rg_end; rg++) {
        std::shared_ptr<arrow::Table> table;
        if (args.computed_only && !args.verify)
            THROW_NOT_OK(reader->ReadRowGroup(rg, weight_cols, &table));
        else
            THROW_NOT_OK(reader->ReadRowGroup(rg, &table));
        const int64_t n = table->num_rows();
        const int64_t nproc =
            (args.limit >= 0 && args.limit < n) ? args.limit : n;

        auto w0 = std::static_pointer_cast<arrow::Int32Array>(col(table, "first_weight0"));
        auto w1 = std::static_pointer_cast<arrow::Int32Array>(col(table, "first_weight1"));
        auto w2 = std::static_pointer_cast<arrow::Int32Array>(col(table, "first_weight2"));
        auto w3 = std::static_pointer_cast<arrow::Int32Array>(col(table, "first_weight3"));
        auto w4 = std::static_pointer_cast<arrow::Int32Array>(col(table, "first_weight4"));
        auto w5 = std::static_pointer_cast<arrow::Int32Array>(col(table, "first_weight5"));

        /* Original columns used only for verify cross-checks. */
        std::shared_ptr<arrow::Int16Array> o_vc, o_fc, o_h11, o_h12, o_h13;
        std::shared_ptr<arrow::Int32Array> o_pc, o_dpc;
        std::shared_ptr<arrow::UInt64Array> o_hlo, o_hhi;
        if (args.verify) {
            o_vc  = std::static_pointer_cast<arrow::Int16Array>(col(table, "vertex_count"));
            o_fc  = std::static_pointer_cast<arrow::Int16Array>(col(table, "facet_count"));
            o_pc  = std::static_pointer_cast<arrow::Int32Array>(col(table, "point_count"));
            o_dpc = std::static_pointer_cast<arrow::Int32Array>(col(table, "dual_point_count"));
            o_h11 = std::static_pointer_cast<arrow::Int16Array>(col(table, "h11"));
            o_h12 = std::static_pointer_cast<arrow::Int16Array>(col(table, "h12"));
            o_h13 = std::static_pointer_cast<arrow::Int16Array>(col(table, "h13"));
            o_hlo = std::static_pointer_cast<arrow::UInt64Array>(col(table, "hash_lo"));
            o_hhi = std::static_pointer_cast<arrow::UInt64Array>(col(table, "hash_hi"));
        }

        /* Builders for the new columns. */
        auto nf_builder = std::make_shared<arrow::ListBuilder>(
            pool, std::make_shared<arrow::Int32Builder>(pool));
        auto *nf_vals = static_cast<arrow::Int32Builder *>(nf_builder->value_builder());
        arrow::Int16Builder nf_nv_b;
        arrow::Int32Builder source_rg_b, source_row_b, h11_b, h12_b, h13_b, h22_b;
        arrow::Int64Builder chi_b;
        arrow::Int32Builder mp_b, mv_b, np_b, nv_b;
        if (!args.verify) {
            if (args.computed_only) {
                THROW_NOT_OK(source_rg_b.Reserve(nproc));
                THROW_NOT_OK(source_row_b.Reserve(nproc));
            }
            THROW_NOT_OK(nf_nv_b.Reserve(nproc));
            THROW_NOT_OK(h11_b.Reserve(nproc)); THROW_NOT_OK(h12_b.Reserve(nproc));
            THROW_NOT_OK(h13_b.Reserve(nproc)); THROW_NOT_OK(h22_b.Reserve(nproc));
            THROW_NOT_OK(chi_b.Reserve(nproc));
            THROW_NOT_OK(mp_b.Reserve(nproc));  THROW_NOT_OK(mv_b.Reserve(nproc));
            THROW_NOT_OK(np_b.Reserve(nproc));  THROW_NOT_OK(nv_b.Reserve(nproc));
        }

        for (int64_t i = 0; i < nproc; i++) {
            int w[6] = { w0->Value(i), w1->Value(i), w2->Value(i),
                         w3->Value(i), w4->Value(i), w5->Value(i) };
            ProcResult r;
            if (!proc_compute(ws, w, &r)) die_row(rg, i, w);

            if (args.verify) {
                v_rows++;
                /* M/N counts: BaHo mp,mv = M points/verts; np,nv = N points/facets.
                   Original columns: point_count, vertex_count, dual_point_count,
                   facet_count. Cross-check both orientations are self-consistent. */
                int64_t o_pcv = o_pc->Value(i), o_dpcv = o_dpc->Value(i);
                int64_t o_vcv = o_vc->Value(i), o_fcv = o_fc->Value(i);
                bool counts_ok =
                    (r.bh_mp == o_pcv && r.bh_np == o_dpcv &&
                     r.bh_mv == o_vcv && r.bh_nv == o_fcv) ||
                    (r.bh_np == o_pcv && r.bh_mp == o_dpcv &&
                     r.bh_nv == o_vcv && r.bh_mv == o_fcv);
                if (!counts_ok) {
                    v_count_mismatch++;
                    if (v_count_mismatch <= 5)
                        std::cerr << "count mismatch row " << i
                                  << " bh(mp,mv,np,nv)=" << r.bh_mp << "," << r.bh_mv
                                  << "," << r.bh_np << "," << r.bh_nv
                                  << " orig(pc,vc,dpc,fc)=" << o_pcv << "," << o_vcv
                                  << "," << o_dpcv << "," << o_fcv << "\n";
                }
                int oh[3] = { o_h11->Value(i), o_h12->Value(i), o_h13->Value(i) };
                int nh[3] = { r.h11, r.h12, r.h13 };
                for (int k = 0; k < 3; k++) {
                    if (nh[k] > 32767 || nh[k] < -32768) v_h_overflow++;
                    else if (nh[k] != oh[k]) v_h_mismatch_inrange++;
                }
                uint64_t lo, hi;
                hash_nf(r.nf, r.dim, r.nv, &lo, &hi);
                if (lo != o_hlo->Value(i) || hi != o_hhi->Value(i)) {
                    v_hash_mismatch++;
                    if (v_hash_mismatch <= 5)
                        std::cerr << "NF hash mismatch row " << i << "\n";
                }
            } else {
                if (args.computed_only) {
                    source_rg_b.UnsafeAppend(rg);
                    source_row_b.UnsafeAppend(static_cast<int32_t>(i));
                }
                THROW_NOT_OK(nf_builder->Append());
                for (int a = 0; a < r.dim; a++)
                    for (int b = 0; b < r.nv; b++)
                        THROW_NOT_OK(nf_vals->Append(static_cast<int32_t>(r.nf[a][b])));
                nf_nv_b.UnsafeAppend(static_cast<int16_t>(r.nv));
                h11_b.UnsafeAppend(r.h11); h12_b.UnsafeAppend(r.h12);
                h13_b.UnsafeAppend(r.h13); h22_b.UnsafeAppend(r.h22);
                chi_b.UnsafeAppend(r.chi);
                mp_b.UnsafeAppend(r.bh_mp); mv_b.UnsafeAppend(r.bh_mv);
                np_b.UnsafeAppend(r.bh_np); nv_b.UnsafeAppend(r.bh_nv);
            }
        }
        total_rows += nproc;

        if (!args.verify) {
            std::shared_ptr<arrow::Array> a_source_rg, a_source_row, a_nf,
                a_nfnv, a_h11, a_h12, a_h13, a_h22, a_chi, a_mp, a_mv, a_np,
                a_nv;
            if (args.computed_only) {
                THROW_NOT_OK(source_rg_b.Finish(&a_source_rg));
                THROW_NOT_OK(source_row_b.Finish(&a_source_row));
            }
            THROW_NOT_OK(nf_builder->Finish(&a_nf));
            THROW_NOT_OK(nf_nv_b.Finish(&a_nfnv));
            THROW_NOT_OK(h11_b.Finish(&a_h11)); THROW_NOT_OK(h12_b.Finish(&a_h12));
            THROW_NOT_OK(h13_b.Finish(&a_h13)); THROW_NOT_OK(h22_b.Finish(&a_h22));
            THROW_NOT_OK(chi_b.Finish(&a_chi));
            THROW_NOT_OK(mp_b.Finish(&a_mp)); THROW_NOT_OK(mv_b.Finish(&a_mv));
            THROW_NOT_OK(np_b.Finish(&a_np)); THROW_NOT_OK(nv_b.Finish(&a_nv));

            std::vector<std::shared_ptr<arrow::Field>> fields;
            std::vector<std::shared_ptr<arrow::ChunkedArray>> cols;
            auto add = [&](const char *name, std::shared_ptr<arrow::DataType> ty,
                           std::shared_ptr<arrow::Array> arr) {
                fields.push_back(arrow::field(name, ty));
                cols.push_back(std::make_shared<arrow::ChunkedArray>(arr));
            };
            if (args.computed_only) {
                add("source_row_group", arrow::int32(), a_source_rg);
                add("source_row_in_group", arrow::int32(), a_source_row);
            } else {
                /* If --limit truncated the row group, slice originals to match. */
                std::shared_ptr<arrow::Table> base =
                    (nproc < n) ? table->Slice(0, nproc) : table;
                fields = base->schema()->fields();
                cols   = base->columns();
            }
            add("nf",    arrow::list(arrow::int32()), a_nf);
            add("nf_nv", arrow::int16(),  a_nfnv);
            add("h11_c", arrow::int32(),  a_h11);
            add("h12_c", arrow::int32(),  a_h12);
            add("h13_c", arrow::int32(),  a_h13);
            add("h22",   arrow::int32(),  a_h22);
            add("chi",   arrow::int64(),  a_chi);
            add("bh_mp", arrow::int32(),  a_mp);
            add("bh_mv", arrow::int32(),  a_mv);
            add("bh_np", arrow::int32(),  a_np);
            add("bh_nv", arrow::int32(),  a_nv);

            auto out_schema = arrow::schema(fields);
            auto out_table  = arrow::Table::Make(out_schema, cols, nproc);

            if (!writer) {
                ASSIGN_OR_THROW(out, arrow::io::FileOutputStream::Open(args.output));
                auto wp = parquet::WriterProperties::Builder()
                              .compression(arrow::Compression::ZSTD)
                              ->compression_level(3)
                              ->build();
                auto ap = parquet::ArrowWriterProperties::Builder().store_schema()->build();
                THROW_NOT_OK(parquet::arrow::FileWriter::Open(
                    *out_schema, pool, out, wp, ap, &writer));
            }
            /* One row group per input row group. */
            THROW_NOT_OK(writer->WriteTable(*out_table, nproc));
        }
    }

    if (writer) THROW_NOT_OK(writer->Close());
    proc_workspace_free(ws);

    if (args.verify) {
        std::cerr << "VERIFY rows=" << v_rows
                  << " count_mismatch=" << v_count_mismatch
                  << " h_overflow(int16)=" << v_h_overflow
                  << " h_mismatch_inrange=" << v_h_mismatch_inrange
                  << " nf_hash_mismatch=" << v_hash_mismatch << "\n";
        /* In-range Hodge disagreements and hash mismatches must be zero. */
        if (v_count_mismatch || v_h_mismatch_inrange || v_hash_mismatch) {
            std::cerr << "VERIFY FAILED\n";
            return 3;
        }
        std::cerr << "VERIFY OK (overflow rows confirm the int16 problem)\n";
    } else {
        std::cerr << "DONE rows=" << total_rows
                  << " rg=[" << args.rg_start << "," << args.rg_end << ") -> "
                  << args.output << "\n";
    }
    return 0;
} catch (const std::exception &e) {
    std::cerr << "ERROR: " << e.what() << "\n";
    return 1;
}
