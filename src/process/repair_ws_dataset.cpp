/**
 * repair_ws_dataset.cpp - Repair the sieved weight-system ML dataset.
 *
 * The sieved dataset has one row per unique normal form, with:
 *   - nf_vertices: 5 x vertex_count int32 normal form, flattened row-major
 *   - weight_systems: count x 6 int32 weights, flattened row-major
 *
 * This tool computes the same xxHash128 normal-form key for nf_vertices and
 * for the clean final dataset's nf column, then writes a new dataset preserving
 * nf_vertices/weight_systems/count from the sieved file and replacing all
 * geometric invariants with the clean values.
 */
#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/file_reader.h>

#define XXH_INLINE_ALL
#include "../classify/xxhash.h"

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
    std::string weights;
    std::string clean;
    std::string output;
    int limit_weight_row_groups = -1;
};

Args parse_args(int argc, char **argv) {
    Args a;
    for (int i = 1; i < argc; i++) {
        std::string k = argv[i];
        auto next = [&]() -> std::string {
            if (i + 1 >= argc) {
                std::cerr << "missing value for " << k << "\n";
                std::exit(1);
            }
            return argv[++i];
        };
        if      (k == "--weights") a.weights = next();
        else if (k == "--clean")   a.clean = next();
        else if (k == "--output")  a.output = next();
        else if (k == "--limit-weight-row-groups")
            a.limit_weight_row_groups = std::stoi(next());
        else {
            std::cerr << "unknown arg: " << k << "\n";
            std::exit(1);
        }
    }
    if (a.weights.empty() || a.clean.empty() || a.output.empty()) {
        std::cerr << "usage: repair_ws_dataset --weights WS.parquet "
                     "--clean CLEAN.parquet --output OUT.parquet "
                     "[--limit-weight-row-groups N]\n";
        std::exit(1);
    }
    return a;
}

struct Key {
    uint64_t lo = 0, hi = 0;
    bool operator==(const Key &o) const { return lo == o.lo && hi == o.hi; }
};

struct KeyHash {
    size_t operator()(const Key &k) const {
        return static_cast<size_t>(k.lo ^ (k.hi + 0x9e3779b97f4a7c15ULL +
            (k.lo << 6) + (k.lo >> 2)));
    }
};

struct Invariants {
    bool found = false;
    int32_t vertex_count = 0;
    int32_t facet_count = 0;
    int32_t point_count = 0;
    int32_t dual_point_count = 0;
    int32_t h11 = 0, h12 = 0, h13 = 0, h22 = 0;
    int64_t chi = 0;
    int32_t bh_mp = 0, bh_mv = 0, bh_np = 0, bh_nv = 0;
};

Key hash_bytes(const uint8_t *data, int64_t nbytes) {
    XXH128_hash_t h = XXH3_128bits(data, static_cast<size_t>(nbytes));
    return Key{h.low64, h.high64};
}

int schema_column_index(const std::shared_ptr<arrow::Schema> &schema,
                        const std::string &name) {
    int idx = schema->GetFieldIndex(name);
    if (idx < 0) throw std::runtime_error("missing column: " + name);
    return idx;
}

std::shared_ptr<arrow::Array> col(const std::shared_ptr<arrow::Table> &t,
                                  const std::string &name) {
    auto c = t->GetColumnByName(name);
    if (!c) throw std::runtime_error("missing column: " + name);
    if (c->num_chunks() != 1)
        throw std::runtime_error("expected single chunk for " + name);
    return c->chunk(0);
}

std::shared_ptr<arrow::Array> decode_weight_systems(
    arrow::MemoryPool *pool,
    const std::shared_ptr<arrow::LargeBinaryArray> &packed) {
    auto int_builder = std::make_shared<arrow::Int32Builder>(pool);
    auto inner_builder = std::make_shared<arrow::ListBuilder>(pool, int_builder);
    auto outer_builder = std::make_shared<arrow::ListBuilder>(pool, inner_builder);
    auto *inner_values =
        static_cast<arrow::Int32Builder *>(inner_builder->value_builder());

    THROW_NOT_OK(outer_builder->Reserve(packed->length()));
    for (int64_t i = 0; i < packed->length(); i++) {
        auto bytes = packed->GetView(i);
        if (bytes.size() % (6 * sizeof(int32_t)) != 0) {
            throw std::runtime_error("weight_systems byte length is not a multiple of 6 int32");
        }
        THROW_NOT_OK(outer_builder->Append());
        const int64_t nsystems = bytes.size() / (6 * sizeof(int32_t));
        const int32_t *weights =
            reinterpret_cast<const int32_t *>(bytes.data());
        for (int64_t s = 0; s < nsystems; s++) {
            THROW_NOT_OK(inner_builder->Append());
            for (int j = 0; j < 6; j++)
                THROW_NOT_OK(inner_values->Append(weights[s * 6 + j]));
        }
    }

    std::shared_ptr<arrow::Array> out;
    THROW_NOT_OK(outer_builder->Finish(&out));
    return out;
}

std::unique_ptr<parquet::arrow::FileReader> open_reader(
    const std::string &path,
    arrow::MemoryPool *pool,
    std::shared_ptr<arrow::io::ReadableFile> *holder) {
    ASSIGN_OR_THROW(*holder, arrow::io::ReadableFile::Open(path, pool));
    std::unique_ptr<parquet::arrow::FileReader> reader;
    THROW_NOT_OK(parquet::arrow::OpenFile(*holder, pool, &reader));
    return reader;
}

} // namespace

int main(int argc, char **argv) try {
    Args args = parse_args(argc, argv);
    arrow::MemoryPool *pool = arrow::default_memory_pool();

    std::shared_ptr<arrow::io::ReadableFile> weights_file;
    auto weights_reader = open_reader(args.weights, pool, &weights_file);
    std::shared_ptr<arrow::Schema> weights_schema;
    THROW_NOT_OK(weights_reader->GetSchema(&weights_schema));

    const std::vector<int> ws_nf_cols = {
        schema_column_index(weights_schema, "nf_vertices")
    };

    std::unordered_map<Key, Invariants, KeyHash> index;
    const int64_t ws_rows_total =
        parquet::ParquetFileReader::OpenFile(args.weights, false)->metadata()->num_rows();
    index.reserve(static_cast<size_t>(ws_rows_total * 1.15));

    int64_t target_rows = 0, duplicate_nf_hashes = 0;
    const int weight_row_groups =
        (args.limit_weight_row_groups >= 0 &&
         args.limit_weight_row_groups < weights_reader->num_row_groups())
            ? args.limit_weight_row_groups
            : weights_reader->num_row_groups();

    for (int rg = 0; rg < weight_row_groups; rg++) {
        std::shared_ptr<arrow::Table> table;
        THROW_NOT_OK(weights_reader->ReadRowGroup(rg, ws_nf_cols, &table));
        auto nf = std::static_pointer_cast<arrow::LargeBinaryArray>(
            col(table, "nf_vertices"));
        for (int64_t i = 0; i < nf->length(); i++) {
            auto bytes = nf->GetView(i);
            Key k = hash_bytes(reinterpret_cast<const uint8_t *>(bytes.data()),
                               bytes.size());
            auto [it, inserted] = index.emplace(k, Invariants{});
            if (!inserted) duplicate_nf_hashes++;
        }
        target_rows += table->num_rows();
        if ((rg + 1) % 10 == 0 || rg + 1 == weight_row_groups)
            std::cerr << "loaded target normal forms row_groups=" << (rg + 1)
                      << "/" << weight_row_groups
                      << " rows=" << target_rows << "\n";
    }
    std::cerr << "target_rows=" << target_rows
              << " target_hashes=" << index.size()
              << " duplicate_nf_hashes=" << duplicate_nf_hashes << "\n";

    std::shared_ptr<arrow::io::ReadableFile> clean_file;
    auto clean_reader = open_reader(args.clean, pool, &clean_file);
    std::shared_ptr<arrow::Schema> clean_schema;
    THROW_NOT_OK(clean_reader->GetSchema(&clean_schema));
    const std::vector<std::string> clean_names = {
        "vertex_count", "facet_count", "point_count", "dual_point_count",
        "nf", "h11", "h12", "h13", "h22", "chi",
        "bh_mp", "bh_mv", "bh_np", "bh_nv"
    };
    std::vector<int> clean_cols;
    for (const auto &name : clean_names)
        clean_cols.push_back(schema_column_index(clean_schema, name));

    int64_t matched_rows = 0, duplicate_clean_matches = 0;
    for (int rg = 0; rg < clean_reader->num_row_groups(); rg++) {
        std::shared_ptr<arrow::Table> table;
        THROW_NOT_OK(clean_reader->ReadRowGroup(rg, clean_cols, &table));

        auto vc = std::static_pointer_cast<arrow::Int32Array>(col(table, "vertex_count"));
        auto fc = std::static_pointer_cast<arrow::Int32Array>(col(table, "facet_count"));
        auto pc = std::static_pointer_cast<arrow::Int32Array>(col(table, "point_count"));
        auto dpc = std::static_pointer_cast<arrow::Int32Array>(col(table, "dual_point_count"));
        auto nf = std::static_pointer_cast<arrow::ListArray>(col(table, "nf"));
        auto nf_vals = std::static_pointer_cast<arrow::Int32Array>(nf->values());
        auto h11 = std::static_pointer_cast<arrow::Int32Array>(col(table, "h11"));
        auto h12 = std::static_pointer_cast<arrow::Int32Array>(col(table, "h12"));
        auto h13 = std::static_pointer_cast<arrow::Int32Array>(col(table, "h13"));
        auto h22 = std::static_pointer_cast<arrow::Int32Array>(col(table, "h22"));
        auto chi = std::static_pointer_cast<arrow::Int64Array>(col(table, "chi"));
        auto bh_mp = std::static_pointer_cast<arrow::Int32Array>(col(table, "bh_mp"));
        auto bh_mv = std::static_pointer_cast<arrow::Int32Array>(col(table, "bh_mv"));
        auto bh_np = std::static_pointer_cast<arrow::Int32Array>(col(table, "bh_np"));
        auto bh_nv = std::static_pointer_cast<arrow::Int32Array>(col(table, "bh_nv"));

        const int32_t *values = nf_vals->raw_values();
        for (int64_t i = 0; i < table->num_rows(); i++) {
            int64_t off = nf->value_offset(i);
            int64_t len = nf->value_length(i);
            Key k = hash_bytes(reinterpret_cast<const uint8_t *>(values + off),
                               len * static_cast<int64_t>(sizeof(int32_t)));
            auto it = index.find(k);
            if (it == index.end()) continue;
            Invariants &inv = it->second;
            if (inv.found) {
                duplicate_clean_matches++;
                continue;
            }
            inv.found = true;
            inv.vertex_count = vc->Value(i);
            inv.facet_count = fc->Value(i);
            inv.point_count = pc->Value(i);
            inv.dual_point_count = dpc->Value(i);
            inv.h11 = h11->Value(i); inv.h12 = h12->Value(i);
            inv.h13 = h13->Value(i); inv.h22 = h22->Value(i);
            inv.chi = chi->Value(i);
            inv.bh_mp = bh_mp->Value(i); inv.bh_mv = bh_mv->Value(i);
            inv.bh_np = bh_np->Value(i); inv.bh_nv = bh_nv->Value(i);
            matched_rows++;
        }
        if ((rg + 1) % 25 == 0 || rg + 1 == clean_reader->num_row_groups())
            std::cerr << "scanned clean row_groups=" << (rg + 1)
                      << "/" << clean_reader->num_row_groups()
                      << " matched=" << matched_rows << "/" << index.size() << "\n";
        if (matched_rows == static_cast<int64_t>(index.size())) {
            std::cerr << "all target normal forms matched after clean row_group="
                      << rg << "\n";
            break;
        }
    }
    if (matched_rows != static_cast<int64_t>(index.size())) {
        throw std::runtime_error("not all target normal forms were found: matched=" +
            std::to_string(matched_rows) + " target_hashes=" +
            std::to_string(index.size()));
    }
    std::cerr << "clean matches complete matched=" << matched_rows
              << " duplicate_clean_matches=" << duplicate_clean_matches << "\n";

    const std::vector<int> write_input_cols = {
        schema_column_index(weights_schema, "nf_vertices"),
        schema_column_index(weights_schema, "weight_systems"),
        schema_column_index(weights_schema, "count"),
    };
    std::shared_ptr<arrow::io::FileOutputStream> out;
    ASSIGN_OR_THROW(out, arrow::io::FileOutputStream::Open(args.output));
    std::unique_ptr<parquet::arrow::FileWriter> writer;

    int64_t written_rows = 0, missing_on_write = 0;
    for (int rg = 0; rg < weight_row_groups; rg++) {
        std::shared_ptr<arrow::Table> table;
        THROW_NOT_OK(weights_reader->ReadRowGroup(rg, write_input_cols, &table));
        auto nf = std::static_pointer_cast<arrow::LargeBinaryArray>(
            col(table, "nf_vertices"));
        auto packed_ws = std::static_pointer_cast<arrow::LargeBinaryArray>(
            col(table, "weight_systems"));
        auto decoded_ws = decode_weight_systems(pool, packed_ws);

        arrow::Int32Builder vc_b, fc_b, pc_b, dpc_b;
        arrow::Int32Builder h11_b, h12_b, h13_b, h22_b;
        arrow::Int64Builder chi_b;
        arrow::Int32Builder bh_mp_b, bh_mv_b, bh_np_b, bh_nv_b;
        THROW_NOT_OK(vc_b.Reserve(table->num_rows()));
        THROW_NOT_OK(fc_b.Reserve(table->num_rows()));
        THROW_NOT_OK(pc_b.Reserve(table->num_rows()));
        THROW_NOT_OK(dpc_b.Reserve(table->num_rows()));
        THROW_NOT_OK(h11_b.Reserve(table->num_rows()));
        THROW_NOT_OK(h12_b.Reserve(table->num_rows()));
        THROW_NOT_OK(h13_b.Reserve(table->num_rows()));
        THROW_NOT_OK(h22_b.Reserve(table->num_rows()));
        THROW_NOT_OK(chi_b.Reserve(table->num_rows()));
        THROW_NOT_OK(bh_mp_b.Reserve(table->num_rows()));
        THROW_NOT_OK(bh_mv_b.Reserve(table->num_rows()));
        THROW_NOT_OK(bh_np_b.Reserve(table->num_rows()));
        THROW_NOT_OK(bh_nv_b.Reserve(table->num_rows()));

        for (int64_t i = 0; i < table->num_rows(); i++) {
            auto bytes = nf->GetView(i);
            Key k = hash_bytes(reinterpret_cast<const uint8_t *>(bytes.data()),
                               bytes.size());
            auto it = index.find(k);
            if (it == index.end() || !it->second.found) {
                missing_on_write++;
                continue;
            }
            const Invariants &inv = it->second;
            vc_b.UnsafeAppend(inv.vertex_count);
            fc_b.UnsafeAppend(inv.facet_count);
            pc_b.UnsafeAppend(inv.point_count);
            dpc_b.UnsafeAppend(inv.dual_point_count);
            h11_b.UnsafeAppend(inv.h11); h12_b.UnsafeAppend(inv.h12);
            h13_b.UnsafeAppend(inv.h13); h22_b.UnsafeAppend(inv.h22);
            chi_b.UnsafeAppend(inv.chi);
            bh_mp_b.UnsafeAppend(inv.bh_mp); bh_mv_b.UnsafeAppend(inv.bh_mv);
            bh_np_b.UnsafeAppend(inv.bh_np); bh_nv_b.UnsafeAppend(inv.bh_nv);
        }
        if (missing_on_write)
            throw std::runtime_error("missing invariant while writing row_group=" +
                std::to_string(rg));

        std::shared_ptr<arrow::Array> vc_a, fc_a, pc_a, dpc_a, h11_a, h12_a,
            h13_a, h22_a, chi_a, bh_mp_a, bh_mv_a, bh_np_a, bh_nv_a;
        THROW_NOT_OK(vc_b.Finish(&vc_a)); THROW_NOT_OK(fc_b.Finish(&fc_a));
        THROW_NOT_OK(pc_b.Finish(&pc_a)); THROW_NOT_OK(dpc_b.Finish(&dpc_a));
        THROW_NOT_OK(h11_b.Finish(&h11_a)); THROW_NOT_OK(h12_b.Finish(&h12_a));
        THROW_NOT_OK(h13_b.Finish(&h13_a)); THROW_NOT_OK(h22_b.Finish(&h22_a));
        THROW_NOT_OK(chi_b.Finish(&chi_a));
        THROW_NOT_OK(bh_mp_b.Finish(&bh_mp_a)); THROW_NOT_OK(bh_mv_b.Finish(&bh_mv_a));
        THROW_NOT_OK(bh_np_b.Finish(&bh_np_a)); THROW_NOT_OK(bh_nv_b.Finish(&bh_nv_a));

        std::vector<std::shared_ptr<arrow::Field>> fields = {
            weights_schema->field(schema_column_index(weights_schema, "nf_vertices")),
            arrow::field("weight_systems",
                arrow::list(arrow::list(arrow::int32()))),
            weights_schema->field(schema_column_index(weights_schema, "count")),
            arrow::field("vertex_count", arrow::int32()),
            arrow::field("facet_count", arrow::int32()),
            arrow::field("point_count", arrow::int32()),
            arrow::field("dual_point_count", arrow::int32()),
            arrow::field("h11", arrow::int32()),
            arrow::field("h12", arrow::int32()),
            arrow::field("h13", arrow::int32()),
            arrow::field("h22", arrow::int32()),
            arrow::field("chi", arrow::int64()),
            arrow::field("bh_mp", arrow::int32()),
            arrow::field("bh_mv", arrow::int32()),
            arrow::field("bh_np", arrow::int32()),
            arrow::field("bh_nv", arrow::int32()),
        };
        std::vector<std::shared_ptr<arrow::ChunkedArray>> cols = {
            table->GetColumnByName("nf_vertices"),
            std::make_shared<arrow::ChunkedArray>(decoded_ws),
            table->GetColumnByName("count"),
            std::make_shared<arrow::ChunkedArray>(vc_a),
            std::make_shared<arrow::ChunkedArray>(fc_a),
            std::make_shared<arrow::ChunkedArray>(pc_a),
            std::make_shared<arrow::ChunkedArray>(dpc_a),
            std::make_shared<arrow::ChunkedArray>(h11_a),
            std::make_shared<arrow::ChunkedArray>(h12_a),
            std::make_shared<arrow::ChunkedArray>(h13_a),
            std::make_shared<arrow::ChunkedArray>(h22_a),
            std::make_shared<arrow::ChunkedArray>(chi_a),
            std::make_shared<arrow::ChunkedArray>(bh_mp_a),
            std::make_shared<arrow::ChunkedArray>(bh_mv_a),
            std::make_shared<arrow::ChunkedArray>(bh_np_a),
            std::make_shared<arrow::ChunkedArray>(bh_nv_a),
        };
        auto out_schema = arrow::schema(fields);
        auto out_table = arrow::Table::Make(out_schema, cols, table->num_rows());
        if (!writer) {
            auto wp = parquet::WriterProperties::Builder()
                          .compression(arrow::Compression::ZSTD)
                          ->compression_level(3)
                          ->build();
            auto ap = parquet::ArrowWriterProperties::Builder().store_schema()->build();
            THROW_NOT_OK(parquet::arrow::FileWriter::Open(
                *out_schema, pool, out, wp, ap, &writer));
        }
        THROW_NOT_OK(writer->WriteTable(*out_table, out_table->num_rows()));
        written_rows += out_table->num_rows();
        if ((rg + 1) % 10 == 0 || rg + 1 == weight_row_groups)
            std::cerr << "written row_groups=" << (rg + 1)
                      << "/" << weight_row_groups
                      << " rows=" << written_rows << "\n";
    }
    if (writer) THROW_NOT_OK(writer->Close());
    std::cerr << "DONE repaired rows=" << written_rows << " -> "
              << args.output << "\n";
    return 0;
} catch (const std::exception &e) {
    std::cerr << "ERROR: " << e.what() << "\n";
    return 1;
}
