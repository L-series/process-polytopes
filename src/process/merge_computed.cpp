/**
 * merge_computed.cpp - Join unique_polytopes.parquet with computed shards.
 *
 * Streams one input row group at a time. The output contains all original
 * columns except the corrupted int16 h11/h12/h13, plus computed columns from
 * unique_polytopes_computed. Clean h11/h12/h13 are written under the original
 * names as int32 columns.
 */
#include <cstdint>
#include <iostream>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/file_reader.h>

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
    std::string computed_dir;
    std::string output;
    int rg_per_shard = 4;
    int rg_start = 0;
    int rg_end = -1;
    int limit_row_groups = -1;
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
        if      (k == "--input")            a.input = next();
        else if (k == "--computed-dir")     a.computed_dir = next();
        else if (k == "--output")           a.output = next();
        else if (k == "--rg-per-shard")     a.rg_per_shard = std::stoi(next());
        else if (k == "--rg-start")         a.rg_start = std::stoi(next());
        else if (k == "--rg-end")           a.rg_end = std::stoi(next());
        else if (k == "--limit-row-groups") a.limit_row_groups = std::stoi(next());
        else {
            std::cerr << "unknown arg: " << k << "\n";
            std::exit(1);
        }
    }
    if (a.input.empty() || a.computed_dir.empty() || a.output.empty() ||
        a.rg_per_shard <= 0) {
        std::cerr << "usage: merge_computed --input F.parquet "
                     "--computed-dir D --output OUT.parquet "
                     "[--rg-per-shard 4] [--rg-start S --rg-end E] "
                     "[--limit-row-groups N]\n";
        std::exit(1);
    }
    return a;
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

std::string shard_path(const std::string &dir, int shard) {
    char buf[64];
    std::snprintf(buf, sizeof(buf), "part-%05d.parquet", shard);
    return dir + "/" + buf;
}

std::shared_ptr<arrow::Field> renamed_field(
    const std::shared_ptr<arrow::Schema> &schema,
    const std::string &from,
    const std::string &to) {
    return schema->field(schema_column_index(schema, from))->WithName(to);
}

void validate_alignment(const std::shared_ptr<arrow::Table> &computed,
                        int rg,
                        int64_t expected_rows) {
    if (computed->num_rows() != expected_rows) {
        throw std::runtime_error("row count mismatch at row group " +
            std::to_string(rg) + ": original=" + std::to_string(expected_rows) +
            " computed=" + std::to_string(computed->num_rows()));
    }
    auto source_rg =
        std::static_pointer_cast<arrow::Int32Array>(col(computed, "source_row_group"));
    auto source_row =
        std::static_pointer_cast<arrow::Int32Array>(col(computed, "source_row_in_group"));
    if (expected_rows > 0 &&
        (source_rg->Value(0) != rg || source_rg->Value(expected_rows - 1) != rg ||
         source_row->Value(0) != 0 ||
         source_row->Value(expected_rows - 1) != expected_rows - 1)) {
        throw std::runtime_error("computed shard alignment mismatch at row group " +
            std::to_string(rg));
    }
}

} // namespace

int main(int argc, char **argv) try {
    Args args = parse_args(argc, argv);
    arrow::MemoryPool *pool = arrow::default_memory_pool();

    std::shared_ptr<arrow::io::ReadableFile> input_file;
    ASSIGN_OR_THROW(input_file, arrow::io::ReadableFile::Open(args.input, pool));
    std::unique_ptr<parquet::arrow::FileReader> input_reader;
    THROW_NOT_OK(parquet::arrow::OpenFile(input_file, pool, &input_reader));

    std::shared_ptr<arrow::Schema> input_schema;
    THROW_NOT_OK(input_reader->GetSchema(&input_schema));

    const std::set<std::string> drop_original = {"hash_lo", "hash_hi", "h11", "h12", "h13"};
    std::vector<int> input_cols;
    std::vector<std::shared_ptr<arrow::Field>> output_fields;
    int count_col = -1;
    std::shared_ptr<arrow::Field> count_field;
    for (int i = 0; i < input_schema->num_fields(); i++) {
        const auto &field = input_schema->field(i);
        if (drop_original.count(field->name())) continue;
        if (field->name() == "count") {
            count_col = i;
            count_field = field;
            continue;
        }
        input_cols.push_back(i);
        output_fields.push_back(field);
    }
    if (count_col < 0) throw std::runtime_error("missing column: count");
    input_cols.push_back(count_col);

    const int nrg_total = input_reader->num_row_groups();
    if (args.rg_start < 0 || args.rg_start >= nrg_total)
        throw std::runtime_error("invalid --rg-start");
    int nrg_end = (args.rg_end >= 0 && args.rg_end < nrg_total)
                    ? args.rg_end
                    : nrg_total;
    if (args.limit_row_groups >= 0 &&
        args.rg_start + args.limit_row_groups < nrg_end)
        nrg_end = args.rg_start + args.limit_row_groups;
    if (args.rg_start >= nrg_end)
        throw std::runtime_error("empty row-group range");

    std::shared_ptr<arrow::io::FileOutputStream> out;
    ASSIGN_OR_THROW(out, arrow::io::FileOutputStream::Open(args.output));
    std::unique_ptr<parquet::arrow::FileWriter> writer;

    int current_shard = -1;
    std::shared_ptr<arrow::io::ReadableFile> computed_file;
    std::unique_ptr<parquet::arrow::FileReader> computed_reader;
    std::shared_ptr<arrow::Schema> computed_schema;

    int64_t total_rows = 0;
    for (int rg = args.rg_start; rg < nrg_end; rg++) {
        std::shared_ptr<arrow::Table> original;
        THROW_NOT_OK(input_reader->ReadRowGroup(rg, input_cols, &original));

        const int shard = rg / args.rg_per_shard;
        const int shard_rg = rg % args.rg_per_shard;
        if (shard != current_shard) {
            current_shard = shard;
            computed_reader.reset();
            computed_file.reset();
            ASSIGN_OR_THROW(computed_file,
                arrow::io::ReadableFile::Open(shard_path(args.computed_dir, shard), pool));
            THROW_NOT_OK(parquet::arrow::OpenFile(computed_file, pool, &computed_reader));
            THROW_NOT_OK(computed_reader->GetSchema(&computed_schema));
        }

        std::shared_ptr<arrow::Table> computed;
        THROW_NOT_OK(computed_reader->ReadRowGroup(shard_rg, &computed));
        validate_alignment(computed, rg, original->num_rows());

        auto count_array = col(original, "count");
        std::vector<std::shared_ptr<arrow::ChunkedArray>> cols = original->columns();
        cols.pop_back();
        std::vector<std::shared_ptr<arrow::Field>> fields = output_fields;
        auto add = [&](std::shared_ptr<arrow::Field> field,
                       std::shared_ptr<arrow::Array> arr) {
            fields.push_back(field);
            cols.push_back(std::make_shared<arrow::ChunkedArray>(arr));
        };

        add(computed_schema->field(schema_column_index(computed_schema, "nf")),
            col(computed, "nf"));
        add(renamed_field(computed_schema, "h11_c", "h11"), col(computed, "h11_c"));
        add(renamed_field(computed_schema, "h12_c", "h12"), col(computed, "h12_c"));
        add(renamed_field(computed_schema, "h13_c", "h13"), col(computed, "h13_c"));
        add(computed_schema->field(schema_column_index(computed_schema, "h22")),
            col(computed, "h22"));
        add(computed_schema->field(schema_column_index(computed_schema, "chi")),
            col(computed, "chi"));
        add(computed_schema->field(schema_column_index(computed_schema, "bh_mp")),
            col(computed, "bh_mp"));
        add(computed_schema->field(schema_column_index(computed_schema, "bh_mv")),
            col(computed, "bh_mv"));
        add(computed_schema->field(schema_column_index(computed_schema, "bh_np")),
            col(computed, "bh_np"));
        add(computed_schema->field(schema_column_index(computed_schema, "bh_nv")),
            col(computed, "bh_nv"));
        add(count_field, col(original, "count"));

        auto out_schema = arrow::schema(fields);
        auto out_table = arrow::Table::Make(out_schema, cols, original->num_rows());
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
        total_rows += out_table->num_rows();

        const int done_rgs = rg + 1 - args.rg_start;
        const int total_rgs = nrg_end - args.rg_start;
        if (done_rgs % 10 == 0 || rg + 1 == nrg_end) {
            std::cerr << "merged row_groups=" << done_rgs << "/" << total_rgs
                      << " rows=" << total_rows << "\n";
        }
    }

    if (writer) THROW_NOT_OK(writer->Close());
    std::cerr << "DONE merged rows=" << total_rows
              << " rg=[" << args.rg_start << "," << nrg_end << ")"
              << " -> " << args.output << "\n";
    return 0;
} catch (const std::exception &e) {
    std::cerr << "ERROR: " << e.what() << "\n";
    return 1;
}
