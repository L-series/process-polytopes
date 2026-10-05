/**
 * convert_nf_vertices.cpp - Convert nf_vertices from packed binary to nested lists.
 *
 * Input is the repaired ML parquet with:
 *   nf_vertices: large_binary containing 5 * vertex_count int32 values
 *
 * Output preserves all columns and order, replacing nf_vertices with:
 *   nf_vertices: list<list<int32>>
 */
#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

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
        if      (k == "--input")  a.input = next();
        else if (k == "--output") a.output = next();
        else if (k == "--limit-row-groups") a.limit_row_groups = std::stoi(next());
        else {
            std::cerr << "unknown arg: " << k << "\n";
            std::exit(1);
        }
    }
    if (a.input.empty() || a.output.empty()) {
        std::cerr << "usage: convert_nf_vertices --input IN.parquet "
                     "--output OUT.parquet [--limit-row-groups N]\n";
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

std::shared_ptr<arrow::Array> decode_nf_vertices(
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
        if (bytes.size() % (5 * sizeof(int32_t)) != 0)
            throw std::runtime_error("nf_vertices byte length is not 5-row int32 matrix");

        const int64_t nints = bytes.size() / sizeof(int32_t);
        const int64_t vertex_count = nints / 5;
        const int32_t *values =
            reinterpret_cast<const int32_t *>(bytes.data());

        THROW_NOT_OK(outer_builder->Append());
        for (int row = 0; row < 5; row++) {
            THROW_NOT_OK(inner_builder->Append());
            for (int64_t j = 0; j < vertex_count; j++)
                THROW_NOT_OK(inner_values->Append(values[row * vertex_count + j]));
        }
    }

    std::shared_ptr<arrow::Array> out;
    THROW_NOT_OK(outer_builder->Finish(&out));
    return out;
}

} // namespace

int main(int argc, char **argv) try {
    Args args = parse_args(argc, argv);
    arrow::MemoryPool *pool = arrow::default_memory_pool();

    std::shared_ptr<arrow::io::ReadableFile> input_file;
    ASSIGN_OR_THROW(input_file, arrow::io::ReadableFile::Open(args.input, pool));
    std::unique_ptr<parquet::arrow::FileReader> reader;
    THROW_NOT_OK(parquet::arrow::OpenFile(input_file, pool, &reader));

    std::shared_ptr<arrow::Schema> input_schema;
    THROW_NOT_OK(reader->GetSchema(&input_schema));
    const int nf_idx = schema_column_index(input_schema, "nf_vertices");

    std::vector<std::shared_ptr<arrow::Field>> fields = input_schema->fields();
    fields[nf_idx] = arrow::field("nf_vertices",
        arrow::list(arrow::list(arrow::int32())));
    auto output_schema = arrow::schema(fields);

    std::shared_ptr<arrow::io::FileOutputStream> out;
    ASSIGN_OR_THROW(out, arrow::io::FileOutputStream::Open(args.output));
    std::unique_ptr<parquet::arrow::FileWriter> writer;

    auto wp = parquet::WriterProperties::Builder()
                  .compression(arrow::Compression::ZSTD)
                  ->compression_level(3)
                  ->build();
    auto ap = parquet::ArrowWriterProperties::Builder().store_schema()->build();
    THROW_NOT_OK(parquet::arrow::FileWriter::Open(
        *output_schema, pool, out, wp, ap, &writer));

    const int nrg = (args.limit_row_groups >= 0 &&
                     args.limit_row_groups < reader->num_row_groups())
                        ? args.limit_row_groups
                        : reader->num_row_groups();
    int64_t total_rows = 0;
    for (int rg = 0; rg < nrg; rg++) {
        std::shared_ptr<arrow::Table> table;
        THROW_NOT_OK(reader->ReadRowGroup(rg, &table));
        auto nf = std::static_pointer_cast<arrow::LargeBinaryArray>(
            col(table, "nf_vertices"));
        auto decoded_nf = decode_nf_vertices(pool, nf);

        std::vector<std::shared_ptr<arrow::ChunkedArray>> cols = table->columns();
        cols[nf_idx] = std::make_shared<arrow::ChunkedArray>(decoded_nf);
        auto out_table = arrow::Table::Make(output_schema, cols, table->num_rows());
        THROW_NOT_OK(writer->WriteTable(*out_table, out_table->num_rows()));
        total_rows += out_table->num_rows();

        if ((rg + 1) % 10 == 0 || rg + 1 == nrg)
            std::cerr << "converted row_groups=" << (rg + 1) << "/" << nrg
                      << " rows=" << total_rows << "\n";
    }

    THROW_NOT_OK(writer->Close());
    std::cerr << "DONE converted rows=" << total_rows
              << " -> " << args.output << "\n";
    return 0;
} catch (const std::exception &e) {
    std::cerr << "ERROR: " << e.what() << "\n";
    return 1;
}
