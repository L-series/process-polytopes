/**
 * concat_parquet.cpp - Concatenate a parquet dataset directory into one file.
 *
 * Files are read in lexical order and row groups are rewritten into one output
 * parquet file. This preserves row order for part-00000.parquet, part-00001...
 */
#include <algorithm>
#include <filesystem>
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

namespace fs = std::filesystem;

int main(int argc, char **argv) try {
    if (argc != 3) {
        std::cerr << "usage: concat_parquet DATASET_DIR OUT.parquet\n";
        return 1;
    }

    const std::string input_dir = argv[1];
    const std::string output = argv[2];
    std::vector<std::string> files;
    for (const auto &entry : fs::directory_iterator(input_dir)) {
        if (!entry.is_regular_file()) continue;
        const auto path = entry.path();
        if (path.extension() == ".parquet" &&
            path.filename().string().rfind("part-", 0) == 0) {
            files.push_back(path.string());
        }
    }
    std::sort(files.begin(), files.end());
    if (files.empty()) throw std::runtime_error("no part-*.parquet files found");

    arrow::MemoryPool *pool = arrow::default_memory_pool();
    std::shared_ptr<arrow::io::FileOutputStream> out;
    ASSIGN_OR_THROW(out, arrow::io::FileOutputStream::Open(output));
    std::unique_ptr<parquet::arrow::FileWriter> writer;
    std::shared_ptr<arrow::Schema> expected_schema;
    int64_t total_rows = 0;

    for (size_t i = 0; i < files.size(); i++) {
        std::shared_ptr<arrow::io::ReadableFile> in;
        ASSIGN_OR_THROW(in, arrow::io::ReadableFile::Open(files[i], pool));
        std::unique_ptr<parquet::arrow::FileReader> reader;
        THROW_NOT_OK(parquet::arrow::OpenFile(in, pool, &reader));

        std::shared_ptr<arrow::Schema> schema;
        THROW_NOT_OK(reader->GetSchema(&schema));
        if (!expected_schema) {
            expected_schema = schema;
            auto wp = parquet::WriterProperties::Builder()
                          .compression(arrow::Compression::ZSTD)
                          ->compression_level(3)
                          ->build();
            auto ap = parquet::ArrowWriterProperties::Builder().store_schema()->build();
            THROW_NOT_OK(parquet::arrow::FileWriter::Open(
                *expected_schema, pool, out, wp, ap, &writer));
        } else if (!schema->Equals(*expected_schema, true)) {
            throw std::runtime_error("schema mismatch in " + files[i]);
        }

        const int nrg = reader->num_row_groups();
        for (int rg = 0; rg < nrg; rg++) {
            std::shared_ptr<arrow::Table> table;
            THROW_NOT_OK(reader->ReadRowGroup(rg, &table));
            THROW_NOT_OK(writer->WriteTable(*table, table->num_rows()));
            total_rows += table->num_rows();
        }
        if ((i + 1) % 25 == 0 || i + 1 == files.size()) {
            std::cerr << "concatenated files=" << (i + 1) << "/" << files.size()
                      << " rows=" << total_rows << "\n";
        }
    }

    THROW_NOT_OK(writer->Close());
    std::cerr << "DONE concatenated files=" << files.size()
              << " rows=" << total_rows << " -> " << output << "\n";
    return 0;
} catch (const std::exception &e) {
    std::cerr << "ERROR: " << e.what() << "\n";
    return 1;
}
