#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>

namespace fs = std::filesystem;

#define CHECK_ARROW(expr)                                            \
    do {                                                             \
        arrow::Status status = (expr);                               \
        if (!status.ok())                                            \
            throw std::runtime_error(status.ToString());             \
    } while (0)

#define ASSIGN_OR_THROW(lhs, expr)                                   \
    do {                                                             \
        auto result = (expr);                                        \
        if (!result.ok())                                            \
            throw std::runtime_error(result.status().ToString());    \
        lhs = std::move(result).ValueOrDie();                        \
    } while (0)

struct Record {
    uint64_t lo;
    uint64_t hi;
    uint64_t count;
};

static bool record_less(const Record &left, const Record &right) {
    if (left.hi != right.hi) return left.hi < right.hi;
    if (left.lo != right.lo) return left.lo < right.lo;
    return left.count < right.count;
}

static std::vector<Record> read_records(const fs::path &path) {
    std::shared_ptr<arrow::io::ReadableFile> infile;
    ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(path.string()));

    std::unique_ptr<parquet::arrow::FileReader> reader;
    parquet::arrow::FileReaderBuilder builder;
    CHECK_ARROW(builder.Open(infile));
    CHECK_ARROW(builder.Build(&reader));

    auto file_schema = reader->parquet_reader()->metadata()->schema();
    std::vector<int> columns;
    for (const char *name : {"hash_lo", "hash_hi", "count"}) {
        int column_index = file_schema->ColumnIndex(name);
        if (column_index < 0) throw std::runtime_error(path.string() + ": missing column " + name);
        columns.push_back(column_index);
    }

    std::shared_ptr<arrow::Table> table;
    CHECK_ARROW(reader->ReadTable(columns, &table));
    ASSIGN_OR_THROW(table, table->CombineChunks());

    auto get_u64 = [&](const std::string &name) -> const uint64_t * {
        auto column = table->GetColumnByName(name);
        if (!column || column->num_chunks() != 1) {
            throw std::runtime_error(path.string() + ": invalid column " + name);
        }
        return std::static_pointer_cast<arrow::UInt64Array>(column->chunk(0))->raw_values();
    };

    const uint64_t *hash_lo = get_u64("hash_lo");
    const uint64_t *hash_hi = get_u64("hash_hi");
    const uint64_t *count = get_u64("count");
    std::vector<Record> records(static_cast<std::size_t>(table->num_rows()));
    for (int64_t row = 0; row < table->num_rows(); ++row) {
        records[static_cast<std::size_t>(row)] = {hash_lo[row], hash_hi[row], count[row]};
    }
    std::sort(records.begin(), records.end(), record_less);
    return records;
}

int main(int argc, char **argv) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <expected.parquet> <actual.parquet>\n";
        return 2;
    }

    try {
        std::vector<Record> expected = read_records(argv[1]);
        std::vector<Record> actual = read_records(argv[2]);
        if (expected.size() != actual.size()) {
            std::cerr << "row-count mismatch: expected=" << expected.size()
                      << " actual=" << actual.size() << "\n";
            return 1;
        }
        for (std::size_t index = 0; index < expected.size(); ++index) {
            if (expected[index].lo != actual[index].lo ||
                expected[index].hi != actual[index].hi ||
                expected[index].count != actual[index].count) {
                std::cerr << "record mismatch at sorted index " << index
                          << ": expected=(" << expected[index].hi << ","
                          << expected[index].lo << "," << expected[index].count
                          << ") actual=(" << actual[index].hi << ","
                          << actual[index].lo << "," << actual[index].count << ")\n";
                return 1;
            }
        }
        std::cout << "PASS: matched " << expected.size() << " hash/count records\n";
        return 0;
    } catch (const std::exception &error) {
        std::cerr << "compare failed: " << error.what() << "\n";
        return 1;
    }
}