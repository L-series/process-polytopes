/* rg_count.cpp — print the number of row groups in a parquet file. */
#include <iostream>
#include <parquet/file_reader.h>

int main(int argc, char **argv) {
    if (argc < 2) { std::cerr << "usage: rg_count F.parquet\n"; return 1; }
    auto rd = parquet::ParquetFileReader::OpenFile(argv[1], false);
    std::cout << rd->metadata()->num_row_groups() << "\n";
    return 0;
}
