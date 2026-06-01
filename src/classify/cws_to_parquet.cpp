/**
 * cws_to_parquet.cpp - Convert PALP CWS text rows to combined-CWS Parquet.
 *
 * Input rows are the PALP Print_CWS format:
 *   d_0 w_00 ... w_0N  d_1 w_10 ... w_1N ... [M:p v] [F:f] [N:p v]
 *
 * The converter is intentionally small and strict: the caller supplies nw, N,
 * and structure_id, so each row can be parsed without guessing.
 */

#include <array>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>
#include <parquet/properties.h>

#include "palp_api.h"

namespace fs = std::filesystem;

#define CHECK_ARROW(expr)                                            \
    do {                                                             \
        arrow::Status _s = (expr);                                   \
        if (!_s.ok())                                                \
            throw std::runtime_error(std::string(__FILE__) + ":" +  \
                std::to_string(__LINE__) + " " + _s.ToString());    \
    } while (0)

#define ASSIGN_OR_THROW(lhs, expr)                                   \
    do {                                                             \
        auto _r = (expr);                                            \
        if (!_r.ok())                                                \
            throw std::runtime_error(std::string(__FILE__) + ":" +  \
                std::to_string(__LINE__) + " " + _r.status().ToString()); \
        lhs = std::move(_r).ValueOrDie();                            \
    } while (0)

static constexpr int32_t CWS_SCHEMA_VERSION = 2;

struct ParsedRow {
    int32_t structure_id = 0;
    int32_t profile_id = 0;
    int64_t source_index = 0;
    int32_t nw = 0;
    int32_t N = 0;
    int32_t degree[PALP_API_MAX_CWS] = {0};
    int32_t weight[PALP_API_MAX_CWS][PALP_API_MAX_COORDS] = {{0}};
    int32_t vertex_count = 0;
    int32_t facet_count = 0;
    int32_t point_count = 0;
    int32_t dual_point_count = 0;
    int32_t h11 = 0, h12 = 0, h13 = 0;
};

static int profile_id_for_structure(int structure_id) {
    if (structure_id <= 1) return 1;
    if (structure_id <= 3) return 2;
    if (structure_id <= 7) return 3;
    if (structure_id <= 14) return 4;
    if (structure_id <= 32) return 5;
    if (structure_id <= 45) return 6;
    if (structure_id <= 47) return 7;
    return 0;
}

static bool starts_with(const std::string &s, const std::string &prefix) {
    return s.rfind(prefix, 0) == 0;
}

static int parse_tag_value(const std::string &token, const std::string &tag) {
    if (!starts_with(token, tag))
        throw std::runtime_error("expected tag " + tag + ", got " + token);
    return std::stoi(token.substr(tag.size()));
}

static ParsedRow parse_row(const std::string &line,
                           int32_t structure_id,
                           int32_t profile_id,
                           int32_t nw,
                           int32_t N,
                           int64_t source_index) {
    if (nw < 1 || nw > PALP_API_MAX_CWS || N < 1 || N > PALP_API_MAX_COORDS)
        throw std::runtime_error("nw/N outside supported 5D classification envelope");
    if (N - nw != POLY_Dmax)
        throw std::runtime_error("invalid CWS dimensions: N - nw must equal POLY_Dmax");

    std::istringstream in(line);
    std::vector<std::string> tok;
    std::string t;
    while (in >> t) tok.push_back(t);
    if (tok.empty()) throw std::runtime_error("empty CWS row");

    ParsedRow row;
    row.structure_id = structure_id;
    row.profile_id = profile_id ? profile_id : profile_id_for_structure(structure_id);
    row.source_index = source_index;
    row.nw = nw;
    row.N = N;

    size_t pos = 0;
    for (int r = 0; r < nw; r++) {
        if (pos >= tok.size()) throw std::runtime_error("truncated CWS row before degree");
        row.degree[r] = std::stoi(tok[pos++]);
        int64_t sum = 0;
        for (int c = 0; c < N; c++) {
            if (pos >= tok.size()) throw std::runtime_error("truncated CWS row before weights");
            if (tok[pos].find(':') != std::string::npos)
                throw std::runtime_error("metadata tag found before all CWS weights were read");
            row.weight[r][c] = std::stoi(tok[pos++]);
            if (row.weight[r][c] < 0) throw std::runtime_error("negative CWS weight");
            sum += row.weight[r][c];
        }
        if (row.degree[r] <= 0 || sum != row.degree[r])
            throw std::runtime_error("degree does not match embedded row weight sum");
    }

    while (pos < tok.size()) {
        const std::string &tag = tok[pos++];
        if (starts_with(tag, "M:")) {
            row.point_count = parse_tag_value(tag, "M:");
            if (pos < tok.size() && tok[pos].find(':') == std::string::npos)
                row.vertex_count = std::stoi(tok[pos++]);
        } else if (starts_with(tag, "F:")) {
            row.facet_count = parse_tag_value(tag, "F:");
        } else if (starts_with(tag, "N:")) {
            row.dual_point_count = parse_tag_value(tag, "N:");
            if (pos < tok.size() && tok[pos].find(':') == std::string::npos)
                pos++;
        } else if (starts_with(tag, "h11:")) {
            row.h11 = parse_tag_value(tag, "h11:");
        } else if (starts_with(tag, "h12:")) {
            row.h12 = parse_tag_value(tag, "h12:");
        } else if (starts_with(tag, "h13:")) {
            row.h13 = parse_tag_value(tag, "h13:");
        }
    }

    return row;
}

static void usage(const char *argv0) {
    std::cerr
        << "Usage: " << argv0 << " --input <txt|-> --output <parquet>"
        << " --structure-id <id> --nw <n> --N <ambient> [--profile-id <id>]\n";
}

int main(int argc, char **argv) {
    std::string input_path = "-", output_path;
    int32_t structure_id = 0, profile_id = 0, nw = 0, N = 0;

    for (int i = 1; i < argc; i++) {
        std::string a = argv[i];
        if      (a == "--input"        && i + 1 < argc) input_path = argv[++i];
        else if (a == "--output"       && i + 1 < argc) output_path = argv[++i];
        else if (a == "--structure-id" && i + 1 < argc) structure_id = std::stoi(argv[++i]);
        else if (a == "--profile-id"   && i + 1 < argc) profile_id = std::stoi(argv[++i]);
        else if (a == "--nw"           && i + 1 < argc) nw = std::stoi(argv[++i]);
        else if (a == "--N"            && i + 1 < argc) N = std::stoi(argv[++i]);
        else if (a == "-h" || a == "--help") { usage(argv[0]); return 0; }
        else { std::cerr << "Unknown option: " << a << "\n"; usage(argv[0]); return 1; }
    }

    if (output_path.empty() || structure_id <= 0 || nw <= 0 || N <= 0) {
        usage(argv[0]);
        return 1;
    }

    std::ifstream file_in;
    std::istream *input = &std::cin;
    if (input_path != "-") {
        file_in.open(input_path);
        if (!file_in) throw std::runtime_error("Cannot open input: " + input_path);
        input = &file_in;
    }

    std::vector<ParsedRow> rows;
    std::string line;
    int64_t source_index = 0;
    while (std::getline(*input, line)) {
        if (line.empty() || line[0] == '#') continue;
        rows.push_back(parse_row(line, structure_id, profile_id, nw, N, source_index++));
    }
    if (rows.empty()) throw std::runtime_error("No CWS rows parsed from input");

    std::vector<std::shared_ptr<arrow::Field>> fields = {
        arrow::field("cws_schema_version", arrow::int32()),
        arrow::field("structure_id", arrow::int32()),
        arrow::field("profile_id", arrow::int32()),
        arrow::field("source_index", arrow::int64()),
        arrow::field("nw", arrow::int32()),
        arrow::field("N", arrow::int32()),
    };
    for (int r = 0; r < PALP_API_MAX_CWS; r++)
        fields.push_back(arrow::field("degree" + std::to_string(r), arrow::int32()));
    for (int r = 0; r < PALP_API_MAX_CWS; r++)
        for (int c = 0; c < PALP_API_MAX_COORDS; c++)
            fields.push_back(arrow::field("weight" + std::to_string(r) + "_" +
                                          std::to_string(c), arrow::int32()));
    fields.insert(fields.end(), {
        arrow::field("vertex_count", arrow::int32()),
        arrow::field("facet_count", arrow::int32()),
        arrow::field("point_count", arrow::int32()),
        arrow::field("dual_point_count", arrow::int32()),
        arrow::field("h11", arrow::int32()),
        arrow::field("h12", arrow::int32()),
        arrow::field("h13", arrow::int32()),
    });
    auto schema = arrow::schema(fields);

    arrow::Int32Builder schema_b, sid_b, pid_b, nw_b, N_b;
    arrow::Int64Builder source_b;
    std::array<arrow::Int32Builder, PALP_API_MAX_CWS> degree_b;
    std::array<std::array<arrow::Int32Builder, PALP_API_MAX_COORDS>, PALP_API_MAX_CWS> weight_b;
    arrow::Int32Builder vc_b, fc_b, pc_b, dpc_b, h11_b, h12_b, h13_b;

    for (const ParsedRow &row : rows) {
        CHECK_ARROW(schema_b.Append(CWS_SCHEMA_VERSION));
        CHECK_ARROW(sid_b.Append(row.structure_id));
        CHECK_ARROW(pid_b.Append(row.profile_id));
        CHECK_ARROW(source_b.Append(row.source_index));
        CHECK_ARROW(nw_b.Append(row.nw));
        CHECK_ARROW(N_b.Append(row.N));
        for (int r = 0; r < PALP_API_MAX_CWS; r++)
            CHECK_ARROW(degree_b[r].Append(row.degree[r]));
        for (int r = 0; r < PALP_API_MAX_CWS; r++)
            for (int c = 0; c < PALP_API_MAX_COORDS; c++)
                CHECK_ARROW(weight_b[r][c].Append(row.weight[r][c]));
        CHECK_ARROW(vc_b.Append(row.vertex_count));
        CHECK_ARROW(fc_b.Append(row.facet_count));
        CHECK_ARROW(pc_b.Append(row.point_count));
        CHECK_ARROW(dpc_b.Append(row.dual_point_count));
        CHECK_ARROW(h11_b.Append(row.h11));
        CHECK_ARROW(h12_b.Append(row.h12));
        CHECK_ARROW(h13_b.Append(row.h13));
    }

    std::vector<std::shared_ptr<arrow::Array>> arrays;
    std::shared_ptr<arrow::Array> arr;
    CHECK_ARROW(schema_b.Finish(&arr)); arrays.push_back(arr);
    CHECK_ARROW(sid_b.Finish(&arr)); arrays.push_back(arr);
    CHECK_ARROW(pid_b.Finish(&arr)); arrays.push_back(arr);
    CHECK_ARROW(source_b.Finish(&arr)); arrays.push_back(arr);
    CHECK_ARROW(nw_b.Finish(&arr)); arrays.push_back(arr);
    CHECK_ARROW(N_b.Finish(&arr)); arrays.push_back(arr);
    for (int r = 0; r < PALP_API_MAX_CWS; r++) {
        CHECK_ARROW(degree_b[r].Finish(&arr)); arrays.push_back(arr);
    }
    for (int r = 0; r < PALP_API_MAX_CWS; r++)
        for (int c = 0; c < PALP_API_MAX_COORDS; c++) {
            CHECK_ARROW(weight_b[r][c].Finish(&arr)); arrays.push_back(arr);
        }
    CHECK_ARROW(vc_b.Finish(&arr)); arrays.push_back(arr);
    CHECK_ARROW(fc_b.Finish(&arr)); arrays.push_back(arr);
    CHECK_ARROW(pc_b.Finish(&arr)); arrays.push_back(arr);
    CHECK_ARROW(dpc_b.Finish(&arr)); arrays.push_back(arr);
    CHECK_ARROW(h11_b.Finish(&arr)); arrays.push_back(arr);
    CHECK_ARROW(h12_b.Finish(&arr)); arrays.push_back(arr);
    CHECK_ARROW(h13_b.Finish(&arr)); arrays.push_back(arr);

    auto table = arrow::Table::Make(schema, arrays);
    fs::path parent = fs::path(output_path).parent_path();
    if (!parent.empty()) fs::create_directories(parent);
    std::shared_ptr<arrow::io::FileOutputStream> out;
    ASSIGN_OR_THROW(out, arrow::io::FileOutputStream::Open(output_path));
    auto props = parquet::WriterProperties::Builder()
        .compression(parquet::Compression::ZSTD)
        ->max_row_group_length(1024 * 1024)
        ->build();
    CHECK_ARROW(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(),
                                           out, 1024 * 1024, props));
    CHECK_ARROW(out->Close());

    std::cerr << "Wrote " << rows.size() << " combined CWS rows to "
              << output_path << "\n";
    return 0;
}
