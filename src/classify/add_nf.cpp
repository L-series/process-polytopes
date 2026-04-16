/**
 * add_nf.cpp — Augment unique_polytopes.parquet with PALP normal form matrices
 * ═══════════════════════════════════════════════════════════════════════════
 *
 * Reads the output of classifier (unique_polytopes.parquet), re-runs PALP
 * for each unique polytope using its stored first_weight0..5, and appends
 * the NF vertex matrix as a new column to produce an enriched parquet.
 *
 * Normal-form representation
 * ──────────────────────────
 * Column  "nf_vertices"  LargeBinary
 *   Each entry holds a row-major int32_t matrix of shape [5 × nv]:
 *     [ nf[0][0], nf[0][1], …, nf[0][nv-1],   ← x-coords of all vertices
 *       nf[1][0], …, nf[1][nv-1],              ← y-coords …
 *       …
 *       nf[4][0], …, nf[4][nv-1] ]             ← 5th-dim coords …
 *   Byte length = 5 * vertex_count * 4
 *
 * Python reconstruction:
 *   import numpy as np, pyarrow.parquet as pq
 *   t  = pq.read_table("enriched.parquet")
 *   nv = t["vertex_count"][i].as_py()
 *   m  = np.frombuffer(t["nf_vertices"][i].as_py(), dtype=np.int32).reshape(5, nv)
 *   # m[:, j] is the j-th vertex in the canonical basis
 *
 * Why int32 and not int16 or int64?
 *   PALP's Long is a C long (64-bit on Linux), but for 5D reflexive polytopes
 *   from weight systems the NF entries fit comfortably in int32.  (int16 was
 *   tried but observed values up to ~34K, which exceeds int16's ±32767 range.
 *   int64 / Long would double the storage unnecessarily for these polytopes.)
 *   A range-check option exists to verify int32 sufficiency; pass --no-range-check
 *   to skip it.
 *
 * Python reconstruction:
 *   import numpy as np, pyarrow.parquet as pq
 *   t  = pq.read_table('enriched.parquet')
 *   nv = t['vertex_count'][i].as_py()
 *   m  = np.frombuffer(t['nf_vertices'][i].as_py(), dtype=np.int32).reshape(5, nv)
 *   # m[:, j] is the j-th vertex in the canonical basis
 *
 * Build: add add_nf to CMakeLists.txt (same flags as classifier)
 * Usage: ./add_nf --input unique_polytopes.parquet --output enriched.parquet
 *                [--threads N] [--no-range-check]
 *
 * NOTE on representation choice
 * ──────────────────────────────
 * The NF vertex matrix is the canonical geometric representative for a
 * reflexive polytope up to GL(n,ℤ) equivalence.  An alternative sometimes
 * used in the literature is to store the *dual* polytope's vertex matrix
 * (the Newton polytope of the CY), which is related by the PALP dual
 * computation.  For most downstream physics/maths analyses the primal NF is
 * the conventional choice and is what PALP exports directly.
 * ═══════════════════════════════════════════════════════════════════════════
 */

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <future>
#include <iomanip>
#include <iostream>
#include <limits>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/properties.h>

#define XXH_INLINE_ALL
#define XXH_STATIC_LINKING_ONLY
#include "xxhash.h"

#include "palp_api.h"

namespace fs = std::filesystem;

/* ═══════════════════════════════════════════════════════════════════════════
 *  Arrow helper macros (shared with classifier.cpp style)
 * ═══════════════════════════════════════════════════════════════════════════ */

#define CHECK_ARROW(expr)                                              \
    do {                                                               \
        arrow::Status _s = (expr);                                     \
        if (!_s.ok())                                                  \
            throw std::runtime_error(std::string(__FILE__) + ":"       \
                + std::to_string(__LINE__) + " " + _s.ToString());     \
    } while (0)

#define ASSIGN_OR_THROW(lhs, expr)                                     \
    do {                                                               \
        auto _r = (expr);                                              \
        if (!_r.ok())                                                  \
            throw std::runtime_error(std::string(__FILE__) + ":"       \
                + std::to_string(__LINE__) + " "                       \
                + _r.status().ToString());                             \
        lhs = std::move(_r).ValueOrDie();                              \
    } while (0)

/* ═══════════════════════════════════════════════════════════════════════════
 *  xxHash helpers — for hash verification
 * ═══════════════════════════════════════════════════════════════════════════ */

struct Hash128 { uint64_t lo, hi; };

static Hash128 hash_normal_form(const Long nf[POLY_Dmax][VERT_Nmax],
                                int dim, int nv)
{
    Long buf[POLY_Dmax * VERT_Nmax];
    int k = 0;
    for (int i = 0; i < dim; i++)
        for (int j = 0; j < nv; j++)
            buf[k++] = nf[i][j];
    XXH128_hash_t h = XXH3_128bits(buf, k * sizeof(Long));
    return {h.low64, h.high64};
}

/* ═══════════════════════════════════════════════════════════════════════════
 *  Per-row input read from parquet
 * ═══════════════════════════════════════════════════════════════════════════ */

struct InputRow {
    int32_t  weights[6];
    int16_t  vertex_count;
    uint64_t hash_lo, hash_hi;   /* for verification */
};

/* ═══════════════════════════════════════════════════════════════════════════
 *  Simple thread pool (identical to classifier.cpp)
 * ═══════════════════════════════════════════════════════════════════════════ */

class ThreadPool {
public:
    explicit ThreadPool(int n) {
        for (int i = 0; i < n; i++)
            workers_.emplace_back([this] { run(); });
    }
    ~ThreadPool() {
        { std::unique_lock<std::mutex> lk(mtx_); stop_ = true; }
        cv_.notify_all();
        for (auto &w : workers_) w.join();
    }
    template <class F>
    std::future<void> enqueue(F &&f) {
        auto task = std::make_shared<std::packaged_task<void()>>(
                        std::forward<F>(f));
        auto fut = task->get_future();
        { std::unique_lock<std::mutex> lk(mtx_);
          queue_.emplace([task] { (*task)(); }); }
        cv_.notify_one();
        return fut;
    }
private:
    void run() {
        for (;;) {
            std::function<void()> job;
            { std::unique_lock<std::mutex> lk(mtx_);
              cv_.wait(lk, [this] { return stop_ || !queue_.empty(); });
              if (stop_ && queue_.empty()) return;
              job = std::move(queue_.front()); queue_.pop(); }
            job();
        }
    }
    std::vector<std::thread>          workers_;
    std::queue<std::function<void()>> queue_;
    std::mutex                        mtx_;
    std::condition_variable           cv_;
    bool                              stop_{false};
};

/* ═══════════════════════════════════════════════════════════════════════════
 *  Read input parquet
 * ═══════════════════════════════════════════════════════════════════════════ */

static std::shared_ptr<arrow::Table> read_table(const std::string &path)
{
    std::shared_ptr<arrow::io::ReadableFile> infile;
    ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(path));

    std::unique_ptr<parquet::arrow::FileReader> reader;
    {
        parquet::arrow::FileReaderBuilder builder;
        CHECK_ARROW(builder.Open(infile));
        CHECK_ARROW(builder.Build(&reader));
    }

    std::shared_ptr<arrow::Table> table;
    CHECK_ARROW(reader->ReadTable(&table));

    /* Combine multi-chunk columns so raw_values() pointers are valid */
    ASSIGN_OR_THROW(table, table->CombineChunks());
    return table;
}

/* Extract per-row input from the combined table */
static std::vector<InputRow> extract_input_rows(
        const std::shared_ptr<arrow::Table> &table)
{
    int64_t n = table->num_rows();
    std::vector<InputRow> rows(n);

    auto get_i32 = [&](const std::string &col) -> const int32_t * {
        auto c = table->GetColumnByName(col);
        if (!c || c->num_chunks() == 0) return nullptr;
        return std::static_pointer_cast<arrow::Int32Array>(
                   c->chunk(0))->raw_values();
    };
    auto get_i16 = [&](const std::string &col) -> const int16_t * {
        auto c = table->GetColumnByName(col);
        if (!c || c->num_chunks() == 0) return nullptr;
        return std::static_pointer_cast<arrow::Int16Array>(
                   c->chunk(0))->raw_values();
    };
    auto get_u64 = [&](const std::string &col) -> const uint64_t * {
        auto c = table->GetColumnByName(col);
        if (!c || c->num_chunks() == 0) return nullptr;
        return std::static_pointer_cast<arrow::UInt64Array>(
                   c->chunk(0))->raw_values();
    };

    const int32_t  *w0  = get_i32("first_weight0");
    const int32_t  *w1  = get_i32("first_weight1");
    const int32_t  *w2  = get_i32("first_weight2");
    const int32_t  *w3  = get_i32("first_weight3");
    const int32_t  *w4  = get_i32("first_weight4");
    const int32_t  *w5  = get_i32("first_weight5");
    const int16_t  *vc  = get_i16("vertex_count");
    const uint64_t *hlo = get_u64("hash_lo");
    const uint64_t *hhi = get_u64("hash_hi");

    if (!w0 || !w1 || !w2 || !w3 || !w4 || !w5 || !vc)
        throw std::runtime_error(
            "Input parquet missing required weight / vertex_count columns");

    for (int64_t i = 0; i < n; i++) {
        rows[i].weights[0]   = w0[i];
        rows[i].weights[1]   = w1[i];
        rows[i].weights[2]   = w2[i];
        rows[i].weights[3]   = w3[i];
        rows[i].weights[4]   = w4[i];
        rows[i].weights[5]   = w5[i];
        rows[i].vertex_count = vc[i];
        rows[i].hash_lo      = hlo ? hlo[i] : 0;
        rows[i].hash_hi      = hhi ? hhi[i] : 0;
    }
    return rows;
}

/* ═══════════════════════════════════════════════════════════════════════════
 *  Compute NF for all rows (multi-threaded)
 *
 *  Each element of nf_data[i] holds the int16 flat representation of the
 *  NF matrix for row i:  nf[dim][nv] stored row-major (dim varies slowest).
 *  If PALP fails for a row the vector is left empty and the error counter
 *  is incremented.
 * ═══════════════════════════════════════════════════════════════════════════ */

static std::vector<std::vector<int32_t>>
compute_all_nf(const std::vector<InputRow> &rows,
               int n_threads,
               bool range_check,
               bool verify_hash,
               std::atomic<int64_t> &n_failed,
               std::atomic<int64_t> &n_mismatch)
{
    int64_t n = static_cast<int64_t>(rows.size());
    std::vector<std::vector<int32_t>> nf_data(n);

    constexpr int64_t BLOCK = 4096;
    int64_t n_blocks = (n + BLOCK - 1) / BLOCK;
    std::atomic<int64_t> next_block{0};

    /* Range-check violations are reported via this flag + mutex */
    std::atomic<bool> range_error{false};
    std::mutex        range_error_mtx;
    std::string       range_error_msg;

    std::atomic<int64_t> done{0};

    auto worker = [&]() {
        PalpWorkspace *ws = palp_workspace_alloc();
        if (!ws) { std::cerr << "Failed to alloc PALP workspace\n"; return; }

        PalpNFResult result;
        bool first_block = true;  /* first block this thread processes */
        bool aborted     = false;

        for (;;) {
            if (aborted || range_error.load(std::memory_order_relaxed)) break;

            int64_t b = next_block.fetch_add(1, std::memory_order_relaxed);
            if (b >= n_blocks) break;

            int64_t bstart = b * BLOCK;
            int64_t bend   = std::min(bstart + BLOCK, n);

            for (int64_t i = bstart; i < bend && !aborted; i++) {
                if (range_error.load(std::memory_order_relaxed)) { aborted = true; break; }

                const InputRow &row = rows[i];
                palp_compute_nf(ws, row.weights, &result);

                if (!result.ok) {
                    n_failed.fetch_add(1, std::memory_order_relaxed);
                    /* leave nf_data[i] empty */
                    done.fetch_add(1, std::memory_order_relaxed);
                    continue;
                }

                int dim = result.dim;  /* should always be 5 */
                int nv  = result.nv;

                /* Optional: verify hash of recomputed NF against stored hash */
                if (verify_hash && row.hash_lo != 0) {
                    Hash128 h = hash_normal_form(result.nf, dim, nv);
                    if (h.lo != row.hash_lo || h.hi != row.hash_hi)
                        n_mismatch.fetch_add(1, std::memory_order_relaxed);
                }

                /* Pack NF into int32 flat vector: nf[coord][vertex] row-major */
                std::vector<int32_t> flat(static_cast<size_t>(dim * nv));
                int k = 0;
                bool overflow = false;
                for (int ci = 0; ci < dim && !overflow; ci++) {
                    for (int vi = 0; vi < nv && !overflow; vi++) {
                        Long val = result.nf[ci][vi];

                        /* Range check (int32 overflow would be extraordinary) */
                        if (range_check && first_block) {
                            if (val < std::numeric_limits<int32_t>::min() ||
                                val > std::numeric_limits<int32_t>::max())
                            {
                                std::unique_lock<std::mutex> lk(range_error_mtx);
                                range_error_msg =
                                    "NF entry " + std::to_string(val) +
                                    " overflows int32 at row " +
                                    std::to_string(i) + " coord " +
                                    std::to_string(ci) + " vertex " +
                                    std::to_string(vi) +
                                    ". Run with --no-range-check to skip";
                                range_error.store(true);
                                overflow = true;
                                aborted  = true;
                            }
                        }

                        if (!overflow) flat[k++] = static_cast<int32_t>(val);
                    }
                }
                if (!overflow) {
                    first_block = false;
                    nf_data[i] = std::move(flat);
                }
                done.fetch_add(1, std::memory_order_relaxed);
            }
        }
        palp_workspace_free(ws);
    };

    ThreadPool pool(n_threads);
    std::vector<std::future<void>> futures;
    futures.reserve(n_threads);
    for (int t = 0; t < n_threads; t++)
        futures.push_back(pool.enqueue(worker));

    /* Progress reporting */
    auto t0 = std::chrono::steady_clock::now();
    bool all_done = false;
    while (!all_done) {
        all_done = true;
        for (auto &f : futures)
            if (f.wait_for(std::chrono::milliseconds(500))
                    != std::future_status::ready)
                all_done = false;

        int64_t d = done.load();
        double  elapsed = std::chrono::duration<double>(
                              std::chrono::steady_clock::now() - t0).count();
        double  rate    = d / (elapsed > 0 ? elapsed : 1.0);
        double  eta     = (n - d) / (rate > 0 ? rate : 1.0);
        std::cerr << "\r  " << d << " / " << n
                  << "  (" << std::fixed << std::setprecision(0)
                  << rate / 1000.0 << " K/s"
                  << "  ETA " << (int)(eta / 60) << "m"
                  << std::setprecision(0)
                  << ")   fail=" << n_failed.load()
                  << "         " << std::flush;
    }
    std::cerr << "\n";
    for (auto &f : futures) f.get();  /* propagate exceptions */

    if (range_error.load()) {
        std::lock_guard<std::mutex> lk(range_error_mtx);
        throw std::runtime_error(range_error_msg);
    }

    return nf_data;
}

/* ═══════════════════════════════════════════════════════════════════════════
 *  Build the Arrow LargeBinary column from computed NF data
 * ═══════════════════════════════════════════════════════════════════════════ */

static std::shared_ptr<arrow::Array>
build_nf_column(const std::vector<std::vector<int32_t>> &nf_data)
{
    arrow::LargeBinaryBuilder builder;
    CHECK_ARROW(builder.Reserve(static_cast<int64_t>(nf_data.size())));

    for (const auto &flat : nf_data) {
        if (flat.empty()) {
            CHECK_ARROW(builder.AppendNull());
        } else {
            CHECK_ARROW(builder.Append(
                reinterpret_cast<const uint8_t *>(flat.data()),
                static_cast<int64_t>(flat.size() * sizeof(int16_t))));
        }
    }

    std::shared_ptr<arrow::Array> arr;
    CHECK_ARROW(builder.Finish(&arr));
    return arr;
}

/* ═══════════════════════════════════════════════════════════════════════════
 *  Write output parquet (existing table + nf_vertices column)
 * ═══════════════════════════════════════════════════════════════════════════ */

static void write_output(const std::shared_ptr<arrow::Table> &table,
                         std::shared_ptr<arrow::Array>        nf_arr,
                         const std::string                   &out_path)
{
    /* Append nf_vertices column to the existing table */
    std::shared_ptr<arrow::Table> enriched;
    ASSIGN_OR_THROW(enriched, table->AddColumn(
        table->num_columns(),
        arrow::field("nf_vertices", arrow::large_binary()),
        std::make_shared<arrow::ChunkedArray>(nf_arr)));

    std::shared_ptr<arrow::io::FileOutputStream> out;
    ASSIGN_OR_THROW(out, arrow::io::FileOutputStream::Open(out_path));

    auto props = parquet::WriterProperties::Builder()
        .compression(parquet::Compression::ZSTD)
        ->max_row_group_length(1 << 20)
        ->build();

    CHECK_ARROW(parquet::arrow::WriteTable(
        *enriched, arrow::default_memory_pool(),
        out, 1 << 20, props));

    CHECK_ARROW(out->Close());
}

/* ═══════════════════════════════════════════════════════════════════════════
 *  CLI
 * ═══════════════════════════════════════════════════════════════════════════ */

static void usage(const char *argv0) {
    std::cerr
        << "Usage: " << argv0 << "\n"
        << "  --input  <path>   unique_polytopes.parquet (from classifier)\n"
        << "  --output <path>   enriched output parquet path\n"
        << " [--threads <n>]   worker threads (default: hardware_concurrency)\n"
        << " [--no-range-check] skip int16 overflow check on NF entries\n"
        << " [--verify-hash]    verify recomputed NF hash matches stored hash\n"
        << "\n"
        << "Output adds one new column to the existing schema:\n"
        << "  nf_vertices  LargeBinary — row-major int16 matrix [5 × vertex_count]\n"
        << "\n"
        << "Python usage:\n"
        << "  import numpy as np, pyarrow.parquet as pq\n"
        << "  t  = pq.read_table('enriched.parquet')\n"
        << "  nv = t['vertex_count'][i].as_py()\n"
        << "  m  = np.frombuffer(t['nf_vertices'][i].as_py(), dtype=np.int32)"
           ".reshape(5, nv)\n";
}

int main(int argc, char **argv)
{
    std::string input_path, output_path;
    int  n_threads    = 0;
    bool range_check  = true;
    bool verify_hash  = false;

    for (int i = 1; i < argc; i++) {
        std::string a = argv[i];
        if      (a == "--input"          && i+1 < argc) input_path  = argv[++i];
        else if (a == "--output"         && i+1 < argc) output_path = argv[++i];
        else if (a == "--threads"        && i+1 < argc) n_threads   = std::stoi(argv[++i]);
        else if (a == "--no-range-check")               range_check = false;
        else if (a == "--verify-hash")                  verify_hash = true;
        else if (a == "-h" || a == "--help") { usage(argv[0]); return 0; }
        else { std::cerr << "Unknown option: " << a << "\n"; usage(argv[0]); return 1; }
    }

    if (input_path.empty() || output_path.empty()) {
        usage(argv[0]);
        return 1;
    }

    if (n_threads <= 0)
        n_threads = static_cast<int>(std::thread::hardware_concurrency());

    /* ── Init PALP ─────────────────────────────────────────────────────── */
    palp_init();

    std::cerr << "=== add_nf: Enriching polytope parquet with NF matrices ===\n"
              << "  Input:   " << input_path  << "\n"
              << "  Output:  " << output_path << "\n"
              << "  Threads: " << n_threads   << "\n\n";

    /* ── Read input ────────────────────────────────────────────────────── */
    std::cerr << "Reading input parquet…\n";
    auto t0 = std::chrono::steady_clock::now();
    auto table = read_table(input_path);
    double read_s = std::chrono::duration<double>(
                        std::chrono::steady_clock::now() - t0).count();
    std::cerr << "  " << table->num_rows() << " rows read in "
              << std::fixed << std::setprecision(1) << read_s << "s\n"
              << "  Schema: " << table->schema()->ToString() << "\n\n";

    auto rows = extract_input_rows(table);

    /* ── Compute NF ────────────────────────────────────────────────────── */
    std::cerr << "Computing NF matrices (" << n_threads << " threads)…\n";
    std::atomic<int64_t> n_failed{0}, n_mismatch{0};
    t0 = std::chrono::steady_clock::now();

    auto nf_data = compute_all_nf(rows, n_threads, range_check,
                                  verify_hash, n_failed, n_mismatch);

    double nf_s = std::chrono::duration<double>(
                      std::chrono::steady_clock::now() - t0).count();
    std::cerr << "  Done in " << std::fixed << std::setprecision(1) << nf_s << "s"
              << "  (failed=" << n_failed.load() << ")";
    if (verify_hash)
        std::cerr << "  (hash mismatches=" << n_mismatch.load() << ")";
    std::cerr << "\n\n";

    if (n_mismatch.load() > 0) {
        std::cerr << "WARNING: " << n_mismatch.load()
                  << " hash mismatches detected — the stored first_weights\n"
                  << "  may not reproduce the exact same NF as the original run.\n";
    }

    /* ── Build Arrow column ────────────────────────────────────────────── */
    std::cerr << "Building nf_vertices Arrow column…\n";
    auto nf_arr = build_nf_column(nf_data);
    nf_data.clear();  /* free memory before writing */
    nf_data.shrink_to_fit();

    /* ── Write output ──────────────────────────────────────────────────── */
    std::cerr << "Writing enriched parquet to " << output_path << "…\n";
    t0 = std::chrono::steady_clock::now();
    write_output(table, nf_arr, output_path);
    double write_s = std::chrono::duration<double>(
                         std::chrono::steady_clock::now() - t0).count();
    std::cerr << "  Done in " << std::fixed << std::setprecision(1)
              << write_s << "s\n";

    /* ── Summary ───────────────────────────────────────────────────────── */
    {
        uintmax_t out_bytes = fs::file_size(output_path);
        uintmax_t in_bytes  = fs::file_size(input_path);
        std::cerr << "\nSummary:\n"
                  << "  Rows processed:  " << table->num_rows()    << "\n"
                  << "  PALP failures:   " << n_failed.load()       << "\n"
                  << "  Input size:      " << in_bytes  / (1024*1024) << " MB\n"
                  << "  Output size:     " << out_bytes / (1024*1024) << " MB\n"
                  << "  Size increase:   "
                  << (out_bytes - in_bytes) / (1024*1024) << " MB\n";
    }

    return 0;
}
