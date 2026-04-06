/**
 * classifier.cpp — Main polytope classification engine
 * ═══════════════════════════════════════════════════════════════════════════
 *
 * Reads CWS from Parquet files, computes polytope normal forms via PALP,
 * deduplicates using xxHash128 + hash map, and writes unique polytopes
 * with frequency counts to an output database.
 *
 * Architecture:
 *   ┌──────────┐    ┌──────────────┐    ┌─────────────┐    ┌──────────┐
 *   │ Parquet  │───>│ Thread Pool  │───>│  Hash Map   │───>│  Output  │
 *   │  Reader  │    │ (PALP NF)    │    │  (dedup)    │    │ Parquet  │
 *   └──────────┘    └──────────────┘    └─────────────┘    └──────────┘
 *
 * Build: see CMakeLists.txt
 * Usage: ./classifier --input <dir> --output <dir> [--threads N]
 * ═══════════════════════════════════════════════════════════════════════════
 */

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <future>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <numeric>
#include <queue>
#include <thread>
#include <unordered_map>
#include <vector>

#include <unistd.h>  /* sysconf, _SC_PAGESIZE */

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/properties.h>

/* ── xxHash — bundled single-header implementation ───────────────────────── */
#define XXH_INLINE_ALL
#define XXH_STATIC_LINKING_ONLY
#include "xxhash.h"

/* ── PALP C API ──────────────────────────────────────────────────────────── */
#include "palp_api.h"

namespace fs = std::filesystem;

/* ═══════════════════════════════════════════════════════════════════════════
 *  Configuration
 * ═══════════════════════════════════════════════════════════════════════════ */

struct Config {
    std::string input_dir;
    std::string output_dir;
    std::string checkpoint_dir;
    int         n_threads       = 0;   /* 0 = hardware_concurrency */
    int64_t     batch_size      = 8192;
    int64_t     checkpoint_rows = 50'000'000;  /* rows between checkpoints  */
    int         start_file      = -1;  /* -1 = all files            */
    int         end_file        = -1;
    bool        resume          = false;
    bool        benchmark_only  = false;
    int64_t     benchmark_rows  = 0;   /* 0 = all rows in first file */
    int64_t     max_rows_per_file = 0; /* 0 = unlimited               */
};

/* ═══════════════════════════════════════════════════════════════════════════
 *  Hash map key: 128-bit xxHash of the normal form
 * ═══════════════════════════════════════════════════════════════════════════ */

struct Hash128 {
    uint64_t lo, hi;

    bool operator==(const Hash128 &o) const {
        return lo == o.lo && hi == o.hi;
    }
};

struct Hash128Hasher {
    size_t operator()(const Hash128 &h) const {
        /* Use the low 64 bits as the bucket hash.  xxHash128 is already
           well-distributed so no further mixing is needed. */
        return static_cast<size_t>(h.lo);
    }
};

/* ═══════════════════════════════════════════════════════════════════════════
 *  Value stored for each unique polytope in the hash map
 * ═══════════════════════════════════════════════════════════════════════════ */

struct PolytopeInfo {
    uint64_t count;               /* how many CWS generate this polytope    */
    int32_t  first_weights[6];    /* weights of the first CWS encountered   */
    int16_t  vertex_count;
    int16_t  facet_count;
    int32_t  point_count;
    int32_t  dual_point_count;
    int16_t  h11, h12, h13;
};
/* sizeof ≈ 8 + 24 + 2 + 2 + 4 + 4 + 6 = 50 bytes */

using PolytopeMap = std::unordered_map<Hash128, PolytopeInfo, Hash128Hasher>;

/* ═══════════════════════════════════════════════════════════════════════════
 *  Flat record for sort-merge operations (matches checkpoint on-disk layout)
 * ═══════════════════════════════════════════════════════════════════════════ */

struct MergeRecord {
    Hash128      key;
    PolytopeInfo info;
};
/* Hash128 ends at offset 16 (8-aligned), PolytopeInfo starts with uint64_t
   (needs 8-alignment), so there is no inter-member padding.  Verify: */
static_assert(sizeof(MergeRecord) == sizeof(Hash128) + sizeof(PolytopeInfo),
              "MergeRecord must have no padding (matches checkpoint I/O format)");

static bool key_less(const Hash128 &a, const Hash128 &b) {
    if (a.hi != b.hi) return a.hi < b.hi;
    return a.lo < b.lo;
}

/* ═══════════════════════════════════════════════════════════════════════════
 *  Compute xxHash128 of a normal form matrix
 * ═══════════════════════════════════════════════════════════════════════════ */

static Hash128 hash_normal_form(const Long nf[POLY_Dmax][VERT_Nmax],
                                int dim, int nv) {
    /* Hash only the used portion: dim rows × nv columns.
       We lay out rows contiguously for the hash. */
    /* Maximum size: 5 * 64 * 8 = 2560 bytes — always fits on stack */
    Long buf[POLY_Dmax * VERT_Nmax];
    int k = 0;
    for (int i = 0; i < dim; i++)
        for (int j = 0; j < nv; j++)
            buf[k++] = nf[i][j];

    XXH128_hash_t h = XXH3_128bits(buf, k * sizeof(Long));
    return {h.low64, h.high64};
}

/* ═══════════════════════════════════════════════════════════════════════════
 *  Arrow / Parquet helpers
 * ═══════════════════════════════════════════════════════════════════════════ */

#define CHECK_ARROW(expr)                                            \
    do {                                                             \
        arrow::Status _s = (expr);                                   \
        if (!_s.ok())                                                \
            throw std::runtime_error(std::string(__FILE__) + ":" +   \
                std::to_string(__LINE__) + " " + _s.ToString());     \
    } while (0)

#define ASSIGN_OR_THROW(lhs, expr)                                   \
    do {                                                             \
        auto _r = (expr);                                            \
        if (!_r.ok())                                                \
            throw std::runtime_error(std::string(__FILE__) + ":" +   \
                std::to_string(__LINE__) + " " + _r.status().ToString()); \
        lhs = std::move(_r).ValueOrDie();                            \
    } while (0)

/* ═══════════════════════════════════════════════════════════════════════════
 *  Thread pool
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
        auto task = std::make_shared<std::packaged_task<void()>>(std::forward<F>(f));
        auto fut = task->get_future();
        { std::unique_lock<std::mutex> lk(mtx_); queue_.emplace([task] { (*task)(); }); }
        cv_.notify_one();
        return fut;
    }
private:
    void run() {
        for (;;) {
            std::function<void()> job;
            {
                std::unique_lock<std::mutex> lk(mtx_);
                cv_.wait(lk, [this] { return stop_ || !queue_.empty(); });
                if (stop_ && queue_.empty()) return;
                job = std::move(queue_.front());
                queue_.pop();
            }
            job();
        }
    }
    std::vector<std::thread>           workers_;
    std::queue<std::function<void()>>  queue_;
    std::mutex                         mtx_;
    std::condition_variable            cv_;
    bool                               stop_{false};
};

/* ═══════════════════════════════════════════════════════════════════════════
 *  Statistics tracking
 * ═══════════════════════════════════════════════════════════════════════════ */

struct Stats {
    std::atomic<int64_t> total_cws{0};
    std::atomic<int64_t> processed_cws{0};
    std::atomic<int64_t> failed_cws{0};
    std::atomic<int64_t> duplicate_cws{0};
    std::atomic<int64_t> unique_polytopes{0};
    std::atomic<int>     files_done{0};
    int                  files_total{0};
    std::chrono::steady_clock::time_point start;

    void print_progress() const {
        auto now = std::chrono::steady_clock::now();
        double elapsed = std::chrono::duration<double>(now - start).count();
        int64_t proc = processed_cws.load();
        double rate = proc / (elapsed > 0 ? elapsed : 1.0);
        double remaining = (total_cws.load() - proc) / (rate > 0 ? rate : 1.0);

        std::cerr << "\r  [" << files_done.load() << "/" << files_total << " files]"
                  << "  " << proc / 1'000'000 << "M / "
                  << total_cws.load() / 1'000'000 << "M CWS"
                  << "  unique: " << unique_polytopes.load()
                  << "  dup: " << duplicate_cws.load() / 1'000'000 << "M"
                  << "  fail: " << failed_cws.load()
                  << "  " << std::fixed << std::setprecision(0)
                  << rate / 1000 << "K/s"
                  << "  ETA " << (int)(remaining / 60) << "m"
                  << "        " << std::flush;
    }
};

/* ═══════════════════════════════════════════════════════════════════════════
 *  Process a batch of CWS rows
 * ═══════════════════════════════════════════════════════════════════════════ */

struct CWSRow {
    int32_t weights[6];
    int32_t vertex_count;
    int32_t facet_count;
    int32_t point_count;
    int32_t dual_point_count;
    int32_t h11, h12, h13;
};

static void process_batch(const std::vector<CWSRow> &rows,
                          PalpWorkspace *ws,
                          PolytopeMap &local_map,
                          Stats &stats)
{
    PalpNFResult result;

    for (const auto &row : rows) {
        palp_compute_nf(ws, row.weights, &result);

        if (!result.ok) {
            stats.failed_cws.fetch_add(1, std::memory_order_relaxed);
            stats.processed_cws.fetch_add(1, std::memory_order_relaxed);
            continue;
        }

        Hash128 key = hash_normal_form(result.nf, result.dim, result.nv);

        auto it = local_map.find(key);
        if (it != local_map.end()) {
            it->second.count++;
            stats.duplicate_cws.fetch_add(1, std::memory_order_relaxed);
        } else {
            PolytopeInfo info{};
            info.count = 1;
            std::memcpy(info.first_weights, row.weights, sizeof(row.weights));
            info.vertex_count     = static_cast<int16_t>(result.nv);
            info.facet_count      = static_cast<int16_t>(result.ne);
            info.point_count      = result.np;
            info.dual_point_count = row.dual_point_count;
            info.h11              = static_cast<int16_t>(row.h11);
            info.h12              = static_cast<int16_t>(row.h12);
            info.h13              = static_cast<int16_t>(row.h13);
            local_map.emplace(key, info);
        }
        stats.processed_cws.fetch_add(1, std::memory_order_relaxed);
    }
}

/* ═══════════════════════════════════════════════════════════════════════════
 *  Merge local map into global map (under lock)
 * ═══════════════════════════════════════════════════════════════════════════ */

static void merge_maps(PolytopeMap &global, PolytopeMap &local,
                       std::mutex &global_mtx, Stats &stats)
{
    std::lock_guard<std::mutex> lk(global_mtx);
    for (auto &[key, info] : local) {
        auto it = global.find(key);
        if (it != global.end()) {
            it->second.count += info.count;
            /* The first occurrence of this key in the batch was counted in
               processed_cws but not as dup (only within-batch duplicates
               2..N were counted by process_batch).  It is a global duplicate,
               so credit it here. */
            stats.duplicate_cws.fetch_add(1, std::memory_order_relaxed);
        } else {
            global.emplace(key, info);
            stats.unique_polytopes.fetch_add(1, std::memory_order_relaxed);
        }
    }
    local.clear();
}

/* ═══════════════════════════════════════════════════════════════════════════
 *  Read a parquet file and extract CWS rows
 * ═══════════════════════════════════════════════════════════════════════════ */

static std::vector<CWSRow> read_parquet_file(const fs::path &path,
                                             int64_t max_rows = 0) {
    std::shared_ptr<arrow::io::ReadableFile> infile;
    ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(path.string()));

    std::unique_ptr<parquet::arrow::FileReader> reader;
    {
        parquet::arrow::FileReaderBuilder builder;
        CHECK_ARROW(builder.Open(infile));
        CHECK_ARROW(builder.Build(&reader));
    }

    /* Read only the columns we need */
    std::vector<int> col_indices;
    auto file_schema = reader->parquet_reader()->metadata()->schema();
    std::vector<std::string> needed = {
        "weight0", "weight1", "weight2", "weight3", "weight4", "weight5",
        "vertex_count", "facet_count", "point_count", "dual_point_count",
        "h11", "h12", "h13"
    };
    for (const auto &name : needed) {
        int idx = file_schema->ColumnIndex(name);
        if (idx >= 0) col_indices.push_back(idx);
    }

    std::shared_ptr<arrow::Table> table;
    CHECK_ARROW(reader->ReadTable(col_indices, &table));

    if (max_rows > 0 && table->num_rows() > max_rows)
        table = table->Slice(0, max_rows);

    /* Flatten multi-chunk columns into single chunks so that raw_values()
       pointers remain valid for the lifetime of `table`.  Multi-chunk columns
       arise when a Parquet file has more than one row group; without this step
       the get_col lambda below would return a raw pointer into a temporary
       combined array that is freed immediately, causing heap corruption. */
    ASSIGN_OR_THROW(table, table->CombineChunks());

    int64_t n = table->num_rows();
    std::vector<CWSRow> rows(n);

    /* Extract columns as flat int32 arrays */
    auto get_col = [&](const std::string &name) -> const int32_t * {
        auto col = table->GetColumnByName(name);
        if (!col || col->num_chunks() == 0) return nullptr;
        return std::static_pointer_cast<arrow::Int32Array>(
                   col->chunk(0))->raw_values();
    };

    const int32_t *w0  = get_col("weight0");
    const int32_t *w1  = get_col("weight1");
    const int32_t *w2  = get_col("weight2");
    const int32_t *w3  = get_col("weight3");
    const int32_t *w4  = get_col("weight4");
    const int32_t *w5  = get_col("weight5");
    const int32_t *vc  = get_col("vertex_count");
    const int32_t *fc  = get_col("facet_count");
    const int32_t *pc  = get_col("point_count");
    const int32_t *dpc = get_col("dual_point_count");
    const int32_t *h11 = get_col("h11");
    const int32_t *h12 = get_col("h12");
    const int32_t *h13 = get_col("h13");

    for (int64_t i = 0; i < n; i++) {
        rows[i].weights[0] = w0 ? w0[i] : 0;
        rows[i].weights[1] = w1 ? w1[i] : 0;
        rows[i].weights[2] = w2 ? w2[i] : 0;
        rows[i].weights[3] = w3 ? w3[i] : 0;
        rows[i].weights[4] = w4 ? w4[i] : 0;
        rows[i].weights[5] = w5 ? w5[i] : 0;
        rows[i].vertex_count     = vc  ? vc[i]  : 0;
        rows[i].facet_count      = fc  ? fc[i]  : 0;
        rows[i].point_count      = pc  ? pc[i]  : 0;
        rows[i].dual_point_count = dpc ? dpc[i] : 0;
        rows[i].h11 = h11 ? h11[i] : 0;
        rows[i].h12 = h12 ? h12[i] : 0;
        rows[i].h13 = h13 ? h13[i] : 0;
    }
    return rows;
}

/* ═══════════════════════════════════════════════════════════════════════════
 *  Write results to output parquet
 * ═══════════════════════════════════════════════════════════════════════════ */

static void write_results(const PolytopeMap &global_map,
                          const fs::path &output_path) {
    /* Build schema */
    auto schema = arrow::schema({
        arrow::field("hash_lo",           arrow::uint64()),
        arrow::field("hash_hi",           arrow::uint64()),
        arrow::field("count",             arrow::uint64()),
        arrow::field("first_weight0",     arrow::int32()),
        arrow::field("first_weight1",     arrow::int32()),
        arrow::field("first_weight2",     arrow::int32()),
        arrow::field("first_weight3",     arrow::int32()),
        arrow::field("first_weight4",     arrow::int32()),
        arrow::field("first_weight5",     arrow::int32()),
        arrow::field("vertex_count",      arrow::int16()),
        arrow::field("facet_count",       arrow::int16()),
        arrow::field("point_count",       arrow::int32()),
        arrow::field("dual_point_count",  arrow::int32()),
        arrow::field("h11",               arrow::int16()),
        arrow::field("h12",               arrow::int16()),
        arrow::field("h13",               arrow::int16()),
    });

    /* Build arrays from the map */
    arrow::UInt64Builder  hash_lo_b, hash_hi_b, count_b;
    arrow::Int32Builder   w0_b, w1_b, w2_b, w3_b, w4_b, w5_b;
    arrow::Int16Builder   vc_b, fc_b, h11_b, h12_b, h13_b;
    arrow::Int32Builder   pc_b, dpc_b;

    int64_t n = static_cast<int64_t>(global_map.size());
    CHECK_ARROW(hash_lo_b.Reserve(n));  CHECK_ARROW(hash_hi_b.Reserve(n));
    CHECK_ARROW(count_b.Reserve(n));
    CHECK_ARROW(w0_b.Reserve(n));  CHECK_ARROW(w1_b.Reserve(n));
    CHECK_ARROW(w2_b.Reserve(n));  CHECK_ARROW(w3_b.Reserve(n));
    CHECK_ARROW(w4_b.Reserve(n));  CHECK_ARROW(w5_b.Reserve(n));
    CHECK_ARROW(vc_b.Reserve(n));  CHECK_ARROW(fc_b.Reserve(n));
    CHECK_ARROW(pc_b.Reserve(n));  CHECK_ARROW(dpc_b.Reserve(n));
    CHECK_ARROW(h11_b.Reserve(n)); CHECK_ARROW(h12_b.Reserve(n));
    CHECK_ARROW(h13_b.Reserve(n));

    for (const auto &[key, info] : global_map) {
        CHECK_ARROW(hash_lo_b.Append(key.lo));
        CHECK_ARROW(hash_hi_b.Append(key.hi));
        CHECK_ARROW(count_b.Append(info.count));
        CHECK_ARROW(w0_b.Append(info.first_weights[0]));
        CHECK_ARROW(w1_b.Append(info.first_weights[1]));
        CHECK_ARROW(w2_b.Append(info.first_weights[2]));
        CHECK_ARROW(w3_b.Append(info.first_weights[3]));
        CHECK_ARROW(w4_b.Append(info.first_weights[4]));
        CHECK_ARROW(w5_b.Append(info.first_weights[5]));
        CHECK_ARROW(vc_b.Append(info.vertex_count));
        CHECK_ARROW(fc_b.Append(info.facet_count));
        CHECK_ARROW(pc_b.Append(info.point_count));
        CHECK_ARROW(dpc_b.Append(info.dual_point_count));
        CHECK_ARROW(h11_b.Append(info.h11));
        CHECK_ARROW(h12_b.Append(info.h12));
        CHECK_ARROW(h13_b.Append(info.h13));
    }

    std::shared_ptr<arrow::Array>
        a_hlo, a_hhi, a_cnt,
        a_w0, a_w1, a_w2, a_w3, a_w4, a_w5,
        a_vc, a_fc, a_pc, a_dpc, a_h11, a_h12, a_h13;

    CHECK_ARROW(hash_lo_b.Finish(&a_hlo)); CHECK_ARROW(hash_hi_b.Finish(&a_hhi));
    CHECK_ARROW(count_b.Finish(&a_cnt));
    CHECK_ARROW(w0_b.Finish(&a_w0));  CHECK_ARROW(w1_b.Finish(&a_w1));
    CHECK_ARROW(w2_b.Finish(&a_w2));  CHECK_ARROW(w3_b.Finish(&a_w3));
    CHECK_ARROW(w4_b.Finish(&a_w4));  CHECK_ARROW(w5_b.Finish(&a_w5));
    CHECK_ARROW(vc_b.Finish(&a_vc));  CHECK_ARROW(fc_b.Finish(&a_fc));
    CHECK_ARROW(pc_b.Finish(&a_pc));  CHECK_ARROW(dpc_b.Finish(&a_dpc));
    CHECK_ARROW(h11_b.Finish(&a_h11)); CHECK_ARROW(h12_b.Finish(&a_h12));
    CHECK_ARROW(h13_b.Finish(&a_h13));

    auto table = arrow::Table::Make(schema, {
        a_hlo, a_hhi, a_cnt,
        a_w0, a_w1, a_w2, a_w3, a_w4, a_w5,
        a_vc, a_fc, a_pc, a_dpc, a_h11, a_h12, a_h13
    });

    /* Write */
    std::shared_ptr<arrow::io::FileOutputStream> out;
    ASSIGN_OR_THROW(out, arrow::io::FileOutputStream::Open(output_path.string()));

    auto writer_props = parquet::WriterProperties::Builder()
        .compression(parquet::Compression::ZSTD)
        ->max_row_group_length(1024 * 1024)
        ->build();

    CHECK_ARROW(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(),
                                           out, 1024 * 1024, writer_props));
    CHECK_ARROW(out->Close());
}

/* ═══════════════════════════════════════════════════════════════════════════
 *  Write a checkpoint (serialise hash map to binary file)
 * ═══════════════════════════════════════════════════════════════════════════ */

static void write_checkpoint(const PolytopeMap &map, const fs::path &path) {
    std::ofstream f(path, std::ios::binary);
    uint64_t n = map.size();
    f.write(reinterpret_cast<const char *>(&n), sizeof(n));
    for (const auto &[key, info] : map) {
        f.write(reinterpret_cast<const char *>(&key), sizeof(key));
        f.write(reinterpret_cast<const char *>(&info), sizeof(info));
    }
    std::cerr << "\n  Checkpoint: " << n << " entries → " << path.string() << "\n";
}

static void read_checkpoint(PolytopeMap &map, const fs::path &path) {
    std::ifstream f(path, std::ios::binary);
    if (!f.is_open()) return;
    uint64_t n;
    f.read(reinterpret_cast<char *>(&n), sizeof(n));
    map.reserve(n);
    for (uint64_t i = 0; i < n; i++) {
        Hash128 key;
        PolytopeInfo info;
        f.read(reinterpret_cast<char *>(&key), sizeof(key));
        f.read(reinterpret_cast<char *>(&info), sizeof(info));
        auto it = map.find(key);
        if (it != map.end())
            it->second.count += info.count;
        else
            map.emplace(key, info);
    }
    std::cerr << "  Loaded checkpoint: " << n << " entries from " << path.string() << "\n";
}

/* ═══════════════════════════════════════════════════════════════════════════
 *  Sort-merge checkpoint merger (memory-efficient for TB-scale data)
 *
 *  Phase 1: Sort each .ckpt file by hash key (parallel across files)
 *  Phase 2: K-way merge with streaming Parquet output
 *
 *  Why this replaces the naive hash-map merge:
 *  - std::unordered_map at billions of entries causes ~100ns/lookup due
 *    to TLB + cache misses on random pointer-chasing through heap nodes.
 *  - Sort-merge uses sequential I/O only → I/O-bound, not cache-bound.
 *  - Phase 1 is embarrassingly parallel across files.
 *  - Phase 2 needs only O(K) memory for merge cursors (K = # files).
 * ═══════════════════════════════════════════════════════════════════════════ */

/* ── Phase 1 helper: sort a single checkpoint file by hash key ─────────── */

static void sort_single_checkpoint(const fs::path &input_path,
                                   const fs::path &sorted_path,
                                   int n_sort_threads,
                                   std::mutex &write_mtx) {
    auto t0 = std::chrono::steady_clock::now();

    std::ifstream f(input_path, std::ios::binary);
    if (!f.is_open())
        throw std::runtime_error("Cannot open: " + input_path.string());

    uint64_t n;
    f.read(reinterpret_cast<char *>(&n), sizeof(n));

    /* Bulk-read all records.  MergeRecord layout matches the on-disk
       format (Hash128 || PolytopeInfo, no padding — verified by static_assert). */
    std::vector<MergeRecord> records(n);
    f.read(reinterpret_cast<char *>(records.data()),
           static_cast<std::streamsize>(n * sizeof(MergeRecord)));
    f.close();

    double read_secs = std::chrono::duration<double>(
        std::chrono::steady_clock::now() - t0).count();

    /* ── Parallel sort: split into n_sort_threads chunks, each sorted in its
       own thread.  Chunks are small enough to remain cache-friendly and the
       work is embarrassingly parallel.                                       */
    int64_t nn = static_cast<int64_t>(n);
    int n_chunks = std::min(n_sort_threads,
                            static_cast<int>((nn + 999) / 1000));
    int64_t chunk_size = (nn + n_chunks - 1) / n_chunks;

    auto cmp = [](const MergeRecord &a, const MergeRecord &b) {
        return key_less(a.key, b.key);
    };

    {
        std::vector<std::thread> sort_threads;
        sort_threads.reserve(n_chunks);
        for (int t = 0; t < n_chunks; t++) {
            int64_t b = static_cast<int64_t>(t) * chunk_size;
            int64_t e = std::min(b + chunk_size, nn);
            sort_threads.emplace_back([&records, b, e, &cmp]() {
                std::sort(records.begin() + b, records.begin() + e, cmp);
            });
        }
        for (auto &th : sort_threads) th.join();
    }

    double sort_secs = std::chrono::duration<double>(
        std::chrono::steady_clock::now() - t0).count() - read_secs;

    /* ── K-way in-memory merge → stream directly to output file ─────────────
       Merge cursors are raw pointers into the already-loaded records vector.
       No second allocation needed: output is flushed through a small write
       buffer.  Memory cost: O(n_chunks) for the heap + ~18 MB write buffer.  */
    struct Cursor {
        const MergeRecord *cur, *end;
        bool operator>(const Cursor &o) const {
            /* min-heap: top = smallest key */
            return key_less(o.cur->key, cur->key);
        }
    };
    std::priority_queue<Cursor, std::vector<Cursor>,
                        std::greater<Cursor>> heap;
    for (int t = 0; t < n_chunks; t++) {
        int64_t b = static_cast<int64_t>(t) * chunk_size;
        if (b >= nn) break;
        int64_t e = std::min(b + chunk_size, nn);
        heap.push({records.data() + b, records.data() + e});
    }

    constexpr size_t WBUF = 256 * 1024;
    std::vector<MergeRecord> wbuf;
    wbuf.reserve(WBUF);

    /* Serialize the merge+write phase across concurrent sorts so only one
       file writes to disk at a time.  This prevents interleaved 20+ GB
       writes from fragmenting sequential I/O into random access.  The
       in-memory sort still runs fully in parallel across files.            */
    std::lock_guard<std::mutex> write_lk(write_mtx);

    auto write_t0 = std::chrono::steady_clock::now();
    std::ofstream o(sorted_path, std::ios::binary);
    o.write(reinterpret_cast<const char *>(&n), sizeof(n));

    while (!heap.empty()) {
        Cursor top = heap.top(); heap.pop();
        wbuf.push_back(*top.cur);
        ++top.cur;
        if (top.cur < top.end) heap.push(top);
        if (wbuf.size() >= WBUF) {
            o.write(reinterpret_cast<const char *>(wbuf.data()),
                    static_cast<std::streamsize>(wbuf.size() * sizeof(MergeRecord)));
            wbuf.clear();
        }
    }
    if (!wbuf.empty())
        o.write(reinterpret_cast<const char *>(wbuf.data()),
                static_cast<std::streamsize>(wbuf.size() * sizeof(MergeRecord)));
    o.close();

    double write_secs = std::chrono::duration<double>(
        std::chrono::steady_clock::now() - write_t0).count();
    double total_secs = std::chrono::duration<double>(
        std::chrono::steady_clock::now() - t0).count();
    std::cerr << "  Sorted " << input_path.filename().string()
              << " (" << n / 1'000'000.0 << "M entries, "
              << n_chunks << " threads)"
              << "  read=" << std::fixed << std::setprecision(1) << read_secs << "s"
              << "  sort=" << sort_secs << "s"
              << "  write=" << write_secs << "s"
              << "  total=" << total_secs << "s\n";
}

/* ── Phase 2 helper: buffered reader for sorted checkpoint files ───────── */

class SortedCheckpointReader {
    static constexpr size_t BUF_RECORDS = 256 * 1024;  /* ~18 MB buffer */

    std::ifstream file_;
    uint64_t remaining_;
    std::vector<MergeRecord> buffer_;
    size_t buf_pos_;
    size_t buf_size_;

public:
    MergeRecord current;
    bool valid;

    explicit SortedCheckpointReader(const fs::path &path)
        : buffer_(BUF_RECORDS), buf_pos_(0), buf_size_(0), valid(false)
    {
        file_.open(path, std::ios::binary);
        if (!file_.is_open())
            throw std::runtime_error("Cannot open sorted shard: " + path.string());
        file_.read(reinterpret_cast<char *>(&remaining_), sizeof(remaining_));
        advance();
    }

    void advance() {
        if (buf_pos_ < buf_size_) {
            current = buffer_[buf_pos_++];
            valid = true;
            return;
        }
        /* Refill buffer from disk */
        if (remaining_ == 0) { valid = false; return; }
        size_t to_read = std::min(static_cast<uint64_t>(BUF_RECORDS), remaining_);
        file_.read(reinterpret_cast<char *>(buffer_.data()),
                   static_cast<std::streamsize>(to_read * sizeof(MergeRecord)));
        buf_pos_ = 1;
        buf_size_ = to_read;
        remaining_ -= to_read;
        current = buffer_[0];
        valid = true;
    }
};

/* ── Heap entry for the K-way merge priority queue ─────────────────────── */

struct HeapEntry {
    Hash128 key;
    int     reader_idx;

    bool operator>(const HeapEntry &o) const {
        if (key.hi != o.key.hi) return key.hi > o.key.hi;
        return key.lo > o.key.lo;
    }
};

/* ── Main sort-merge entry point ───────────────────────────────────────── */

static void merge_checkpoints(const std::vector<fs::path> &shard_paths,
                               const fs::path &output_path,
                               int n_threads) {
    auto t_total = std::chrono::steady_clock::now();

    std::cerr << "═══════════════════════════════════════════════════════════════\n"
              << " Sort-Merge: " << shard_paths.size() << " shards, "
              << n_threads << " threads\n"
              << "═══════════════════════════════════════════════════════════════\n\n";

    /* ── Count total records across all shards ───────────────────────────── */
    uint64_t total_input_records = 0;
    for (const auto &p : shard_paths) {
        std::ifstream f(p, std::ios::binary);
        uint64_t n;
        f.read(reinterpret_cast<char *>(&n), sizeof(n));
        total_input_records += n;
    }
    double input_gb = static_cast<double>(total_input_records) * sizeof(MergeRecord)
                      / (1024.0 * 1024 * 1024);
    std::cerr << "Total input records: " << total_input_records / 1'000'000.0
              << "M  (" << std::fixed << std::setprecision(1) << input_gb << " GB)\n\n";

    /* ── Phase 1: Sort each checkpoint file (parallel) ───────────────────── */
    std::cerr << "Phase 1: Sorting checkpoint files...\n";
    auto t1 = std::chrono::steady_clock::now();

    fs::path temp_dir = output_path.parent_path() / "merge_tmp";
    fs::create_directories(temp_dir);

    std::vector<fs::path> sorted_paths(shard_paths.size());
    for (size_t i = 0; i < shard_paths.size(); i++)
        sorted_paths[i] = temp_dir / (shard_paths[i].stem().string() + ".sorted");

    {
        size_t avg_shard_gb = static_cast<size_t>(
            input_gb / shard_paths.size()) + 1;

        /* Concurrency limit: each concurrent sort holds one shard as an
           anonymous vector in RAM.  We also need OS page cache for the
           sequential read (~1×) and write (~1×) of each file, so the true
           per-sort cost is ~3× the shard size.  Use at most 15% of
           physical RAM to stay well clear of swap.

           NOTE: do NOT use sysconf(_SC_AVPHYS_PAGES) / MemAvailable here —
           those include reclaimable page cache and caused the original
           over-commitment that led to 19 concurrent sorts and swap thrashing. */
        size_t phys_gb = sysconf(_SC_PHYS_PAGES) * (size_t)sysconf(_SC_PAGESIZE)
                         / (1024ULL * 1024 * 1024);
        int max_concurrent = std::max(1,
            static_cast<int>(phys_gb * 15 / 100 / avg_shard_gb));
        int concurrent = std::min({n_threads,
                                   static_cast<int>(shard_paths.size()),
                                   max_concurrent});

        /* Distribute all threads evenly across concurrent sorts so every
           core is busy sorting rather than sitting idle.                   */
        int sort_threads_per_file = std::max(1, n_threads / concurrent);

        std::cerr << "  Sorting " << shard_paths.size() << " files, "
                  << concurrent << " concurrent, "
                  << sort_threads_per_file << " threads/file"
                  << " (avg shard ~" << avg_shard_gb << " GB,"
                  << " phys RAM " << phys_gb << " GB)\n";

        std::atomic<size_t> next_file{0};
        std::vector<std::thread> sort_threads;
        std::mutex err_mtx;
        std::mutex write_mtx;  /* serializes disk writes across concurrent sorts */
        std::exception_ptr first_error;

        for (int t = 0; t < concurrent; t++) {
            sort_threads.emplace_back([&, sort_threads_per_file] {
                for (;;) {
                    size_t idx = next_file.fetch_add(1);
                    if (idx >= shard_paths.size()) break;
                    try {
                        sort_single_checkpoint(shard_paths[idx], sorted_paths[idx],
                                               sort_threads_per_file, write_mtx);
                    } catch (...) {
                        std::lock_guard<std::mutex> lk(err_mtx);
                        if (!first_error) first_error = std::current_exception();
                        break;
                    }
                }
            });
        }
        for (auto &th : sort_threads) th.join();
        if (first_error) std::rethrow_exception(first_error);
    }

    double sort_secs = std::chrono::duration<double>(
        std::chrono::steady_clock::now() - t1).count();
    std::cerr << "\nPhase 1 complete: " << std::fixed << std::setprecision(1)
              << sort_secs << "s\n\n";

    /* ── Phase 2: K-way merge with streaming Parquet output ──────────────── */
    std::cerr << "Phase 2: K-way merge (" << sorted_paths.size() << "-way)...\n";
    auto t2 = std::chrono::steady_clock::now();

    /* Open all sorted files with buffered readers */
    std::vector<std::unique_ptr<SortedCheckpointReader>> readers;
    readers.reserve(sorted_paths.size());
    for (const auto &p : sorted_paths)
        readers.push_back(std::make_unique<SortedCheckpointReader>(p));

    /* Initialise min-heap */
    std::priority_queue<HeapEntry, std::vector<HeapEntry>,
                        std::greater<HeapEntry>> heap;
    for (int i = 0; i < static_cast<int>(readers.size()); i++)
        if (readers[i]->valid)
            heap.push({readers[i]->current.key, i});

    /* ── Set up streaming Parquet writer ─────────────────────────────────── */
    auto schema = arrow::schema({
        arrow::field("hash_lo",           arrow::uint64()),
        arrow::field("hash_hi",           arrow::uint64()),
        arrow::field("count",             arrow::uint64()),
        arrow::field("first_weight0",     arrow::int32()),
        arrow::field("first_weight1",     arrow::int32()),
        arrow::field("first_weight2",     arrow::int32()),
        arrow::field("first_weight3",     arrow::int32()),
        arrow::field("first_weight4",     arrow::int32()),
        arrow::field("first_weight5",     arrow::int32()),
        arrow::field("vertex_count",      arrow::int16()),
        arrow::field("facet_count",       arrow::int16()),
        arrow::field("point_count",       arrow::int32()),
        arrow::field("dual_point_count",  arrow::int32()),
        arrow::field("h11",               arrow::int16()),
        arrow::field("h12",               arrow::int16()),
        arrow::field("h13",               arrow::int16()),
    });

    std::shared_ptr<arrow::io::FileOutputStream> out;
    ASSIGN_OR_THROW(out, arrow::io::FileOutputStream::Open(output_path.string()));

    auto writer_props = parquet::WriterProperties::Builder()
        .compression(parquet::Compression::ZSTD)
        ->max_row_group_length(1024 * 1024)
        ->build();

    std::unique_ptr<parquet::arrow::FileWriter> writer;
    {
        auto result = parquet::arrow::FileWriter::Open(
            *schema, arrow::default_memory_pool(), out, writer_props);
        if (!result.ok())
            throw std::runtime_error("Parquet writer: " + result.status().ToString());
        writer = std::move(result).ValueOrDie();
    }

    /* Column builders — flushed in batches */
    constexpr int64_t FLUSH_SIZE = 1'000'000;
    arrow::UInt64Builder hash_lo_b, hash_hi_b, count_b;
    arrow::Int32Builder  w0_b, w1_b, w2_b, w3_b, w4_b, w5_b, pc_b, dpc_b;
    arrow::Int16Builder  vc_b, fc_b, h11_b, h12_b, h13_b;

    int64_t batch_n = 0;

    auto flush_batch = [&]() {
        if (batch_n == 0) return;
        std::shared_ptr<arrow::Array>
            a_hlo, a_hhi, a_cnt,
            a_w0, a_w1, a_w2, a_w3, a_w4, a_w5,
            a_vc, a_fc, a_pc, a_dpc, a_h11, a_h12, a_h13;
        CHECK_ARROW(hash_lo_b.Finish(&a_hlo)); CHECK_ARROW(hash_hi_b.Finish(&a_hhi));
        CHECK_ARROW(count_b.Finish(&a_cnt));
        CHECK_ARROW(w0_b.Finish(&a_w0));  CHECK_ARROW(w1_b.Finish(&a_w1));
        CHECK_ARROW(w2_b.Finish(&a_w2));  CHECK_ARROW(w3_b.Finish(&a_w3));
        CHECK_ARROW(w4_b.Finish(&a_w4));  CHECK_ARROW(w5_b.Finish(&a_w5));
        CHECK_ARROW(vc_b.Finish(&a_vc));  CHECK_ARROW(fc_b.Finish(&a_fc));
        CHECK_ARROW(pc_b.Finish(&a_pc));  CHECK_ARROW(dpc_b.Finish(&a_dpc));
        CHECK_ARROW(h11_b.Finish(&a_h11)); CHECK_ARROW(h12_b.Finish(&a_h12));
        CHECK_ARROW(h13_b.Finish(&a_h13));
        auto batch = arrow::RecordBatch::Make(schema, batch_n, {
            a_hlo, a_hhi, a_cnt,
            a_w0, a_w1, a_w2, a_w3, a_w4, a_w5,
            a_vc, a_fc, a_pc, a_dpc, a_h11, a_h12, a_h13
        });
        CHECK_ARROW(writer->WriteRecordBatch(*batch));
        batch_n = 0;
    };

    /* ── K-way merge loop ────────────────────────────────────────────────── */
    uint64_t total_unique = 0;
    uint64_t total_merged = 0;
    auto last_report = std::chrono::steady_clock::now();

    while (!heap.empty()) {
        HeapEntry top = heap.top();
        heap.pop();

        Hash128 current_key = top.key;
        PolytopeInfo merged_info = readers[top.reader_idx]->current.info;
        total_merged++;

        /* Advance the reader we just consumed */
        readers[top.reader_idx]->advance();
        if (readers[top.reader_idx]->valid)
            heap.push({readers[top.reader_idx]->current.key, top.reader_idx});

        /* Merge all entries with the same key from other readers */
        while (!heap.empty() && heap.top().key == current_key) {
            HeapEntry dup = heap.top();
            heap.pop();
            merged_info.count += readers[dup.reader_idx]->current.info.count;
            total_merged++;

            readers[dup.reader_idx]->advance();
            if (readers[dup.reader_idx]->valid)
                heap.push({readers[dup.reader_idx]->current.key, dup.reader_idx});
        }

        /* Append merged record to output batch */
        CHECK_ARROW(hash_lo_b.Append(current_key.lo));
        CHECK_ARROW(hash_hi_b.Append(current_key.hi));
        CHECK_ARROW(count_b.Append(merged_info.count));
        CHECK_ARROW(w0_b.Append(merged_info.first_weights[0]));
        CHECK_ARROW(w1_b.Append(merged_info.first_weights[1]));
        CHECK_ARROW(w2_b.Append(merged_info.first_weights[2]));
        CHECK_ARROW(w3_b.Append(merged_info.first_weights[3]));
        CHECK_ARROW(w4_b.Append(merged_info.first_weights[4]));
        CHECK_ARROW(w5_b.Append(merged_info.first_weights[5]));
        CHECK_ARROW(vc_b.Append(merged_info.vertex_count));
        CHECK_ARROW(fc_b.Append(merged_info.facet_count));
        CHECK_ARROW(pc_b.Append(merged_info.point_count));
        CHECK_ARROW(dpc_b.Append(merged_info.dual_point_count));
        CHECK_ARROW(h11_b.Append(merged_info.h11));
        CHECK_ARROW(h12_b.Append(merged_info.h12));
        CHECK_ARROW(h13_b.Append(merged_info.h13));
        batch_n++;
        total_unique++;

        if (batch_n >= FLUSH_SIZE) {
            flush_batch();

            auto now = std::chrono::steady_clock::now();
            if (std::chrono::duration<double>(now - last_report).count() >= 2.0) {
                double elapsed = std::chrono::duration<double>(now - t2).count();
                double rate = total_merged / (elapsed > 0 ? elapsed : 1.0);
                double pct = 100.0 * total_merged / total_input_records;
                double eta = (total_input_records - total_merged) / (rate > 0 ? rate : 1.0);
                std::cerr << "\r  " << std::fixed << std::setprecision(1) << pct << "%"
                          << "  unique: " << total_unique / 1'000'000.0 << "M"
                          << "  merged: " << total_merged / 1'000'000.0 << "M"
                          << "  " << std::setprecision(0) << rate / 1'000'000 << "M rec/s"
                          << "  ETA " << static_cast<int>(eta / 60) << "m"
                          << "        " << std::flush;
                last_report = now;
            }
        }
    }

    /* Flush remaining records and close writer */
    flush_batch();
    CHECK_ARROW(writer->Close());
    CHECK_ARROW(out->Close());

    /* ── Cleanup temp files ──────────────────────────────────────────────── */
    for (const auto &p : sorted_paths)
        if (fs::exists(p)) fs::remove(p);
    fs::remove(temp_dir);

    double merge_secs = std::chrono::duration<double>(
        std::chrono::steady_clock::now() - t2).count();
    double total_secs = std::chrono::duration<double>(
        std::chrono::steady_clock::now() - t_total).count();

    std::cerr << "\n\n═══════════════════════════════════════════════════════════════\n"
              << " Sort-Merge Complete\n"
              << "═══════════════════════════════════════════════════════════════\n"
              << "  Total input records:  " << total_merged << "\n"
              << "  Unique polytopes:     " << total_unique << "\n"
              << "  Duplicates removed:   " << (total_merged - total_unique) << "\n"
              << "  Sort phase:           " << std::fixed << std::setprecision(1)
              << sort_secs << "s\n"
              << "  Merge phase:          " << merge_secs << "s\n"
              << "  Total time:           " << total_secs << "s ("
              << total_secs / 60 << " min)\n"
              << "  Output:               " << output_path.string() << "\n";
}

/* ═══════════════════════════════════════════════════════════════════════════
 *  Main processing loop: process one parquet file
 * ═══════════════════════════════════════════════════════════════════════════ */

static void process_file(const fs::path &input_path,
                         PolytopeMap &global_map,
                         std::mutex &global_mtx,
                         Stats &stats,
                         int n_threads,
                         int64_t max_rows = 0)
{
    /* Read all CWS from the parquet file */
    auto t0 = std::chrono::steady_clock::now();
    std::vector<CWSRow> rows = read_parquet_file(input_path, max_rows);
    double read_time = std::chrono::duration<double>(
        std::chrono::steady_clock::now() - t0).count();

    int64_t n = static_cast<int64_t>(rows.size());
    stats.total_cws.fetch_add(n, std::memory_order_relaxed);

    std::cerr << "\n  " << input_path.filename().string()
              << ": " << n / 1'000'000.0 << "M rows, read in "
              << std::fixed << std::setprecision(1) << read_time << "s\n";

    /* ── Work distribution strategy ──────────────────────────────────────
     * Processing cost is dominated by point_count (Make_CWS_Points and
     * Find_Equations scale with # lattice points).  The input data has
     * a natural random distribution of point_counts, which provides
     * reasonable load balance across threads.  We use dynamic work-stealing
     * with small blocks so that if one thread hits an expensive polytope,
     * the others continue processing lighter ones from the shared queue. */
    int actual_threads = std::min(n_threads, (int)((n + 999) / 1000));
    constexpr int64_t BLOCK_SIZE = 1024;

    int64_t n_blocks = (n + BLOCK_SIZE - 1) / BLOCK_SIZE;
    std::atomic<int64_t> next_block{0};

    /* Each thread gets its own workspace and local map */
    std::vector<std::future<void>> futures;
    std::vector<PolytopeMap> local_maps(actual_threads);

    /* Pre-reserve local maps based on expected unique polytope density */
    int64_t chunk_size = (n + actual_threads - 1) / actual_threads;
    for (auto &m : local_maps)
        m.reserve(std::min(chunk_size, (int64_t)1'000'000));

    ThreadPool pool(actual_threads);

    for (int t = 0; t < actual_threads; t++) {
        futures.push_back(pool.enqueue([&, t] {
            PalpWorkspace *ws = palp_workspace_alloc();
            if (!ws) {
                std::cerr << "Failed to allocate PALP workspace\n";
                return;
            }

            PalpNFResult result;

            /* Dynamic work-stealing: grab next block from shared counter */
            for (;;) {
                int64_t b = next_block.fetch_add(1, std::memory_order_relaxed);
                if (b >= n_blocks) break;

                int64_t bstart = b * BLOCK_SIZE;
                int64_t bend = std::min(bstart + BLOCK_SIZE, n);

                for (int64_t i = bstart; i < bend; i++) {
                    const CWSRow &row = rows[i];
                    palp_compute_nf(ws, row.weights, &result);

                    if (!result.ok) {
                        stats.failed_cws.fetch_add(1, std::memory_order_relaxed);
                        stats.processed_cws.fetch_add(1, std::memory_order_relaxed);
                        continue;
                    }

                    Hash128 key = hash_normal_form(result.nf, result.dim, result.nv);

                    auto it = local_maps[t].find(key);
                    if (it != local_maps[t].end()) {
                        it->second.count++;
                        stats.duplicate_cws.fetch_add(1, std::memory_order_relaxed);
                    } else {
                        PolytopeInfo info{};
                        info.count = 1;
                        std::memcpy(info.first_weights, row.weights, sizeof(row.weights));
                        info.vertex_count     = static_cast<int16_t>(result.nv);
                        info.facet_count      = static_cast<int16_t>(result.ne);
                        info.point_count      = result.np;
                        info.dual_point_count = row.dual_point_count;
                        info.h11              = static_cast<int16_t>(row.h11);
                        info.h12              = static_cast<int16_t>(row.h12);
                        info.h13              = static_cast<int16_t>(row.h13);
                        local_maps[t].emplace(key, info);
                    }
                    stats.processed_cws.fetch_add(1, std::memory_order_relaxed);
                }
            }

            palp_workspace_free(ws);
        }));
    }

    /* Wait for all threads and show progress periodically */
    bool all_done = false;
    while (!all_done) {
        all_done = true;
        for (auto &f : futures) {
            if (f.wait_for(std::chrono::milliseconds(500)) != std::future_status::ready)
                all_done = false;
        }
        stats.print_progress();
    }
    for (auto &f : futures) f.get();  /* propagate exceptions */

    /* Merge all local maps into global */
    for (auto &lm : local_maps) {
        merge_maps(global_map, lm, global_mtx, stats);
    }

    stats.files_done.fetch_add(1, std::memory_order_relaxed);
}

/* ═══════════════════════════════════════════════════════════════════════════
 *  Get current RSS in bytes (Linux-specific)
 * ═══════════════════════════════════════════════════════════════════════════ */

static size_t get_rss_bytes() {
    std::ifstream f("/proc/self/statm");
    size_t pages;
    f >> pages;  /* total */
    f >> pages;  /* RSS in pages */
    return pages * sysconf(_SC_PAGESIZE);
}

/* ═══════════════════════════════════════════════════════════════════════════
 *  CLI and main
 * ═══════════════════════════════════════════════════════════════════════════ */

static void usage(const char *argv0) {
    std::cerr
        << "Usage: " << argv0 << "\n"
        << "  --input   <dir>   Directory containing ws-5d-reflexive-*.parquet\n"
        << "  --output  <dir>   Output directory for results\n"
        << " [--checkpoint <dir>] Directory for checkpoint files\n"
        << " [--threads <n>]   Thread count (default: hardware_concurrency)\n"
        << " [--start <n>]     First file index (default: 0)\n"
        << " [--end <n>]       Last file index (inclusive, default: last)\n"
        << " [--resume]        Resume from checkpoint\n"
        << " [--max-rows <n>]  Limit rows processed per file (for testing)\n"
        << " [--benchmark <n>] Benchmark mode: process N rows from first file\n"
        << " [--merge <dir>]   Merge checkpoint shards from <dir>\n"
        << "\n"
        << "Examples:\n"
        << "  # Process all files\n"
        << "  " << argv0 << " --input ./data --output ./results\n"
        << "\n"
        << "  # Process files 0-99 on runner 1\n"
        << "  " << argv0 << " --input ./data --output ./results --start 0 --end 99\n"
        << "\n"
        << "  # Benchmark: process 100K rows\n"
        << "  " << argv0 << " --input ./data --output ./results --benchmark 100000\n";
}

int main(int argc, char **argv) {
    Config cfg;
    std::string merge_dir;

    for (int i = 1; i < argc; i++) {
        std::string a = argv[i];
        if      (a == "--input"       && i+1 < argc) cfg.input_dir      = argv[++i];
        else if (a == "--output"      && i+1 < argc) cfg.output_dir     = argv[++i];
        else if (a == "--checkpoint"  && i+1 < argc) cfg.checkpoint_dir  = argv[++i];
        else if (a == "--threads"     && i+1 < argc) cfg.n_threads      = std::stoi(argv[++i]);
        else if (a == "--start"       && i+1 < argc) cfg.start_file     = std::stoi(argv[++i]);
        else if (a == "--end"         && i+1 < argc) cfg.end_file       = std::stoi(argv[++i]);
        else if (a == "--resume")                     cfg.resume         = true;
        else if (a == "--max-rows"    && i+1 < argc) cfg.max_rows_per_file = std::stoll(argv[++i]);
        else if (a == "--benchmark"   && i+1 < argc) {
            cfg.benchmark_only = true;
            cfg.benchmark_rows = std::stoll(argv[++i]);
        }
        else if (a == "--merge"       && i+1 < argc) merge_dir          = argv[++i];
        else if (a == "-h" || a == "--help") { usage(argv[0]); return 0; }
        else { std::cerr << "Unknown option: " << a << "\n"; usage(argv[0]); return 1; }
    }

    /* ── Merge mode ──────────────────────────────────────────────────────── */
    if (!merge_dir.empty()) {
        if (cfg.output_dir.empty()) { usage(argv[0]); return 1; }
        if (cfg.n_threads <= 0)
            cfg.n_threads = static_cast<int>(std::thread::hardware_concurrency());
        std::vector<fs::path> shards;
        for (auto &e : fs::directory_iterator(merge_dir))
            if (e.path().extension() == ".ckpt") shards.push_back(e.path());
        std::sort(shards.begin(), shards.end());
        if (shards.empty()) { std::cerr << "No .ckpt files in " << merge_dir << "\n"; return 1; }
        fs::create_directories(cfg.output_dir);
        merge_checkpoints(shards, fs::path(cfg.output_dir) / "unique_polytopes.parquet",
                          cfg.n_threads);
        return 0;
    }

    if (cfg.input_dir.empty() || cfg.output_dir.empty()) {
        usage(argv[0]);
        return 1;
    }
    if (cfg.n_threads <= 0)
        cfg.n_threads = static_cast<int>(std::thread::hardware_concurrency());
    if (cfg.checkpoint_dir.empty())
        cfg.checkpoint_dir = cfg.output_dir + "/checkpoints";

    /* ── Initialize PALP ─────────────────────────────────────────────────── */
    palp_init();

    /* ── Collect input files ─────────────────────────────────────────────── */
    std::vector<fs::path> input_files;
    for (auto &e : fs::directory_iterator(cfg.input_dir))
        if (e.path().extension() == ".parquet")
            input_files.push_back(e.path());
    std::sort(input_files.begin(), input_files.end());

    if (input_files.empty()) {
        std::cerr << "No .parquet files in " << cfg.input_dir << "\n";
        return 1;
    }

    /* Apply file range filter */
    if (cfg.start_file >= 0) {
        int end = cfg.end_file >= 0 ? cfg.end_file + 1 : static_cast<int>(input_files.size());
        end = std::min(end, static_cast<int>(input_files.size()));
        input_files = std::vector<fs::path>(
            input_files.begin() + cfg.start_file,
            input_files.begin() + end);
    }

    if (cfg.benchmark_only) {
        /* In benchmark mode, only process the first file */
        input_files.resize(1);
    }

    fs::create_directories(cfg.output_dir);
    fs::create_directories(cfg.checkpoint_dir);

    std::cerr << "=== Polytope Classifier ===\n"
              << "Input:      " << cfg.input_dir << "\n"
              << "Output:     " << cfg.output_dir << "\n"
              << "Files:      " << input_files.size() << "\n"
              << "Threads:    " << cfg.n_threads << "\n"
              << "CPU:        ";
    {
        std::ifstream cpuinfo("/proc/cpuinfo");
        std::string line;
        while (std::getline(cpuinfo, line))
            if (line.find("model name") != std::string::npos) {
                std::cerr << line.substr(line.find(':') + 2) << "\n";
                break;
            }
    }
    std::cerr << "RAM:        " << sysconf(_SC_PHYS_PAGES) * sysconf(_SC_PAGESIZE)
                                   / (1024*1024*1024) << " GB\n\n";

    /* ── Global hash map ─────────────────────────────────────────────────── */
    PolytopeMap global_map;
    std::mutex global_mtx;

    /* Resume from checkpoint if requested */
    if (cfg.resume) {
        /* Load ALL .ckpt files from the checkpoint directory.  In a properly
           managed run there should be exactly one (the latest full snapshot),
           but we load all in case of manual checkpoint management. */
        for (auto &e : fs::directory_iterator(cfg.checkpoint_dir))
            if (e.path().extension() == ".ckpt")
                read_checkpoint(global_map, e.path());
    } else {
        /* Fresh start: clean any stale checkpoint files from previous runs
           to avoid disk accumulation and confusion during --resume later. */
        for (auto &e : fs::directory_iterator(cfg.checkpoint_dir)) {
            if (e.path().extension() == ".ckpt") {
                std::cerr << "  Removing old checkpoint: "
                          << e.path().filename().string() << "\n";
                fs::remove(e.path());
            }
        }
    }

    /* ── Main loop ───────────────────────────────────────────────────────── */
    Stats stats;
    stats.files_total = static_cast<int>(input_files.size());
    stats.start = std::chrono::steady_clock::now();
    stats.unique_polytopes.store(global_map.size());

    auto t_total = std::chrono::steady_clock::now();
    fs::path prev_checkpoint;   /* track previous checkpoint for cleanup */

    for (size_t fi = 0; fi < input_files.size(); fi++) {
        int64_t max_rows = (cfg.benchmark_only && cfg.benchmark_rows > 0)
                           ? cfg.benchmark_rows
                           : cfg.max_rows_per_file;

        process_file(input_files[fi], global_map, global_mtx, stats,
                     cfg.n_threads, max_rows);

        /* Memory monitoring */
        size_t rss = get_rss_bytes();
        std::cerr << "\n  RSS: " << rss / (1024*1024) << " MB"
                  << "  Map size: " << global_map.size() << "\n";

        /* Periodic checkpoint — each checkpoint is a FULL snapshot of the
           global map, so we only need to keep the latest one. */
        if ((fi + 1) % 10 == 0 || fi == input_files.size() - 1) {
            char buf[64];
            int global_idx = (cfg.start_file >= 0 ? cfg.start_file : 0)
                             + static_cast<int>(fi);
            std::snprintf(buf, sizeof(buf), "checkpoint-%04d.ckpt", global_idx);
            fs::path ckpt_path = fs::path(cfg.checkpoint_dir) / buf;
            write_checkpoint(global_map, ckpt_path);

            /* Delete previous checkpoint — it is a strict subset of the
               one just written. */
            if (!prev_checkpoint.empty() && fs::exists(prev_checkpoint))
                fs::remove(prev_checkpoint);
            prev_checkpoint = ckpt_path;
        }
    }

    double total_secs = std::chrono::duration<double>(
        std::chrono::steady_clock::now() - t_total).count();

    /* ── Final output ────────────────────────────────────────────────────── */
    /* Accounting invariant check:
       Every CWS is either failed, within-thread duplicate, or creates a
       local map entry.  Every local map entry adds 1 to either unique or
       duplicate during merge.  So: processed == unique + dgplicate + failed. */
    {
        int64_t p = stats.processed_cws.load();
        int64_t u = static_cast<int64_t>(global_map.size());
        int64_t d = stats.duplicate_cws.load();
        int64_t f = stats.failed_cws.load();
        if (p != u + d + f) {
            std::cerr << "\n⚠ ACCOUNTING MISMATCH: processed(" << p
                      << ") != unique(" << u << ") + dup(" << d
                      << ") + fail(" << f << ") = " << (u + d + f)
                      << "  [diff=" << (p - u - d - f) << "]\n";
        }
    }

    std::cerr << "\n\n=== Results ===\n"
              << "Total CWS processed:   " << stats.processed_cws.load() << "\n"
              << "Failed:                " << stats.failed_cws.load() << "\n"
              << "Duplicate CWS:         " << stats.duplicate_cws.load() << "\n"
              << "Unique polytopes:      " << global_map.size() << "\n"
              << "Total time:            " << std::fixed << std::setprecision(1)
              << total_secs << "s (" << total_secs / 60 << " min)\n"
              << "Throughput:            " << std::setprecision(0)
              << stats.processed_cws.load() / total_secs << " CWS/s\n";

    /* Write final parquet */
    fs::path output_parquet = fs::path(cfg.output_dir) / "unique_polytopes.parquet";
    std::cerr << "\nWriting results to " << output_parquet.string() << " ...\n";
    write_results(global_map, output_parquet);

    /* Write summary JSON */
    {
        fs::path summary_path = fs::path(cfg.output_dir) / "summary.json";
        std::ofstream sf(summary_path);
        sf << "{\n"
           << "  \"total_cws\": " << stats.processed_cws.load() << ",\n"
           << "  \"failed_cws\": " << stats.failed_cws.load() << ",\n"
           << "  \"duplicate_cws\": " << stats.duplicate_cws.load() << ",\n"
           << "  \"unique_polytopes\": " << global_map.size() << ",\n"
           << "  \"total_seconds\": " << total_secs << ",\n"
           << "  \"throughput_cws_per_sec\": " << stats.processed_cws.load() / total_secs << ",\n"
           << "  \"files_processed\": " << stats.files_done.load() << ",\n"
           << "  \"threads\": " << cfg.n_threads << "\n"
           << "}\n";
    }

    std::cerr << "Done.\n";
    return 0;
}
