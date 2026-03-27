/**
 * sort_parquet.cpp
 * ─────────────────────────────────────────────────────────────────────────────
 * Two-phase external merge-sort for the ws-5d-reflexive Parquet dataset.
 *
 * Phase 1 – parallel intra-file sort
 *   Each input file is read into memory, sorted by vertex_count using
 *   Arrow's SIMD-accelerated sort kernel, then written back as a sorted
 *   temporary Parquet file.  Files are processed concurrently by a thread pool.
 *
 * Phase 2 – k-way merge
 *   All sorted temporary files are merged via a min-heap.  Rows are read in
 *   batches (MERGE_BATCH_ROWS rows per file) to keep random-access overhead
 *   low.  The merged stream is written to one or more output Parquet files,
 *   each capped at OUTPUT_FILE_ROWS rows.
 *
 * Key optimisations
 *   • Memory-mapped I/O (arrow::io::MemoryMappedFile) for phase-1 reads
 *   • Arrow compute::SortIndices – SIMD radix/comparison hybrid (O(n log n))
 *   • LZ4_RAW compression – fastest codec that Parquet supports
 *   • Large row groups (1 M rows) → fewer seeks, better compression ratio
 *   • Pre-allocated output buffers via Arrow PoolAllocator
 *   • OpenMP-parallel Arrow compute kernels (set via OMP_NUM_THREADS)
 *   • Thread pool sized to hardware_concurrency (overridable via --threads)
 *   • Progress + throughput reporting every completed file
 *
 * Build:  see CMakeLists.txt
 * Usage:
 *   ./sort_parquet --input  /data/ws-5d-reflexive \
 *                 --tmp    /data/ws-5d-sorted-tmp \
 *                 --output /data/ws-5d-sorted     \
 *                 --threads 8
 *
 *   --skip-phase1  reuse an existing tmp dir (e.g. resume after crash)
 * ─────────────────────────────────────────────────────────────────────────────
 */

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <future>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <queue>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/io/api.h>
#include <arrow/util/thread_pool.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/properties.h>

namespace fs  = std::filesystem;
namespace cp  = arrow::compute;

// ─── Tuneable constants ────────────────────────────────────────────────────────
static constexpr int      VERTEX_COUNT_COL  = 6;          // 0-indexed in schema
static constexpr int64_t  MERGE_BATCH_ROWS  = 128 * 1024; // rows buffered / file
static constexpr int64_t  OUTPUT_RG_ROWS    = 1024 * 1024;// output row-group size
static constexpr int64_t  OUTPUT_FILE_ROWS  = 50'000'000LL;// rows per output file
static constexpr int      READ_BUFFER_BYTES = 32 * 1024 * 1024; // 32 MB read buf

// ─── Macro helpers ─────────────────────────────────────────────────────────────
#define CHECK_ARROW(expr)                                                \
  do {                                                                   \
    arrow::Status _s = (expr);                                           \
    if (!_s.ok()) {                                                      \
      throw std::runtime_error(std::string(__FILE__) + ":" +            \
                               std::to_string(__LINE__) + " " +          \
                               _s.ToString());                            \
    }                                                                    \
  } while (0)

#define ASSIGN_OR_THROW(lhs, expr)                                       \
  do {                                                                   \
    auto _res = (expr);                                                  \
    if (!_res.ok()) {                                                    \
      throw std::runtime_error(std::string(__FILE__) + ":" +            \
                               std::to_string(__LINE__) + " " +          \
                               _res.status().ToString());                 \
    }                                                                    \
    lhs = std::move(_res).ValueOrDie();                                  \
  } while (0)

// ─── Shared writer properties (LZ4, large pages) ──────────────────────────────
static std::shared_ptr<parquet::WriterProperties> make_writer_props() {
    return parquet::WriterProperties::Builder()
        .compression(parquet::Compression::LZ4)
        ->data_pagesize(4 * 1024 * 1024)   // 4 MB data pages
        ->dictionary_pagesize_limit(2 * 1024 * 1024)
        ->write_batch_size(8192)
        ->max_row_group_length(OUTPUT_RG_ROWS)
        ->build();
}

static std::shared_ptr<parquet::ArrowWriterProperties> make_arrow_writer_props() {
    return parquet::ArrowWriterProperties::Builder()
        .store_schema()
        ->build();
}

// ─── Arrow reader properties (multithreaded decode) ───────────────────────────
static parquet::ArrowReaderProperties make_reader_props(int64_t batch_size = 0) {
    parquet::ArrowReaderProperties p;
    p.set_use_threads(true);
    if (batch_size > 0) p.set_batch_size(batch_size);
    return p;
}

// ─── Progress / timing helpers ────────────────────────────────────────────────
struct Progress {
    std::mutex          mtx;
    std::atomic<int>    done{0};
    int                 total{0};
    std::chrono::steady_clock::time_point start;

    explicit Progress(int n) : total(n), start(std::chrono::steady_clock::now()) {}

    void tick(const std::string& label) {
        int d = ++done;
        auto now   = std::chrono::steady_clock::now();
        double secs = std::chrono::duration<double>(now - start).count();
        double rate = d / secs;
        double eta  = (total - d) / (rate > 0 ? rate : 1.0);

        std::lock_guard<std::mutex> lk(mtx);
        std::cout << "\r  [" << std::setw(5) << d << "/" << total << "]  "
                  << std::fixed << std::setprecision(1)
                  << rate << " files/s  ETA " << (int)eta << "s  "
                  << label << "          " << std::flush;
    }
};

// ─── Thread pool ──────────────────────────────────────────────────────────────
class ThreadPool {
public:
    explicit ThreadPool(int n) {
        for (int i = 0; i < n; ++i)
            workers_.emplace_back([this] { run(); });
    }
    ~ThreadPool() {
        { std::unique_lock<std::mutex> lk(mtx_); stop_ = true; }
        cv_.notify_all();
        for (auto& w : workers_) w.join();
    }

    template<class F>
    std::future<void> enqueue(F&& f) {
        auto task = std::make_shared<std::packaged_task<void()>>(std::forward<F>(f));
        auto fut  = task->get_future();
        { std::unique_lock<std::mutex> lk(mtx_); queue_.emplace([task]{ (*task)(); }); }
        cv_.notify_one();
        return fut;
    }

private:
    void run() {
        for (;;) {
            std::function<void()> job;
            {
                std::unique_lock<std::mutex> lk(mtx_);
                cv_.wait(lk, [this]{ return stop_ || !queue_.empty(); });
                if (stop_ && queue_.empty()) return;
                job = std::move(queue_.front());
                queue_.pop();
            }
            job();
        }
    }

    std::vector<std::thread>          workers_;
    std::queue<std::function<void()>> queue_;
    std::mutex                        mtx_;
    std::condition_variable           cv_;
    bool                              stop_{false};
};

// ─────────────────────────────────────────────────────────────────────────────
// PHASE 1 — sort a single Parquet file by vertex_count
// ─────────────────────────────────────────────────────────────────────────────
static void sort_file(const fs::path& in_path, const fs::path& out_path) {
    // Memory-mapped read – avoids a kernel copy
    std::shared_ptr<arrow::io::MemoryMappedFile> mmap;
    ASSIGN_OR_THROW(mmap, arrow::io::MemoryMappedFile::Open(
        in_path.string(), arrow::io::FileMode::READ));

    // Build a parquet FileReader with parallel column decoding
    std::unique_ptr<parquet::arrow::FileReader> reader;
    parquet::ReaderProperties rp;
    rp.enable_buffered_stream();
    rp.set_buffer_size(READ_BUFFER_BYTES);

    {
        parquet::arrow::FileReaderBuilder fb;
        CHECK_ARROW(fb.Open(mmap));
        fb.memory_pool(arrow::default_memory_pool())
          ->properties(make_reader_props(OUTPUT_RG_ROWS));
        CHECK_ARROW(fb.Build(&reader));
    }

    // Read the whole file as a single Table
    std::shared_ptr<arrow::Table> table;
    CHECK_ARROW(reader->ReadTable(&table));

    // Sort indices using Arrow's SIMD-accelerated kernel
    cp::SortOptions sort_opts({cp::SortKey(VERTEX_COUNT_COL, cp::SortOrder::Ascending)});
    std::shared_ptr<arrow::Array> indices;
    ASSIGN_OR_THROW(indices, cp::SortIndices(arrow::Datum(table), sort_opts));

    // Reorder the table (zero-copy where possible via dictionary / slice tricks)
    std::shared_ptr<arrow::RecordBatch> sorted_batch;
    {
        arrow::Datum result;
        ASSIGN_OR_THROW(result, cp::Take(arrow::Datum(table),
                                         arrow::Datum(indices),
                                         cp::TakeOptions::BoundsCheck()));
        table = result.table();
    }

    // Write sorted table to output path
    std::shared_ptr<arrow::io::FileOutputStream> out_stream;
    ASSIGN_OR_THROW(out_stream, arrow::io::FileOutputStream::Open(out_path.string()));

    CHECK_ARROW(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(),
        out_stream, OUTPUT_RG_ROWS, make_writer_props(), make_arrow_writer_props()));

    CHECK_ARROW(out_stream->Close());
}

// ─────────────────────────────────────────────────────────────────────────────
// PHASE 2 — k-way merge of sorted files
// ─────────────────────────────────────────────────────────────────────────────

// State held for each file being merged
struct MergeSource {
    std::unique_ptr<parquet::arrow::FileReader> reader;
    std::shared_ptr<arrow::Table>               batch;   // current buffer
    int64_t                                     row{0};  // position in batch
    int64_t                                     total_rows{0};
    int64_t                                     rows_consumed{0};
    int                                         rg{0};   // next row-group to read
    int                                         num_rg{0};
    int                                         file_idx{0};

    int32_t vertex_count() const {
        auto col = std::static_pointer_cast<arrow::Int32Array>(
            batch->column(VERTEX_COUNT_COL)->chunk(0));
        return col->Value(row);
    }

    // Fill buffer from the next row group(s)
    bool refill() {
        if (rg >= num_rg) return false;
        std::vector<int> rgs;
        // Grab enough row groups to fill at least MERGE_BATCH_ROWS rows
        int64_t acc = 0;
        while (rg < num_rg && acc < MERGE_BATCH_ROWS) {
            rgs.push_back(rg++);
            acc += reader->parquet_reader()
                        ->metadata()->RowGroup(rgs.back())->num_rows();
        }
        CHECK_ARROW(reader->ReadRowGroups(rgs, &batch));
        // Flatten chunked arrays for simpler random access below
        ASSIGN_OR_THROW(batch, batch->CombineChunks());
        row = 0;
        return batch->num_rows() > 0;
    }
};

// Min-heap comparator (we want the smallest vertex_count at the top)
struct HeapCmp {
    bool operator()(const MergeSource* a, const MergeSource* b) const {
        return a->vertex_count() > b->vertex_count(); // max-heap → invert
    }
};

static void merge_files(const std::vector<fs::path>& sorted_files,
                        const fs::path& out_dir,
                        int n_threads) {
    std::cout << "\nPhase 2: k-way merge of " << sorted_files.size()
              << " sorted files\n";

    // Open all readers up-front (parallelised)
    std::vector<std::unique_ptr<MergeSource>> sources(sorted_files.size());

    {
        std::atomic<int> idx{0};
        std::vector<std::thread> openers;
        for (int t = 0; t < n_threads; ++t) {
            openers.emplace_back([&] {
                for (;;) {
                    int i = idx.fetch_add(1);
                    if (i >= (int)sorted_files.size()) break;
                    auto src = std::make_unique<MergeSource>();
                    src->file_idx = i;

                    std::shared_ptr<arrow::io::MemoryMappedFile> mmap;
                    ASSIGN_OR_THROW(mmap, arrow::io::MemoryMappedFile::Open(
                        sorted_files[i].string(), arrow::io::FileMode::READ));

                    {
                        parquet::arrow::FileReaderBuilder fb;
                        CHECK_ARROW(fb.Open(mmap));
                        fb.memory_pool(arrow::default_memory_pool())
                          ->properties(make_reader_props(MERGE_BATCH_ROWS));
                        CHECK_ARROW(fb.Build(&src->reader));
                    }

                    src->num_rg = src->reader->parquet_reader()
                                            ->metadata()->num_row_groups();
                    src->total_rows = src->reader->parquet_reader()
                                               ->metadata()->num_rows();

                    if (src->refill())
                        sources[i] = std::move(src);
                }
            });
        }
        for (auto& t : openers) t.join();
    }
    std::cout << "  All readers opened.\n";

    // Retrieve schema from the first valid source
    std::shared_ptr<arrow::Schema> schema;
    for (auto& s : sources) {
        if (s) { CHECK_ARROW(s->reader->GetSchema(&schema)); break; }
    }

    // Build heap
    std::priority_queue<MergeSource*, std::vector<MergeSource*>, HeapCmp> heap;
    for (auto& s : sources)
        if (s) heap.push(s.get());

    // Output file management
    fs::create_directories(out_dir);
    auto writer_props = make_writer_props();
    auto arrow_props  = make_arrow_writer_props();

    int    out_file_idx  = 0;
    int64_t rows_in_file = 0;
    int64_t total_written = 0;

    std::shared_ptr<arrow::io::FileOutputStream>  out_stream;
    std::unique_ptr<parquet::arrow::FileWriter>   file_writer;

    auto open_next_output = [&]() {
        char buf[32];
        std::snprintf(buf, sizeof(buf), "sorted-%04d.parquet", out_file_idx++);
        fs::path p = out_dir / buf;
        std::cout << "  Writing: " << p.filename().string() << "\n";
        ASSIGN_OR_THROW(out_stream, arrow::io::FileOutputStream::Open(p.string()));
        ASSIGN_OR_THROW(file_writer,
            parquet::arrow::FileWriter::Open(*schema,
                arrow::default_memory_pool(),
                out_stream, writer_props, arrow_props));
        rows_in_file = 0;
    };
    open_next_output();

    // Accumulate a write-batch of OUTPUT_RG_ROWS rows before flushing
    std::vector<std::shared_ptr<arrow::Array>> columns(schema->num_fields());
    std::vector<std::vector<int32_t>>          col_bufs(schema->num_fields());
    for (auto& v : col_bufs) v.reserve(OUTPUT_RG_ROWS);

    auto flush_batch = [&](int64_t nrows) {
        if (nrows == 0) return;

        // Build record batch from raw int32 vectors
        arrow::Int32Builder builder;
        std::vector<std::shared_ptr<arrow::Array>> arrs;
        arrs.reserve(schema->num_fields());

        for (int c = 0; c < schema->num_fields(); ++c) {
            builder.Reset();
            CHECK_ARROW(builder.AppendValues(col_bufs[c].data(), nrows));
            std::shared_ptr<arrow::Array> arr;
            CHECK_ARROW(builder.Finish(&arr));
            arrs.push_back(std::move(arr));
            col_bufs[c].clear();
        }
        auto batch = arrow::RecordBatch::Make(schema, nrows, arrs);
        CHECK_ARROW(file_writer->NewRowGroup());
        for (int c = 0; c < schema->num_fields(); ++c)
            CHECK_ARROW(file_writer->WriteColumnChunk(*batch->column(c)));

        rows_in_file  += nrows;
        total_written += nrows;

        if (total_written % (OUTPUT_FILE_ROWS / 10) == 0)
            std::cout << "  " << total_written / 1'000'000 << "M rows written\n"
                      << std::flush;
    };

    int64_t  batch_rows = 0;

    // Merge loop
    while (!heap.empty()) {
        MergeSource* src = heap.top();
        heap.pop();

        // Extract one row into column buffers
        for (int c = 0; c < schema->num_fields(); ++c) {
            auto col = std::static_pointer_cast<arrow::Int32Array>(
                src->batch->column(c)->chunk(0));
            col_bufs[c].push_back(col->Value(src->row));
        }
        ++src->row;
        ++src->rows_consumed;
        ++batch_rows;

        // Refill this source if its buffer is exhausted
        if (src->row >= src->batch->num_rows()) {
            if (src->refill())
                heap.push(src);
            // else source is exhausted — don't re-insert
        } else {
            heap.push(src);
        }

        // Flush row group
        if (batch_rows == OUTPUT_RG_ROWS) {
            flush_batch(batch_rows);
            batch_rows = 0;
        }

        // Roll to next output file if needed
        if (rows_in_file >= OUTPUT_FILE_ROWS) {
            flush_batch(batch_rows);
            batch_rows = 0;
            CHECK_ARROW(file_writer->Close());
            CHECK_ARROW(out_stream->Close());
            open_next_output();
        }
    }

    // Flush remaining rows
    flush_batch(batch_rows);
    CHECK_ARROW(file_writer->Close());
    CHECK_ARROW(out_stream->Close());

    std::cout << "\nMerge complete. " << total_written
              << " rows written across " << out_file_idx << " output file(s).\n";
}

// ─────────────────────────────────────────────────────────────────────────────
// main
// ─────────────────────────────────────────────────────────────────────────────
static void usage(const char* argv0) {
    std::cerr
        << "Usage: " << argv0 << "\n"
        << "  --input   <dir>   Directory containing ws-5d-reflexive-*.parquet\n"
        << "  --tmp     <dir>   Temporary directory for phase-1 sorted files\n"
        << "  --output  <dir>   Output directory for globally sorted files\n"
        << " [--threads <n>]   Thread count (default: hardware_concurrency)\n"
        << " [--skip-phase1]   Reuse existing tmp dir (resume after crash)\n";
}

int main(int argc, char** argv) {
    std::string in_dir, tmp_dir, out_dir;
    int  n_threads   = (int)std::thread::hardware_concurrency();
    bool skip_phase1 = false;

    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if      (a == "--input"       && i+1 < argc) in_dir     = argv[++i];
        else if (a == "--tmp"         && i+1 < argc) tmp_dir    = argv[++i];
        else if (a == "--output"      && i+1 < argc) out_dir    = argv[++i];
        else if (a == "--threads"     && i+1 < argc) n_threads  = std::stoi(argv[++i]);
        else if (a == "--skip-phase1")               skip_phase1 = true;
        else { usage(argv[0]); return 1; }
    }
    if (in_dir.empty() || tmp_dir.empty() || out_dir.empty()) {
        usage(argv[0]);
        return 1;
    }

    // Initialize Arrow compute kernel registry
    CHECK_ARROW(arrow::compute::Initialize());

    // Tell Arrow's thread pool how many threads to use
    CHECK_ARROW(arrow::SetCpuThreadPoolCapacity(n_threads));

    // ── Collect input files ───────────────────────────────────────────────────
    std::vector<fs::path> input_files;
    for (auto& e : fs::directory_iterator(in_dir))
        if (e.path().extension() == ".parquet")
            input_files.push_back(e.path());
    std::sort(input_files.begin(), input_files.end());

    if (input_files.empty()) {
        std::cerr << "No .parquet files found in: " << in_dir << "\n";
        return 1;
    }

    std::cout << "Found " << input_files.size() << " input files.\n"
              << "Threads: " << n_threads << "\n\n";

    // ── Phase 1 ───────────────────────────────────────────────────────────────
    fs::create_directories(tmp_dir);

    std::vector<fs::path> sorted_files;
    sorted_files.reserve(input_files.size());
    for (auto& p : input_files)
        sorted_files.push_back(fs::path(tmp_dir) / p.filename());

    if (!skip_phase1) {
        std::cout << "Phase 1: sorting " << input_files.size()
                  << " files with " << n_threads << " threads...\n";

        Progress progress((int)input_files.size());
        std::vector<std::future<void>> futures;

        ThreadPool pool(n_threads);

        std::mutex            err_mtx;
        std::vector<std::string> errors;

        for (int i = 0; i < (int)input_files.size(); ++i) {
            const fs::path& inp = input_files[i];
            const fs::path& out = sorted_files[i];

            futures.push_back(pool.enqueue([&inp, &out, &progress, &errors, &err_mtx] {
                // Skip if already done (e.g. partial rerun)
                if (fs::exists(out) && fs::file_size(out) > 0) {
                    progress.tick(out.filename().string() + " [skipped]");
                    return;
                }
                try {
                    sort_file(inp, out);
                    progress.tick(out.filename().string());
                } catch (std::exception& e) {
                    std::lock_guard<std::mutex> lk(err_mtx);
                    errors.push_back(inp.filename().string() + ": " + e.what());
                }
            }));
        }

        for (auto& f : futures) f.get();
        std::cout << "\n";

        if (!errors.empty()) {
            std::cerr << "\n" << errors.size() << " file(s) failed in phase 1:\n";
            for (auto& e : errors) std::cerr << "  " << e << "\n";
            return 1;
        }

        std::cout << "Phase 1 complete.\n";
    } else {
        std::cout << "Phase 1 skipped (--skip-phase1).\n";
    }

    // ── Phase 2 ───────────────────────────────────────────────────────────────
    // Filter to only actually present sorted files
    std::vector<fs::path> present;
    for (auto& p : sorted_files)
        if (fs::exists(p)) present.push_back(p);

    if (present.empty()) {
        std::cerr << "No sorted files found in tmp dir.\n";
        return 1;
    }

    auto t0 = std::chrono::steady_clock::now();
    merge_files(present, out_dir, n_threads);
    double secs = std::chrono::duration<double>(
        std::chrono::steady_clock::now() - t0).count();
    std::cout << "Phase 2 time: " << std::fixed << std::setprecision(1)
              << secs << "s\n";

    return 0;
}
