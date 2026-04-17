#include <assert.h>

#define DIM5_MAX_SIMPLEX_SIZE 5
#define DIM5_MAX_SELECTIONS 16

typedef long Long;

typedef struct {
    int d;
    int N;
    Long w[DIM5_MAX_SIMPLEX_SIZE];
} Dim5WeightEntry;

typedef struct {
    Dim5WeightEntry items[DIM5_MAX_SELECTIONS];
    int count;
} Dim5WeightPool;

static void AppendDim5WeightEntry(Dim5WeightPool *pool,
                                  const Dim5WeightEntry *entry) {
    assert(pool->count >= 0);
    assert(pool->count < DIM5_MAX_SELECTIONS);
    pool->items[pool->count++] = *entry;
}

static void AppendDim5SelectedWeight(const Dim5WeightEntry *source,
                                     const int *selected_indices,
                                     int selected_count,
                                     Dim5WeightPool *pool) {
    Dim5WeightEntry entry;
    int i;

    entry.d = source->d;
    entry.N = selected_count;
    for (i = 0; i < selected_count; i++)
        entry.w[i] = source->w[selected_indices[i]];
    AppendDim5WeightEntry(pool, &entry);
}

static void EnumerateDim5Selections(const Dim5WeightEntry *source,
                                    int selected_count, int *selected_indices,
                                    int depth, Dim5WeightPool *pool) {
    int start, i;

    if (depth == selected_count) {
        AppendDim5SelectedWeight(source, selected_indices, selected_count,
                                 pool);
        return;
    }

    start = selected_indices[depth - 1] + 1;
    for (i = start; i <= source->N - (selected_count - depth); i++) {
        selected_indices[depth] = i;
        EnumerateDim5Selections(source, selected_count, selected_indices,
                                depth + 1, pool);
    }
}

static void SelectDim5Weights(const Dim5WeightEntry *source,
                              int selected_count, Dim5WeightPool *pool) {
    int selected_indices[DIM5_MAX_SIMPLEX_SIZE];
    int i;

    if (selected_count == 0) {
        AppendDim5SelectedWeight(source, selected_indices, selected_count,
                                 pool);
        return;
    }

    for (i = 0; i <= source->N - selected_count; i++) {
        selected_indices[0] = i;
        EnumerateDim5Selections(source, selected_count, selected_indices, 1,
                                pool);
    }
}

static int ChooseInt(int n, int k) {
    int i;
    int result = 1;

    if (k < 0 || k > n)
        return 0;
    if (k > (n - k))
        k = n - k;
    for (i = 1; i <= k; i++)
        result = (result * (n - k + i)) / i;
    return result;
}

static void CheckSelectionCase(int source_size, int selected_count) {
    Dim5WeightEntry source;
    Dim5WeightPool pool;
    int i;

    source.d = 42;
    source.N = source_size;
    for (i = 0; i < source.N; i++)
        source.w[i] = (Long)(i + 1);

    pool.count = 0;
    SelectDim5Weights(&source, selected_count, &pool);

    assert(pool.count == ChooseInt(source.N, selected_count));

    for (i = 0; i < pool.count; i++) {
        int j;
        int source_index = 0;

        assert(pool.items[i].d == source.d);
        assert(pool.items[i].N == selected_count);

        for (j = 0; j + 1 < pool.items[i].N; j++)
            assert(pool.items[i].w[j] < pool.items[i].w[j + 1]);

        for (j = 0; j < pool.items[i].N; j++) {
            while ((source_index < source.N) &&
                   (source.w[source_index] != pool.items[i].w[j]))
                source_index++;
            assert(source_index < source.N);
            source_index++;
        }
    }

    for (i = 0; i < pool.count; i++) {
        int j;

        for (j = i + 1; j < pool.count; j++) {
            int k;
            int identical = 1;

            for (k = 0; k < selected_count; k++)
                if (pool.items[i].w[k] != pool.items[j].w[k]) {
                    identical = 0;
                    break;
                }
            assert(!identical);
        }
    }
}

void harness(void) {
    int source_size;

    for (source_size = 1; source_size <= 4; source_size++) {
        int selected_count;

        for (selected_count = 0; selected_count <= source_size;
             selected_count++)
            CheckSelectionCase(source_size, selected_count);
    }
}