#ifndef TYPE3_BOUNDS_BENCH_COMMON_H
#define TYPE3_BOUNDS_BENCH_COMMON_H

#include <stddef.h>
#include <stdint.h>

#if defined(__CUDACC__)
#define T3B_HD __host__ __device__
#else
#define T3B_HD
#endif

#if defined(__GNUC__)
#define T3B_UNUSED __attribute__((unused))
#else
#define T3B_UNUSED
#endif

enum {
    kType3BoundsVersion = 2,
    kType3BoundsMagic = 0x31423354u,
    kType3BoundsMaxConstraints = 16,
};

enum {
    kType3BoundsDatasetSynthetic = 1,
    kType3BoundsDatasetCoordTiming = 2,
};

enum {
    kType3BoundsFlagSeedEmpty = 1u << 0,
    kType3BoundsFlagTightenEmpty = 1u << 1,
    kType3BoundsFlagZeroFail = 1u << 2,
    kType3BoundsFlagSurvived = 1u << 3,
    kType3BoundsFlagSingleton = 1u << 4,
};

typedef struct {
    uint32_t magic;
    uint32_t version;
    uint64_t seed;
    uint32_t dataset_kind;
    uint32_t reserved;
    uint32_t job_count;
    uint32_t max_constraints;
    uint32_t seed_empty_pct;
    uint32_t tighten_empty_pct;
    uint32_t survive_pct;
} Type3BoundsFileHeader;

typedef struct {
    int32_t seed_low;
    int32_t seed_upp;
    int32_t seed_r;
    uint32_t constraint_count;
    int32_t low[kType3BoundsMaxConstraints];
    int32_t upp[kType3BoundsMaxConstraints];
    int32_t r[kType3BoundsMaxConstraints];
    int32_t zero_x[kType3BoundsMaxConstraints];
    int32_t zero_xmax[kType3BoundsMaxConstraints];
} Type3BoundsJob;

typedef struct {
    int32_t xmin;
    int32_t xmax;
    uint32_t flags;
    uint32_t steps;
} Type3BoundsResult;

typedef struct {
    uint64_t ordinal;
    Type3BoundsJob job;
    Type3BoundsResult expected;
} Type3BoundsRecord;

T3B_HD T3B_UNUSED static inline int64_t Type3FloorDiv(int64_t n, int64_t d) {
    int64_t q = n / d;
    int64_t r = n % d;

    if (r < 0)
        --q;
    return q;
}

T3B_HD T3B_UNUSED static inline Type3BoundsResult
EvaluateType3BoundsJob(const Type3BoundsJob *job) {
    Type3BoundsResult result;
    int64_t xmin, xmax;
    int32_t r;

    result.xmin = 0;
    result.xmax = -1;
    result.flags = 0;
    result.steps = 1;

    r = job->seed_r;
    if (r == 1) {
        xmin = job->seed_low;
        xmax = job->seed_upp;
    } else if (r > 0) {
        xmin = -Type3FloorDiv(-(int64_t)job->seed_low, r);
        xmax = Type3FloorDiv(job->seed_upp, r);
    } else if (r == -1) {
        xmin = -job->seed_upp;
        xmax = -job->seed_low;
    } else if (r < 0) {
        xmin = -Type3FloorDiv(job->seed_upp, -(int64_t)r);
        xmax = Type3FloorDiv(-(int64_t)job->seed_low, -(int64_t)r);
    } else {
        result.flags = kType3BoundsFlagSeedEmpty;
        return result;
    }

    if (xmin > xmax) {
        result.flags = kType3BoundsFlagSeedEmpty;
        result.xmin = (int32_t)xmin;
        result.xmax = (int32_t)xmax;
        return result;
    }

    if (xmin == xmax)
        result.flags |= kType3BoundsFlagSingleton;

    for (uint32_t i = 0; i < job->constraint_count; ++i) {
        ++result.steps;
        r = job->r[i];
        if (r > 0) {
            if (r == 1) {
                if (xmax > job->upp[i])
                    xmax = job->upp[i];
                if (xmin < job->low[i])
                    xmin = job->low[i];
            } else {
                int64_t new_xmax = Type3FloorDiv(job->upp[i], r);
                int64_t new_xmin = -Type3FloorDiv(-(int64_t)job->low[i], r);

                if (xmax > new_xmax)
                    xmax = new_xmax;
                if (xmin < new_xmin)
                    xmin = new_xmin;
            }
        } else if (r < 0) {
            if (r == -1) {
                if (xmax > (-(int64_t)job->low[i]))
                    xmax = -job->low[i];
                if (xmin < (-(int64_t)job->upp[i]))
                    xmin = -job->upp[i];
            } else {
                int64_t new_xmax = Type3FloorDiv(-(int64_t)job->low[i],
                                                 -(int64_t)r);
                int64_t new_xmin = -Type3FloorDiv(job->upp[i], -(int64_t)r);

                if (xmax > new_xmax)
                    xmax = new_xmax;
                if (xmin < new_xmin)
                    xmin = new_xmin;
            }
        } else if ((job->zero_x[i] < 0) || (job->zero_x[i] > job->zero_xmax[i])) {
            result.flags |= kType3BoundsFlagZeroFail;
            result.xmin = (int32_t)xmin;
            result.xmax = (int32_t)xmax;
            return result;
        }

        if (xmin > xmax) {
            result.flags |= kType3BoundsFlagTightenEmpty;
            result.xmin = (int32_t)xmin;
            result.xmax = (int32_t)xmax;
            return result;
        }

        if (xmin == xmax)
            result.flags |= kType3BoundsFlagSingleton;
    }

    result.flags |= kType3BoundsFlagSurvived;
    result.xmin = (int32_t)xmin;
    result.xmax = (int32_t)xmax;
    return result;
}

T3B_HD T3B_UNUSED static inline int Type3BoundsResultsEqual(
    const Type3BoundsResult *lhs, const Type3BoundsResult *rhs) {
    return (lhs->xmin == rhs->xmin) && (lhs->xmax == rhs->xmax) &&
           (lhs->flags == rhs->flags) && (lhs->steps == rhs->steps);
}

T3B_HD T3B_UNUSED static inline uint64_t Type3BoundsChecksumUpdate(
    uint64_t checksum, const Type3BoundsResult *result) {
    checksum ^= (uint32_t)result->xmin;
    checksum *= 1099511628211ull;
    checksum ^= (uint32_t)result->xmax;
    checksum *= 1099511628211ull;
    checksum ^= result->flags;
    checksum *= 1099511628211ull;
    checksum ^= result->steps;
    checksum *= 1099511628211ull;
    return checksum;
}

#endif