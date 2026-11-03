// Host-only builds of src/gpu headers for verification (no CUDA toolkit): only
// check_cuda refers to the runtime, and the host checks never call it.
#pragma once
typedef int cudaError_t;
static const cudaError_t cudaSuccess = 0;
inline const char *cudaGetErrorString(cudaError_t) { return "CUDA runtime unavailable in host verification"; }
