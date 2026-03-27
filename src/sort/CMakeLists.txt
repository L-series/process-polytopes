
cmake_minimum_required(VERSION 3.20)
project(sort_parquet CXX)
 
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
 
# ── Release build with aggressive optimisation ─────────────────────────────────
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()
 
add_compile_options(
  -O3
  -march=native          # SIMD: AVX2/AVX-512 if available
  -funroll-loops
  -fomit-frame-pointer
  -DARROW_WITH_OPENMP    # tell Arrow headers OMP is present
)
 
# ── Dependencies ───────────────────────────────────────────────────────────────
find_package(Arrow        REQUIRED)
find_package(ArrowCompute REQUIRED)  # compute kernels (sort_indices etc.)
find_package(ArrowDataset REQUIRED)  # for scanning
find_package(Parquet      REQUIRED)
find_package(OpenMP       REQUIRED)  # parallel STL + Arrow's OMP kernels
 
# ── Executable ────────────────────────────────────────────────────────────────
add_executable(sort_parquet sort_parquet.cpp)
 
target_link_libraries(sort_parquet PRIVATE
  Arrow::arrow_shared
  ArrowCompute::arrow_compute_shared
  Parquet::parquet_shared
  ArrowDataset::arrow_dataset_shared
  OpenMP::OpenMP_CXX
)
 
# ── Install ───────────────────────────────────────────────────────────────────
install(TARGETS sort_parquet RUNTIME DESTINATION bin)
 
