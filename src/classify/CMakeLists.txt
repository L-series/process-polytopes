cmake_minimum_required(VERSION 3.20)
project(polytope_classifier C CXX)

set(CMAKE_C_STANDARD   11)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

option(ENABLE_CUDA "Build CUDA geometry backend" OFF)

if(ENABLE_CUDA)
    set(PROCESS_POLYTOPES_CUDA_ARCHITECTURES "75-real;89-real;120-real" CACHE STRING
            "CUDA architectures for process-polytopes GPU builds")
    if(NOT DEFINED CMAKE_CUDA_ARCHITECTURES OR
         CMAKE_CUDA_ARCHITECTURES STREQUAL "" OR
         CMAKE_CUDA_ARCHITECTURES STREQUAL "75")
        set(CMAKE_CUDA_ARCHITECTURES "${PROCESS_POLYTOPES_CUDA_ARCHITECTURES}" CACHE STRING
                "CUDA architectures" FORCE)
    endif()
    enable_language(CUDA)
endif()

# ── Build type ─────────────────────────────────────────────────────────────
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

# ── Common optimisation flags ──────────────────────────────────────────────
# ── PALP as a static library ──────────────────────────────────────────────
# We compile only the source files needed for the NF computation path.
# poly.c / cws.c / nef.c / mori.c / class.c are excluded (they have main()).
set(PALP_DIR "${CMAKE_SOURCE_DIR}/../../PALP")

set(PALP_SOURCES
    ${PALP_DIR}/Coord.c
    ${PALP_DIR}/Rat.c
    ${PALP_DIR}/Vertex.c
    ${PALP_DIR}/Polynf.c
    ${PALP_DIR}/LG.c
)

# PALP needs these defines for our use case:
#  POLY_Dmax=5        → 5-dimensional polytopes
#  PALP_FAST_ASSERT   → keep side effects in assert() without aborting
#  PALP_THREADSAFE    → use __thread for static scratch buffers
set(PALP_DEFINES
    -DPOLY_Dmax=5
    -DPALP_FAST_ASSERT
    -DPALP_THREADSAFE
    -DCEQ_Nmax=2048    # candidate eq list; default 64 is too small for some 5D polytopes
)

add_library(palp STATIC ${PALP_SOURCES})
target_compile_options(palp PRIVATE -O3 -march=native -flto -funroll-loops -fomit-frame-pointer -w)
target_compile_definitions(palp PRIVATE ${PALP_DEFINES})
target_include_directories(palp PUBLIC ${PALP_DIR})

# PALP's global symbols (inFILE, outFILE) need a definition.
# We provide them via a tiny glue file.
configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/palp_globals.c"
    "${CMAKE_BINARY_DIR}/palp_globals.c"
    COPYONLY
)
target_sources(palp PRIVATE "${CMAKE_BINARY_DIR}/palp_globals.c")

# ── Dependencies ───────────────────────────────────────────────────────────
find_package(Arrow   REQUIRED)
find_package(Parquet REQUIRED)
find_package(Threads REQUIRED)
if(ENABLE_CUDA)
  find_package(CUDAToolkit REQUIRED)
endif()

# ── Optional CUDA backend ─────────────────────────────────────────────────
if(ENABLE_CUDA)
  add_library(cuda_geometry STATIC cuda_geometry.cu)
  set_target_properties(cuda_geometry PROPERTIES
      CUDA_STANDARD 17
      CUDA_STANDARD_REQUIRED ON
      CUDA_SEPARABLE_COMPILATION OFF)
  target_compile_options(cuda_geometry PRIVATE
      $<$<COMPILE_LANGUAGE:CUDA>:-O3>)
  target_compile_definitions(cuda_geometry PRIVATE
      POLY_Dmax=5
      PALP_FAST_ASSERT
      PALP_THREADSAFE
      CEQ_Nmax=2048
      CLASSIFIER_ENABLE_CUDA=1)
  target_include_directories(cuda_geometry PRIVATE
      ${PALP_DIR}
      ${CMAKE_CURRENT_SOURCE_DIR})
  target_link_libraries(cuda_geometry PUBLIC
      palp
      CUDA::cudart)
endif()

# ── Main executable ────────────────────────────────────────────────────────
add_executable(classifier classifier.cpp geometry_backend.cpp)

target_compile_options(classifier PRIVATE
    -O3 -march=native -flto -funroll-loops
    -DPOLY_Dmax=5
    -DPALP_FAST_ASSERT
    -DPALP_THREADSAFE
    -DCEQ_Nmax=2048
)
target_compile_definitions(classifier PRIVATE
    CLASSIFIER_ENABLE_CUDA=$<BOOL:${ENABLE_CUDA}>
)

target_include_directories(classifier PRIVATE
    ${PALP_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(classifier PRIVATE
    palp
    Arrow::arrow_shared
    Parquet::parquet_shared
    Threads::Threads
    -flto
)

if(ENABLE_CUDA)
  target_link_libraries(classifier PRIVATE cuda_geometry)

  add_executable(cuda_backend_smoke cuda_backend_smoke.cpp geometry_backend.cpp)
  target_compile_options(cuda_backend_smoke PRIVATE
      -O3 -march=native -funroll-loops
      -DPOLY_Dmax=5
      -DPALP_FAST_ASSERT
      -DPALP_THREADSAFE
      -DCEQ_Nmax=2048)
  target_compile_definitions(cuda_backend_smoke PRIVATE CLASSIFIER_ENABLE_CUDA=1)
  target_include_directories(cuda_backend_smoke PRIVATE
      ${PALP_DIR}
      ${CMAKE_CURRENT_SOURCE_DIR})
  target_link_libraries(cuda_backend_smoke PRIVATE
      cuda_geometry
      palp
      Threads::Threads)

  add_executable(cuda_type3_55_scan cuda_type3_55_scan.cu)
  set_target_properties(cuda_type3_55_scan PROPERTIES
      CUDA_STANDARD 17
      CUDA_STANDARD_REQUIRED ON
      CUDA_SEPARABLE_COMPILATION OFF)
  target_compile_options(cuda_type3_55_scan PRIVATE
      $<$<COMPILE_LANGUAGE:CUDA>:-O3>)
  target_link_libraries(cuda_type3_55_scan PRIVATE CUDA::cudart)

  add_executable(cuda_dim5_cws_scan cuda_dim5_cws_scan.cu)
  set_target_properties(cuda_dim5_cws_scan PROPERTIES
      CUDA_STANDARD 17
      CUDA_STANDARD_REQUIRED ON
      CUDA_SEPARABLE_COMPILATION OFF)
  target_compile_options(cuda_dim5_cws_scan PRIVATE
      $<$<COMPILE_LANGUAGE:CUDA>:-O3>)
  target_include_directories(cuda_dim5_cws_scan PRIVATE ${PALP_DIR})
  target_link_libraries(cuda_dim5_cws_scan PRIVATE CUDA::cudart)
endif()

# ── add_nf executable ──────────────────────────────────────────────────────
add_executable(add_nf add_nf.cpp)

target_compile_options(add_nf PRIVATE
    -O3 -march=native -flto -funroll-loops
    -DPOLY_Dmax=5
    -DPALP_FAST_ASSERT
    -DPALP_THREADSAFE
    -DCEQ_Nmax=2048
)

target_include_directories(add_nf PRIVATE
    ${PALP_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(add_nf PRIVATE
    palp
    Arrow::arrow_shared
    Parquet::parquet_shared
    Threads::Threads
    -flto
)

# ── PALP text CWS to Parquet converter ───────────────────────────────────
add_executable(cws_to_parquet cws_to_parquet.cpp)

target_compile_options(cws_to_parquet PRIVATE
    -O3 -march=native -flto -funroll-loops
    -DPOLY_Dmax=5
    -DPALP_FAST_ASSERT
    -DPALP_THREADSAFE
    -DCEQ_Nmax=2048
)

target_include_directories(cws_to_parquet PRIVATE
    ${PALP_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(cws_to_parquet PRIVATE
    Arrow::arrow_shared
    Parquet::parquet_shared
    Threads::Threads
    -flto
)

# ── Parity helper for CPU/CUDA output comparison ─────────────────────────
add_executable(compare_polytope_outputs compare_polytope_outputs.cpp)

target_compile_options(compare_polytope_outputs PRIVATE
    -O3 -march=native -flto -funroll-loops
)

target_link_libraries(compare_polytope_outputs PRIVATE
    Arrow::arrow_shared
    Parquet::parquet_shared
    Threads::Threads
    -flto
)

# ── Install ────────────────────────────────────────────────────────────────
install(TARGETS classifier add_nf cws_to_parquet compare_polytope_outputs RUNTIME DESTINATION bin)
if(ENABLE_CUDA)
    install(TARGETS cuda_backend_smoke cuda_type3_55_scan cuda_dim5_cws_scan RUNTIME DESTINATION bin)
endif()
