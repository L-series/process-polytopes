cmake_minimum_required(VERSION 3.20)
project(polytope_classifier C CXX)

set(CMAKE_C_STANDARD   11)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

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

# ── Main executable ────────────────────────────────────────────────────────
add_executable(classifier classifier.cpp)

target_compile_options(classifier PRIVATE
    -O3 -march=native -flto -funroll-loops
    -DPOLY_Dmax=5
    -DPALP_FAST_ASSERT
    -DPALP_THREADSAFE
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

# ── Install ────────────────────────────────────────────────────────────────
install(TARGETS classifier RUNTIME DESTINATION bin)
