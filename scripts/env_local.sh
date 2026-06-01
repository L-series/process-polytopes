#!/usr/bin/env bash
# Source this file to use the user-local process-polytopes toolchain.

export CONDA_PREFIX="$HOME/.local/share/micromamba/envs/process-polytopes"
export PATH="$CONDA_PREFIX/bin:$HOME/.elan/bin:$PATH"
export PKG_CONFIG_PATH="$CONDA_PREFIX/lib/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"
export CMAKE_PREFIX_PATH="$CONDA_PREFIX${CMAKE_PREFIX_PATH:+:$CMAKE_PREFIX_PATH}"
export LD_LIBRARY_PATH="$CONDA_PREFIX/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
