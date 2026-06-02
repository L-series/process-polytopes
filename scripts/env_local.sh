#!/usr/bin/env bash
# Source this file to use the user-local process-polytopes toolchain.

export CONDA_PREFIX="$HOME/.local/share/micromamba/envs/process-polytopes"
export PATH="$CONDA_PREFIX/bin:$HOME/.elan/bin:$PATH"
export PKG_CONFIG_PATH="$CONDA_PREFIX/lib/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"
export CMAKE_PREFIX_PATH="$CONDA_PREFIX${CMAKE_PREFIX_PATH:+:$CMAKE_PREFIX_PATH}"
export LD_LIBRARY_PATH="$CONDA_PREFIX/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

# Return the CUDA device index that SLURM allocated to this process.
# When SLURM manages GPUs via CUDA_VISIBLE_DEVICES (the common case) the
# runtime remaps the allocated physical GPU(s) to indices 0, 1, ..., so
# device 0 is always the correct choice.  When SLURM exposes the physical
# ordinal only through SLURM_JOB_GPUS (no CUDA_VISIBLE_DEVICES), use the
# first ordinal in that list as the device index directly.
_slurm_cuda_device() {
    if [[ -n "${CUDA_VISIBLE_DEVICES:-}" && "${CUDA_VISIBLE_DEVICES}" != "NoDevFiles" ]]; then
        echo 0
    elif [[ -n "${SLURM_JOB_GPUS:-}" ]]; then
        echo "${SLURM_JOB_GPUS%%,*}"
    else
        echo 0
    fi
}
