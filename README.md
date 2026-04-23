# process-polytopes

## Getting Started

### Initializing Git Submodules

After cloning this repository, initialize and update the git submodules with:

```bash
git submodule init
git submodule update
```

Alternatively, you can clone the repository with submodules in one step:

```bash
git clone --recurse-submodules <repository-url>
```

## Nix GPU Shells

The flake now exposes dedicated GPU development shells:

- `cuda`: CUDA runtime builds and type-3 frontier benchmarks
- `rocm`: ROCm probing and HIP compiler checks

On this host, direct `nix develop` is blocked by a broken default build-root setup.
Use the local wrapper instead:

```bash
./scripts/nix_develop_local.sh cuda
./scripts/nix_develop_local.sh rocm
```

Useful checks:

```bash
./scripts/nix_develop_local.sh cuda --command scripts/probe_gpu_stack.sh cuda
./scripts/nix_develop_local.sh rocm --command scripts/probe_gpu_stack.sh rocm
```