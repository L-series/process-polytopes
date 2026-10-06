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

### Commit message hook

Enable the native Git hook with `git config core.hooksPath .githooks`.

### Reproducible build environments

Nix is a package manager and build system that can describe development tools and dependencies declaratively. This repository's flake and lockfile pin the build environment so it can be recreated consistently. Run `nix develop` to enter it, then build PALP with `make -C PALP all-dims`. See the [Nix documentation](https://nix.dev/) and [flake overview](https://nix.dev/concepts/flakes) for more.
