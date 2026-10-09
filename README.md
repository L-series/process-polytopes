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

Nix is a package manager and build system that can describe development tools and dependencies declaratively. This repository's flake and lockfile pin the build environment so it can be recreated consistently. Run `nix develop -c ./scripts/lint.sh` to check C, Markdown, and shell formatting in both repositories; add `--fix` to apply formatting. Build PALP with `nix develop -c make -C PALP all-dims`. See the [Nix documentation](https://nix.dev/) and [flake overview](https://nix.dev/concepts/flakes) for more.

### CWS regression tests

Run `nix develop -i -c ./scripts/test-cws.sh` to build the four required CPU executables and check CWS counts, combination types, malformed input, and vertex normal forms. Add `--case 4d` to run one generation case. See [tests/README.md](tests/README.md) for fixture provenance and coverage. CI runs these tests on every push and pull request; the full `all-dims` build retains its source-change gate.
