{
  description = "Development shells for formal verification, GPU benchmarking, Lean, and paper builds";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
  };

  outputs = { self, nixpkgs }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs {
        inherit system;
        config.allowUnfree = true;
      };
      cudaPkgs = pkgs.cudaPackages_12_6;
      rocmPkgs = pkgs.rocmPackages;
      libdrmLibDir = "${pkgs.libdrm}/lib";
      gpuCommonPackages = with pkgs; [
        coreutils
        findutils
        gawk
        gcc
        git
        gnugrep
        gnused
        gnumake
        jq
        pciutils
        util-linux
        which
      ];
    in
    {
      devShells.${system} = {
        proofing = pkgs.mkShell {
          name = "proofing";

          packages = with pkgs; [
            # Bounded model checkers
            cbmc              # C/C++ bounded model checking (plans 1.1-1.4, 1.7, 2.6)

            # Abstract interpretation & deductive verification
            framac             # Frama-C: Eva + WP plugins (plans 2.2-2.5)

            # SMT solvers (backends for CBMC and Frama-C)
            z3
            cvc5

            # Build tools (needed to compile harnesses for syntax checking)
            gcc
            gnumake
          ];

          shellHook = ''
            echo ""
            echo "=== Proofing devshell ==="
            echo "  cbmc    $(cbmc --version 2>&1 | head -1)"
            echo "  frama-c $(frama-c -version 2>&1 | head -1)"
            echo "  z3      $(z3 --version 2>&1)"
            echo ""
            echo "Run verification:  ./src/verify/run_verification.sh"
            echo ""
          '';
        };

        lean = pkgs.mkShell {
          name = "lean";

          packages = with pkgs; [
            lean4
            elan
            git
          ];

          shellHook = ''
            echo ""
            echo "=== Lean devshell ==="
            if command -v lean >/dev/null 2>&1; then
              echo "  lean    $(lean --version 2>&1 | head -1)"
            fi
            if command -v lake >/dev/null 2>&1; then
              echo "  lake    $(lake --version 2>&1 | head -1)"
            fi
            echo ""
            echo "Lean project:  cd lean && lake build"
            echo ""
          '';
        };

        paper = pkgs.mkShell {
          name = "paper";

          packages = with pkgs; [
            texlivePackages.latexmk
            texliveFull
          ];

          shellHook = ''
            echo ""
            echo "=== Paper devshell ==="
            echo "  latexmk $(latexmk -v | head -1)"
            echo "  pdflatex $(pdflatex --version | head -1)"
            echo ""
            echo "Build paper:  cd paper && latexmk -pdf draft.tex"
            echo ""
          '';
        };

        cuda = pkgs.mkShell {
          name = "cuda";

          packages = gpuCommonPackages ++ [
            cudaPkgs.cuda_cudart
            cudaPkgs.cuda_cupti
            cudaPkgs.cuda_nvcc
            cudaPkgs.cuda_nvrtc
          ];

          shellHook = ''
            export CUDA_NIXPKGS_SET="cudaPackages_12_6"
            export TYPE3_CUDA_RUNTIME_DIR="$PWD/.type3-cuda-runtime"
            export TYPE3_FRONTIER_BENCH_DIR="$PWD/.type3-frontier-bench"

            if [ -d /run/opengl-driver/lib ]; then
              export LD_LIBRARY_PATH="/run/opengl-driver/lib''${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
            fi

            echo ""
            echo "=== CUDA devshell ==="
            echo "  nvcc    $(nvcc --version | tail -n1)"
            if command -v nvidia-smi >/dev/null 2>&1; then
              echo "  gpu     $(nvidia-smi --query-gpu=name,driver_version,memory.total --format=csv,noheader | head -n1)"
            else
              echo "  gpu     nvidia-smi unavailable on host PATH"
            fi
            echo ""
            echo "Probe stack:   scripts/probe_gpu_stack.sh cuda"
            echo "Build runtime: scripts/build_type3_cuda_runtime.sh"
            echo "Build PALP:    make -C PALP -f GNUmakefile cws.x"
            echo "Benchmark:     scripts/benchmark_type3_frontier.sh"
            echo ""
          '';
        };

        rocm = pkgs.mkShell {
          name = "rocm";

          packages = gpuCommonPackages ++ [
            rocmPkgs.amdsmi
            rocmPkgs.clr
            rocmPkgs.hipcc
            rocmPkgs.llvm.clang
            rocmPkgs.rocminfo
            rocmPkgs.rocm-device-libs
            rocmPkgs.rocm-runtime
            rocmPkgs.rocm-smi
          ];

          shellHook = ''
            export HIP_PLATFORM=amd
            export HIP_COMPILER=clang
            export HIP_PATH="${rocmPkgs.hipcc}"
            export ROCM_PATH="${rocmPkgs.clr}"
            export HSA_PATH="${rocmPkgs.rocm-runtime}"
            export HIP_CLANG_PATH="${rocmPkgs.llvm.clang}/bin"
            export HIP_DEVICE_LIB_PATH="${rocmPkgs.rocm-device-libs}/amdgcn/bitcode"
            export LD_LIBRARY_PATH="${libdrmLibDir}:''${HSA_PATH}/lib''${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

            echo ""
            echo "=== ROCm devshell ==="
            echo "  hipcc   $(command -v hipcc)"
            echo "  clang   $HIP_CLANG_PATH/clang++"
            echo "  device-libs $HIP_DEVICE_LIB_PATH"
            echo ""
            echo "Probe stack: scripts/probe_gpu_stack.sh rocm"
            echo ""
          '';
        };
      };
    };
}
