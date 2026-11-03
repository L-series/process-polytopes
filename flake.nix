{
  description = "Reproducible PALP build environment";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";

  outputs = { self, nixpkgs }:
    let
      system = "x86_64-linux";
      pkgs = nixpkgs.legacyPackages.${system};
    in {
      devShells.${system} = {
        default = pkgs.mkShell {
          packages = with pkgs; [
            gcc
            gnumake
            clang-tools
            prettier
            shfmt
            git
            python3
          ];
        };
        profiling = pkgs.mkShell {
          packages = [ pkgs.gcc pkgs.binutils pkgs.python3 pkgs.git pkgs.valgrind ];
        };
        # Model checking of the CUDA sources (verification/gpu) with CBMC.
        gpu-verify = pkgs.mkShell {
          packages = with pkgs; [ gcc gnumake python3 git cbmc ];
        };
      };
    };
}
