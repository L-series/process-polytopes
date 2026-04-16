{
  description = "Formal verification devshell for polytope classifier & PALP";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
  };

  outputs = { self, nixpkgs }:
    let
      system = "x86_64-linux";
      pkgs = nixpkgs.legacyPackages.${system};
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
      };
    };
}
