{
  # Dev shell for the implementation. The nixpkgs revision matches the specification repository's
  # flake at the pinned commit, so both shells come from one package set. W1 adds the pinned
  # PostgreSQL here once its version is settled in docs/decisions.md.
  description = "provisiond-rust development shell";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/aff8a0b28396750446e5537a96461bc4facdb287";

  outputs = { self, nixpkgs }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];
      forAll = f: nixpkgs.lib.genAttrs systems (system: f nixpkgs.legacyPackages.${system});
    in {
      devShells = forAll (pkgs: {
        default = pkgs.mkShell {
          packages = with pkgs; [ cargo rustc clippy rustfmt python3 git ];
        };
      });
    };
}
