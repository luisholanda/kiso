{
  inputs = {
    flake-utils.follows = "cargo2nix/flake-utils";
    nixpkgs.follows = "cargo2nix/nixpkgs";

    cargo2nix = {
      url = "github:cargo2nix/cargo2nix";
      inputs.rust-overlay.follows = "rust-overlay";
    };
    pre-commit-hooks = {
      url = "github:cachix/pre-commit-hooks.nix";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.flake-utils.follows = "flake-utils";
    };
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.flake-utils.follows = "flake-utils";
    };
  };

  outputs = inputs:
    with inputs;
      flake-utils.lib.eachDefaultSystem (
        system: let
          inherit (pkgs.stdenv) isLinux;
          inherit (nixpkgs.lib) optionalString;

          pkgs = import nixpkgs {
            inherit system;
            overlays = [
              (import rust-overlay)
              cargo2nix.overlays.default
            ];
          };

          rustVersion = "1.77.0";

          rustPkgs = pkgs.rustBuilder.makePackageSet {
            inherit rustVersion;
            packageFun = import ./Cargo.nix;
            extraRustComponents = ["clippy"];
            packageOverrides = pkgs: pkgs.rustBuilder.overrides.all ++ [
              (pkgs.rustBuilder.rustLib.makeOverride {
                name = "glommio";
                overrideAttrs = drv: {
                  GLOMMIO_LIBURING_DIR = "./liburing";
                };
              })
            ];
          };

          pre-commit = pre-commit-hooks.lib.${system}.run {
            src = ./.;
            hooks = {
              rustfmt.enable = true;
              rustfmt.pass_filenames = pkgs.lib.mkForce true;
              rust-clippy = {
                enable = true;
                name = "clippy";
                entry = "cargo clippy --offline --all-features --all-targets -- -D warnings";
                files = "\\.rs$";
                pass_filenames = false;
              };
              rust-tests = {
                enable = true;
                name = "tests";
                entry = "cargo test --offline --all-features --all-targets";
                files = "\\.rs$";
                pass_filenames = false;
              };
            };
            tools = pkgs // {
              inherit (pkgs.rust-bin.nightly.latest) rustfmt;
            };
          };

        in rec {
          checks.kiso-tests = rustPkgs.workspace.kiso { compileMode = "test"; };

          devShells.default = rustPkgs.workspaceShell {
            inherit (pre-commit) shellHook;

            packages = [
              pkgs.rust-bin.nightly.latest.rustfmt
              cargo2nix.packages.${system}.cargo2nix
            ];

            RUSTC_FLAGS = "'-C linker=${pkgs.clang}/bin/clang'" 
              + (optionalString isLinux "'-C link-arg=-fuse-ld=${pkgs.mold}/bin/mold'");
          };

          packages = {
            kiso = rustPkgs.workspace.kiso {};
            default = packages.kiso;
          };
        }
      );
}
