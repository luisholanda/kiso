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
          inherit (nixpkgs.lib) optional;

          pkgs = import nixpkgs {
            inherit system;
            overlays = [
              (import rust-overlay)
              cargo2nix.overlays.default
            ];
          };

          rustVersion = "1.77.0";

          rustcLinkFlags = [
            "-C linker=${pkgs.clang}/bin/clang"
          ] ++ (optional isLinux "-C link-arg=-fuse-ld=${pkgs.mold}/bin/mold");

          rustPkgs = pkgs.rustBuilder.makePackageSet {
            inherit rustVersion rustcLinkFlags;
            packageFun = import ./Cargo.nix;
            extraRustComponents = ["clippy"];
            packageOverrides = pkgs: pkgs.rustBuilder.overrides.all ++ [
              (pkgs.rustBuilder.rustLib.makeOverride {
                name = "glommio";
                overrideAttrs = drv: {
                  GLOMMIO_LIBURING_DIR = "./liburing";
                };
              })
              (pkgs.rustBuilder.rustLib.makeOverride {
                name = "echo";
                overrideAttrs = drv: {
                  nativeBuildInputs = [pkgs.protobuf] ++ drv.nativeBuildInputs;
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

          buildGhzBenchmark = {bin, bench, protoFile}: pkgs.writeShellApplication {
            name = "benchmark-${bin.name}";
            runtimeInputs = with pkgs; [ghz bin];
            text = "sh ${bench} ${bin}/bin/${bin.name} ${protoFile}";
          };
        in rec {
          checks.kiso-tests = rustPkgs.workspace.kiso { compileMode = "test"; };

          devShells.default = rustPkgs.workspaceShell {
            inherit (pre-commit) shellHook;

            packages = with pkgs; [
              ghz
              rust-bin.nightly.latest.rustfmt
              protobuf
            ];

            RUSTC_FLAGS = rustcLinkFlags;
          };

          packages = {
            # Example servers
            echo = rustPkgs.workspace.echo {};

            # Benchmarks
            benchmark-echo = buildGhzBenchmark {
              bin = packages.echo;
              bench = ./examples/echo/benchmark.sh;
              protoFile = ./examples/echo/echo.proto;
            };
          };
        }
      );
}
