{
  description = "ARC (Autonomous Robotic Core) dev environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = {
    self,
    nixpkgs,
    rust-overlay,
    flake-utils,
  }:
    flake-utils.lib.eachDefaultSystem (system: let
      overlays = [rust-overlay.overlays.default];
      pkgs = import nixpkgs {inherit system overlays;};
      rustToolchain = pkgs.rust-bin.stable.latest.default.override {
        extensions = ["rust-src" "rust-analyzer" "clippy" "rustfmt"];
      };

      # Perfetto's trace_processor, for checking what arcd writes to
      # data/traces/ (DESIGN.md §8): it parses a .pftrace and answers SQL
      # over it, so a trace can be verified without opening a browser.
      # nixpkgs has no perfetto package, so this is the upstream prebuilt,
      # pinned by hash. Bump `version` and the hashes together; they come
      # from the manifest in https://get.perfetto.dev/trace_processor.
      traceProcessorVersion = "v57.2";
      traceProcessorBinaries = {
        x86_64-linux = {
          arch = "linux-amd64";
          sha256 = "55ba613fc6d4f71df81eee2dbfc293020063655c241b3e314bff75345b802684";
        };
        aarch64-linux = {
          arch = "linux-arm64";
          sha256 = "1dcc1d9aaff2eb92e8bc58f1957e4e445600294bd61dbc09345c1018c5ff0868";
        };
        x86_64-darwin = {
          arch = "mac-amd64";
          sha256 = "c0f61397901da47cbe1bb9a0843624f7c2038ac92176ce15e3736ce9aa0afef0";
        };
        aarch64-darwin = {
          arch = "mac-arm64";
          sha256 = "98a41b80e9f60da0373d64aff6455681f8c26b7c391ae5736324a5b11e3dacc2";
        };
      };
      traceProcessor = let
        binary = traceProcessorBinaries.${system};
      in
        pkgs.stdenv.mkDerivation {
          pname = "trace-processor-shell";
          version = traceProcessorVersion;
          src = pkgs.fetchurl {
            url = "https://commondatastorage.googleapis.com/perfetto-luci-artifacts/${traceProcessorVersion}/${binary.arch}/trace_processor_shell";
            inherit (binary) sha256;
          };
          dontUnpack = true;
          # Upstream ships a generic-linux dynamic binary; NixOS needs its
          # interpreter and libstdc++ rewritten in.
          nativeBuildInputs = pkgs.lib.optional pkgs.stdenv.isLinux pkgs.autoPatchelfHook;
          buildInputs = pkgs.lib.optional pkgs.stdenv.isLinux pkgs.stdenv.cc.cc.lib;
          installPhase = ''
            install -Dm755 $src $out/bin/trace_processor_shell
          '';
        };
    in {
      devShells.default = pkgs.mkShell {
        buildInputs =
          [
            rustToolchain
            pkgs.jujutsu
            pkgs.protobuf
            pkgs.pkg-config
            pkgs.openssl
            pkgs.sqlite
            pkgs.just
            pkgs.ripgrep
          ]
          ++ pkgs.lib.optional (traceProcessorBinaries ? ${system}) traceProcessor;

        RUST_BACKTRACE = "1";
      };
    });
}
