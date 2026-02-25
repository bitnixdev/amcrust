{
  stdenv,
  lib,
  makeRustPlatform,
  rust-bin,
}: let
  toolchain = rust-bin.stable.latest.default.override {
    targets = lib.optionals stdenv.isLinux [
      "x86_64-unknown-linux-musl"
    ];
  };
  rustPlatform = makeRustPlatform {
    cargo = toolchain;
    rustc = toolchain;
  };
  cargoToml = lib.importTOML ../../Cargo.toml;
in
  rustPlatform.buildRustPackage {
    pname = cargoToml.package.name;
    version = cargoToml.package.version;
    src = lib.sourceByRegex ../../. [
      "Cargo\.(toml|lock)$"
      "src.*"
      "hap.*"
    ];
    target = lib.optionals stdenv.isLinux "x86_64-unknown-linux-musl";
    cargoLock.lockFile = ../../Cargo.lock;
  }
