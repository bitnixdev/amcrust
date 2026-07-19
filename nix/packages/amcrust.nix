{
  lib,
  rustPlatform,
}: let
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
    cargoLock.lockFile = ../../Cargo.lock;
  }
