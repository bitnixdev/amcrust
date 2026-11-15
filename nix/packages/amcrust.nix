{
  lib,
  rustPlatform,
  pkg-config,
  ffmpeg,
}: let
  cargoToml = lib.importTOML ../../Cargo.toml;
in
  rustPlatform.buildRustPackage {
    pname = cargoToml.package.name;
    version = cargoToml.package.version;
    src = lib.sourceByRegex ../../. [
      "Cargo\.(toml|lock)$"
      "src.*"
      "tests.*"
      "hap.*"
    ];
    cargoLock.lockFile = ../../Cargo.lock;
    nativeBuildInputs = [pkg-config rustPlatform.bindgenHook];
    buildInputs = [ffmpeg];
    meta.mainProgram = "amcrust";
  }
