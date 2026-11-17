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
    # Lifecycle checks exercise the remaining recording child, not only the libraries.
    nativeCheckInputs = [(lib.getBin ffmpeg)];
    meta.mainProgram = "amcrust";
  }
