{pkgs}: pkgs.mkShell {
  nativeBuildInputs = with pkgs; [cargo rustc rustfmt clippy pkg-config rustPlatform.bindgenHook];
  buildInputs = with pkgs; [ffmpeg libiconv];
}
