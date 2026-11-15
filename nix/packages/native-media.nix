{lib, stdenv, pkg-config, ffmpeg, json_c}: stdenv.mkDerivation {
  pname = "amcrust-native-media";
  version = "0.1.0";
  src = lib.sourceByRegex ../../. ["native.*" "probes.*"];
  nativeBuildInputs = [pkg-config];
  buildInputs = [ffmpeg json_c];
  buildPhase = ''
    $CC -std=c11 -O2 -Wall -Iprobes native/ingest.c -o amcrust-ingest $(pkg-config --cflags --libs libavformat libavcodec libavutil)
    if test -f native/output.c; then
      $CC -std=c11 -O2 -Wall -Iprobes native/output.c -o amcrust-output $(pkg-config --cflags --libs libavformat libavcodec libavutil json-c)
    fi
  '';
  installPhase = ''
    mkdir -p $out/bin
    cp amcrust-* $out/bin/
  '';
}
