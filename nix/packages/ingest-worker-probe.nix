{lib, stdenv, pkg-config, ffmpeg, python3}: stdenv.mkDerivation {
  pname = "amcrust-ingest-worker-probe";
  version = "0.1.0";
  src = lib.sourceByRegex ../../. ["probes.*" "scripts.*"];
  nativeBuildInputs = [pkg-config];
  buildInputs = [ffmpeg];
  buildPhase = ''
    runHook preBuild
    $CC -std=c11 -Wall -Wextra -Werror -O2 probes/ingest_worker.c \
      $(pkg-config --cflags --libs libavformat libavcodec libavutil) -o ingest-worker-probe
    runHook postBuild
  '';
  doCheck = true;
  nativeCheckInputs = [python3 ffmpeg];
  checkPhase = ''
    runHook preCheck
    export PYTHONDONTWRITEBYTECODE=1
    python3 -m unittest discover -s scripts -p 'test_*.py' -v
    python3 scripts/packet_ingest_probe.py --worker ./ingest-worker-probe
    python3 scripts/rtsp_ingest_probe.py --worker ./ingest-worker-probe
    runHook postCheck
  '';
  installPhase = ''
    mkdir -p $out/bin
    cp ingest-worker-probe $out/bin/
  '';
  meta.mainProgram = "ingest-worker-probe";
}
