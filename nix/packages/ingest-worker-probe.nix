{lib, stdenv, pkg-config, ffmpeg, python3, json_c}: stdenv.mkDerivation {
  pname = "amcrust-ingest-worker-probe";
  version = "0.1.0";
  src = lib.sourceByRegex ../../. ["probes.*" "scripts.*"];
  nativeBuildInputs = [pkg-config];
  buildInputs = [ffmpeg json_c];
  buildPhase = ''
    runHook preBuild
    $CC -std=c11 -Wall -Wextra -Werror -O2 probes/ingest_worker.c \
      $(pkg-config --cflags --libs libavformat libavcodec libavutil) -o ingest-worker-probe
    $CC -std=c11 -D_POSIX_C_SOURCE=200809L -Wall -Wextra -Werror -O2 probes/mux_worker.c \
      $(pkg-config --cflags --libs libavformat libavcodec libavutil json-c) -o mux-worker-probe
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
    python3 scripts/av_output_probe.py --worker ./ingest-worker-probe --sink ./mux-worker-probe
    python3 scripts/rtsp_clock_probe.py --worker ./ingest-worker-probe --sink ./mux-worker-probe
    python3 scripts/fanout_load_probe.py --packets 2000 --subscribers 4
    python3 scripts/admission_churn_probe.py --generations 32
    runHook postCheck
  '';
  installPhase = ''
    mkdir -p $out/bin
    cp ingest-worker-probe mux-worker-probe $out/bin/
  '';
  meta.mainProgram = "ingest-worker-probe";
}
