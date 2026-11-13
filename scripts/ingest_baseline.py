#!/usr/bin/env python3
"""Sample an existing amcrust process tree without reading arguments or credentials.

Outputs JSONL. Does not start media, access a camera, or change configuration.
ps CPU percentages are OS-defined (often lifetime averages), not interval CPU time.
RSS sums can double-count shared pages. FFmpeg workers are NOT RTSP connections.
"""

import argparse
import json
import subprocess
import time
import urllib.parse
import urllib.request


MAX_METRICS_BYTES = 1024 * 1024


def descendants(rows, root):
    found = {root}
    while True:
        children = {r["pid"] for r in rows if r["ppid"] in found}
        expanded = found | children
        if expanded == found:
            return [r for r in rows if r["pid"] in found]
        found = expanded


def process_sample(pid):
    output = subprocess.check_output(
        ["ps", "-axo", "pid=,ppid=,rss=,pcpu=,comm="], text=True, timeout=5
    )
    rows = []
    for line in output.splitlines():
        fields = line.split(maxsplit=4)
        if len(fields) != 5:
            continue
        process, parent, rss, cpu, executable = fields
        rows.append({"pid": int(process), "ppid": int(parent),
                     "rss_kib": int(rss), "ps_cpu_percent": float(cpu),
                     "executable": executable.rsplit("/", 1)[-1]})
    if not any(row["pid"] == pid for row in rows):
        raise RuntimeError("target process no longer exists")
    selected = descendants(rows, pid)
    return {
        "processes": selected,
        "process_count": len(selected),
        "ffmpeg_processes": sum(p["executable"] == "ffmpeg" for p in selected),
        "rss_sum_kib": sum(p["rss_kib"] for p in selected),
        "ps_cpu_percent_sum": sum(p["ps_cpu_percent"] for p in selected),
    }


def metrics_sample(url):
    # No ambient HTTP proxies or redirects to other hosts.
    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, req, fp, code, msg, headers, newurl):
            return None

    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    with opener.open(url, timeout=3) as response:
        data = response.read(MAX_METRICS_BYTES + 1)
    if len(data) > MAX_METRICS_BYTES:
        raise RuntimeError("metrics response exceeded 1 MiB")
    return [line for line in data.decode().splitlines() if line.startswith("amcrust_")]


def local_metrics_url(value):
    url = urllib.parse.urlsplit(value)
    if (url.scheme != "http" or url.hostname not in ("localhost", "127.0.0.1", "::1")
            or url.username or url.password or url.path != "/metrics"
            or url.query or url.fragment):
        raise argparse.ArgumentTypeError("use http://127.0.0.1:PORT/metrics (no credentials)")
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pid", type=int, required=True)
    parser.add_argument("--samples", type=int, default=30, choices=range(1, 301))
    parser.add_argument("--metrics-url", type=local_metrics_url)
    args = parser.parse_args()
    if args.pid <= 0:
        parser.error("--pid must be positive")
    for index in range(args.samples):
        started = time.monotonic()
        sample = {"unix_seconds": time.time(), **process_sample(args.pid)}
        if args.metrics_url:
            try:
                sample["metrics"] = metrics_sample(args.metrics_url)
            except Exception as error:
                sample["metrics_error"] = str(error)
        print(json.dumps(sample), flush=True)
        if index + 1 < args.samples:
            time.sleep(max(0, 1 - (time.monotonic() - started)))


if __name__ == "__main__":
    main()
