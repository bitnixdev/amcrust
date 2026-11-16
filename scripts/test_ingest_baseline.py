"""Contract checks for the retained read-only production workload sampler."""

import argparse
import unittest

from ingest_baseline import descendants, local_metrics_url


class BaselineTests(unittest.TestCase):
    def test_process_tree_excludes_unrelated_processes(self):
        rows = [{"pid": 3, "ppid": 2}, {"pid": 2, "ppid": 1},
                {"pid": 1, "ppid": 0}, {"pid": 4, "ppid": 0}]
        self.assertEqual({r["pid"] for r in descendants(rows, 1)}, {1, 2, 3})

    def test_metrics_url_requires_local_no_credentials(self):
        self.assertEqual(local_metrics_url("http://127.0.0.1:9000/metrics"),
                         "http://127.0.0.1:9000/metrics")
        for url in ("http://example.com/metrics", "http://user:secret@localhost/metrics",
                    "http://localhost/metrics?secret=yes", "file:///metrics"):
            with self.assertRaises(argparse.ArgumentTypeError):
                local_metrics_url(url)


if __name__ == "__main__":
    unittest.main()
