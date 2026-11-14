import unittest

from admission_churn_probe import run


class AdmissionChurnTests(unittest.IsolatedAsyncioTestCase):
    async def test_changed_configs_all_terminal_paths_and_zero_retired_owners(self):
        result = await run(generations=8, payload_bytes=1024)
        self.assertEqual(result["delivered_packets"], [240, 240, 120])
        self.assertEqual(result["terminal_counts"], {"generation_changed": 2, "source_failed": 2,
                                                   "cancelled": 2, "source_complete": 2})
        self.assertEqual(result["peaks"]["inflight_frames"], 2)
        self.assertGreater(result["peaks"]["inflight_bytes"], 2048)
        self.assertEqual(result["final_frames"], 0)
        self.assertEqual(result["final_ranges"], 0)
