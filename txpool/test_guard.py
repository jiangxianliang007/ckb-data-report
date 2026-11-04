"""Regression tests for destructive testnet txpool cleanup decisions."""
import contextlib
import importlib.util
import io
import sys
import time
import unittest
from pathlib import Path
from unittest.mock import patch


spec = importlib.util.spec_from_file_location("txpool_guard", Path(__file__).with_name("guard.py"))
guard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(guard)


class FullPoolThresholdTests(unittest.TestCase):
    def run_guard(self, *, pending_count=1, age=0, execute=False, extra=()):
        now_ms = int(time.time() * 1000)
        entry = {
            "ancestors_count": "0x1",
            "timestamp": hex(now_ms - age * 1000),
        }
        pool = {"pending": {f"0x{i:064x}": entry for i in range(pending_count)},
                "proposed": {}}
        calls = []

        def rpc(url, method, params=None):
            calls.append(method)
            if method == "get_blockchain_info":
                return {"chain": "ckb_testnet"}
            if method == "get_raw_tx_pool":
                return pool
            if method == "clear_tx_pool":
                return None
            if method == "tx_pool_info":
                return {"pending": hex(pending_count)}
            raise AssertionError(f"Unexpected RPC: {method}")

        argv = ["guard.py", "--pending-threshold", "2", "--max-age-seconds", "300"]
        if execute:
            argv.append("--execute")
        argv.extend(extra)
        with patch.object(sys, "argv", argv), patch.object(guard, "rpc", side_effect=rpc), \
             patch.object(guard, "remote_cleanup") as remote, \
             contextlib.redirect_stderr(io.StringIO()):
            code = guard.main()
        return code, calls, remote

    def test_pending_exceeds_threshold_clears(self):
        code, calls, remote = self.run_guard(pending_count=3, execute=True)
        self.assertEqual(code, 0)
        self.assertIn("clear_tx_pool", calls)
        remote.assert_called_once()
        self.assertTrue(remote.call_args.kwargs["clear"])

    def test_age_exceeds_threshold_clears_even_below_pending_limit(self):
        code, calls, remote = self.run_guard(age=301, execute=True)
        self.assertEqual(code, 0)
        self.assertIn("clear_tx_pool", calls)
        remote.assert_called_once()

    def test_at_age_threshold_does_not_clear(self):
        code, calls, remote = self.run_guard(age=300, execute=True)
        self.assertEqual(code, 0)
        self.assertNotIn("clear_tx_pool", calls)
        remote.assert_not_called()

    def test_dry_run_never_mutates(self):
        code, calls, remote = self.run_guard(pending_count=3)
        self.assertEqual(code, 0)
        self.assertNotIn("clear_tx_pool", calls)
        remote.assert_not_called()

    def test_targeted_only_disables_auto_clear(self):
        with patch.object(guard, "trace_roots", return_value=[]):
            code, calls, remote = self.run_guard(
                pending_count=3, execute=True, extra=("--targeted-only",))
        self.assertEqual(code, 0)
        self.assertNotIn("clear_tx_pool", calls)
        remote.assert_not_called()


if __name__ == "__main__":
    unittest.main()
