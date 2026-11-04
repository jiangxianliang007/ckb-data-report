"""Unit tests for optional diagnostics cleanup without live RPC calls."""
import contextlib
import importlib.util
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("diagnostics_report", Path(__file__).with_name("report.py"))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class LocalClearTests(unittest.TestCase):
    def run_cli(self, *, chain="ckb_testnet", clear=True, extra=()):
        calls = []
        output = io.StringIO()
        def fake_single(url, method, params, timeout):
            calls.append(method)
            if method == "get_blockchain_info":
                return {"chain": chain}
            if method == "tx_pool_info":
                return {"pending": "0x3", "proposed": "0x0"}
            if method == "clear_tx_pool":
                return None
            raise AssertionError(method)

        with tempfile.TemporaryDirectory() as tmp:
            argv = ["report.py", "--output-dir", tmp]
            if clear:
                argv += ["--clear-tx-pool", "--execute", "--yes"]
            argv += list(extra)
            with patch.object(sys, "argv", argv), patch.object(module, "report", return_value=("# Report", {"pending": {}, "proposed": {}})), patch.object(module, "single", side_effect=fake_single), contextlib.redirect_stdout(output):
                if chain == "ckb_testnet":
                    module.main()
                else:
                    with self.assertRaisesRegex(RuntimeError, "仅允许清理 Testnet"):
                        module.main()
            saved = list(Path(tmp).glob("txpool-snapshot-*.json"))
            self.assertEqual(len(saved), 1)
            self.assertEqual(json.loads(saved[0].read_text())["result"]["pending"], {})
        return calls, output.getvalue()

    def test_default_is_read_only(self):
        calls, _ = self.run_cli(clear=False)
        self.assertEqual(calls, [])

    def test_testnet_clear(self):
        calls, stdout = self.run_cli()
        self.assertEqual(calls.count("clear_tx_pool"), 1)
        self.assertEqual(calls, ["get_blockchain_info", "tx_pool_info", "clear_tx_pool", "tx_pool_info"])
        self.assertIn("Pending 3 -> 3", stdout)

    def test_mainnet_rejected(self):
        calls, _ = self.run_cli(chain="ckb")
        self.assertNotIn("clear_tx_pool", calls)


if __name__ == "__main__":
    unittest.main()
