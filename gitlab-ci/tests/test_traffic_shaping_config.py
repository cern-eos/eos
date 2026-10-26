#!/usr/bin/env python3
# Copyright (C) 2026 CERN/Switzerland. GPL-3.0-or-later.
"""Check shaping configuration assertions using fake EOS and the real jq parser."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


SOURCE = Path(__file__).resolve().parents[2] / "test" / "eos-traffic-shaping-test"
BASELINE = {
    "enabled": True,
    "detail_level": "aggregate",
    "estimators_update_period_ms": 100,
    "fst_io_policy_update_period_ms": 100,
    "fst_io_stats_reporting_period_ms": 100,
    "system_stats_time_window_seconds": 15,
}
QUERY = ["io", "shaping", "config", "ls", "--json"]


class ShapingConfigTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if shutil.which("jq") is None:
            raise RuntimeError("jq is required for shaping configuration fixtures")

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="eos-shaping-config-test-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.calls = self.root / "calls"
        self.payload = self.root / "config"
        self.env = dict(os.environ,
                        PATH=f"{self.bin}{os.pathsep}{os.environ['PATH']}",
                        TRAFFIC_SHAPING_TEST=str(SOURCE),
                        MOCK_CALLS=str(self.calls), MOCK_CONFIG=str(self.payload))
        eos = self.bin / "eos"
        eos.write_text(f"#!{sys.executable}\n" + r'''
import json, os, sys
from pathlib import Path
with open(os.environ['MOCK_CALLS'], 'a') as output:
    output.write(json.dumps(sys.argv[1:]) + '\n')
sys.stdout.write(Path(os.environ['MOCK_CONFIG']).read_text())
sys.exit(int(os.environ.get('MOCK_EXIT', '0')))
''')
        eos.chmod(0o700)

    def check(self, payload, detail="aggregate", baseline=None, status=0):
        self.calls.unlink(missing_ok=True)
        self.payload.write_text(payload)
        arguments = [detail] if baseline is None else [detail, str(baseline).lower()]
        result = subprocess.run(
            ["bash", "-c", '''set -euo pipefail
source "$TRAFFIC_SHAPING_TEST"
if check_shaping_config "$@"; then status=0; else status=$?; fi
printf '%s' "$CONFIG_JSON_RESULT"
exit "$status"
''', "fixture", *arguments], cwd=self.root,
            env=dict(self.env, MOCK_EXIT=str(status)), capture_output=True,
            text=True, timeout=8)
        self.assertEqual([json.loads(line) for line in self.calls.read_text().splitlines()],
                         [QUERY], result.stderr)
        # Bash strips trailing newlines in command substitution, as the caller does.
        self.assertEqual(result.stdout, payload.rstrip("\n"))
        return result

    def test_source_has_no_argument_setup_or_workload_side_effects(self):
        result = subprocess.run(
            ["bash", "-c", 'set -euo pipefail; source "$TRAFFIC_SHAPING_TEST"; declare -F check_shaping_config'],
            cwd=self.root, env=self.env, capture_output=True, text=True, timeout=8)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "check_shaping_config\n")
        self.assertFalse(self.calls.exists())

    def test_aggregate_and_fs_accept_compact_or_pretty_json(self):
        for detail in ("aggregate", "fs"):
            for indent in (None, 2):
                with self.subTest(detail=detail, indent=indent):
                    config = dict(BASELINE, detail_level=detail)
                    result = self.check(json.dumps(config, indent=indent), detail,
                                        baseline=detail == "aggregate")
                    self.assertEqual(result.returncode, 0, result.stderr)

    def test_default_check_does_not_require_baseline_periods(self):
        for detail in ("aggregate", "fs"):
            with self.subTest(detail=detail):
                result = self.check(json.dumps({"enabled": True, "detail_level": detail}), detail)
                self.assertEqual(result.returncode, 0, result.stderr)

    def test_disabled_missing_or_wrong_type_enabled_is_rejected(self):
        for enabled in (False, None, 0, 1, "true", [], {}):
            with self.subTest(enabled=enabled):
                result = self.check(json.dumps(dict(BASELINE, enabled=enabled)))
                self.assertNotEqual(result.returncode, 0)
        config = dict(BASELINE)
        del config["enabled"]
        self.assertNotEqual(self.check(json.dumps(config)).returncode, 0)

    def test_missing_wrong_mode_or_wrong_type_detail_is_rejected(self):
        for detail in ("fs", "other", "", None, True, 1, ["aggregate"], {}):
            with self.subTest(detail=detail):
                result = self.check(json.dumps(dict(BASELINE, detail_level=detail)))
                self.assertNotEqual(result.returncode, 0)
        config = dict(BASELINE)
        del config["detail_level"]
        self.assertNotEqual(self.check(json.dumps(config)).returncode, 0)
        self.assertNotEqual(self.check(json.dumps(BASELINE), detail="fs").returncode, 0)

    def test_exactly_one_json_object_is_required(self):
        valid = json.dumps(BASELINE)
        for payload in ("", " \n\t", "null", "true", "1", '"config"', "[]", "{}",
                        json.dumps([BASELINE]), valid + "\n" + valid,
                        "false\n" + valid, valid + "\nfalse"):
            with self.subTest(payload=payload):
                self.assertNotEqual(self.check(payload).returncode, 0)

    def test_malformed_json_is_rejected(self):
        valid = json.dumps(BASELINE)
        for payload in ("{", "garbage", valid + "\n{", "garbage\n" + valid):
            with self.subTest(payload=payload):
                self.assertNotEqual(self.check(payload).returncode, 0)

    def test_successful_json_cannot_hide_failed_eos_exit(self):
        result = self.check(json.dumps(BASELINE), baseline=True, status=17)
        self.assertEqual(result.returncode, 17)

    def test_baseline_periods_require_exact_numeric_values(self):
        for field, expected in BASELINE.items():
            if field in ("enabled", "detail_level"):
                continue
            for value in (expected + 1, str(expected), None, True, [], {}):
                with self.subTest(field=field, value=value):
                    result = self.check(json.dumps(dict(BASELINE, **{field: value})), baseline=True)
                    self.assertNotEqual(result.returncode, 0)
            config = dict(BASELINE)
            del config[field]
            with self.subTest(field=field, missing=True):
                self.assertNotEqual(self.check(json.dumps(config), baseline=True).returncode, 0)


if __name__ == "__main__":
    unittest.main()
