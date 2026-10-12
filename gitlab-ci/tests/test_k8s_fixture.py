#!/usr/bin/env python3
# Copyright (C) 2026 CERN/Switzerland. GPL-3.0-or-later.
"""Fixture regression tests use a fake kubectl; they never contact a cluster."""

import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import yaml

ROOT = Path(
    os.environ.get(
        "EOS_K8S_FIXTURE_DIR", Path(__file__).resolve().parents[2] / "eos-on-k8s"
    )
).resolve()
sys.dont_write_bytecode = True
MOCK = r"""#!/usr/bin/env python3
import json, os, sys
from pathlib import Path
args = sys.argv[1:]
with open(os.environ["MOCK_RECORD"], "a") as record:
    record.write(json.dumps(args) + "\n")
scenario = os.environ.get("MOCK_SCENARIO", "")
if "delete" in args and "namespace" in args:
    if scenario == "delete-failure":
        sys.stderr.write("apiserver unavailable\n"); sys.exit(27)
    sys.exit(0)
if "patch" in args:
    if scenario == "pv-failure": sys.exit(29)
    sys.exit(0)
if "current-context" in args: print("mock-context"); sys.exit(0)
if "namespace" in args and "get" in args:
    if scenario == "namespace-api-failure":
        sys.stderr.write("Forbidden\n"); sys.exit(23)
    if scenario == "gone": sys.exit(0)
    print('{}' if "json" in args else "namespace/mock"); sys.exit(0)
if "pvc" in args and "get" in args: print("pv-one\npv-two"); sys.exit(0)
if "pods" in args and "get" in args:
    if "json" in args:
        if scenario == "discovery-failure":
            sys.stderr.write("Forbidden\n"); sys.exit(24)
        items=[]
        for name in ("eos-mgm1-deployment-abc-123", "eos-fst10-deployment-abc-123", "eos-qdb-deployment-abc-123", "eos-cli1-deployment-abc-123", "support-abc-123"):
            items.append({"metadata":{"name":name}, "spec":{"containers":[{"name":"eos-storage"},{"name":"sidecar"}], "initContainers":[{"name":"seed"}]}, "status":{"containerStatuses":[{"name":"eos-storage","state":{"running":{}},"restartCount":1},{"name":"sidecar","state":{"running":{}}}], "initContainerStatuses":[{"name":"seed","state":{"terminated":{}}}]}})
        print(json.dumps({"items":items})); sys.exit(0)
    if any("conditions" in arg for arg in args): print("True " * 999); sys.exit(0)
    app = args[args.index("-l")+1].split("=",1)[1] if "-l" in args else "eos-kdc"
    print("NAME READY STATUS RESTARTS AGE\n"+app+"-deployment-abc-123 1/1 Running 0 1m"); sys.exit(0)
if "logs" in args:
    if scenario == "missing-previous" and "--previous" in args:
        sys.stderr.write('previous terminated container "eos-storage" in pod "mock" not found\n'); sys.exit(1)
    if scenario == "logs-api-failure":
        sys.stderr.write("apiserver unavailable\n"); sys.exit(25)
    print("container stdout " + " ".join(args)); sys.exit(0)
if "exec" in args:
    if scenario == "create": sys.exit(0)
    if scenario == "exec-api-failure":
        sys.stderr.write("apiserver unavailable\n"); sys.exit(28)
    if scenario == "missing-shell":
        sys.stderr.write('exec: "/bin/sh": executable file not found in $PATH\n'); sys.exit(127)
    command=args[-1]
    if scenario == "missing-optional": sys.exit(42)
    if "for log" in command:
        print("normal-log\nrestart-log"); sys.exit(0)
    if "qdb/xrdlog" in command: print("qdb-log"); sys.exit(0)
    if "tar " in command and "-C" in command: sys.stdout.buffer.write(b"mock archive"); sys.exit(0)
    if "test -e" in command: sys.exit(42)
    sys.exit(0)
if "get" in args and "events" in args:
    if scenario == "events-api-failure": sys.exit(26)
    print("fixture events"); sys.exit(0)
if "describe" in args: print("pod description"); sys.exit(0)
sys.exit(0)
"""
TIMEOUT = r"""#!/usr/bin/env python3
import os, sys
args=sys.argv[1:]
args=args[3:] if args[:2]==["-s","KILL"] else args[1:]
if os.environ.get("MOCK_SCENARIO")=="delete-timeout" and "delete" in args:
    sys.exit(137)
os.execvp(args[0],args)
"""
SED = r"""#!/usr/bin/env python3
import os, sys
args=sys.argv[1:]
if sys.platform=="darwin" and args[0]=="-i": args.insert(1, "")
os.execv(os.environ["FIXTURE_REAL_SED"],["sed",*args])
"""


class FixtureTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.directory = Path(self.temporary.name)
        self.bin = self.directory / "bin"
        self.bin.mkdir()
        for name, source in (
            ("kubectl", MOCK),
            ("timeout", TIMEOUT),
            ("sed", SED),
            ("sleep", "#!/bin/sh\nexit 0\n"),
        ):
            script = self.bin / name
            script.write_text(source)
            script.chmod(0o755)
        self.record = self.directory / "calls.jsonl"
        self.environment = {
            **os.environ,
            "PATH": str(self.bin) + os.pathsep + os.environ["PATH"],
            "MOCK_RECORD": str(self.record),
            "FIXTURE_REAL_SED": shutil.which("sed"),
            "KUBECONFIG": "/no-real-cluster",
        }

    def tearDown(self):
        self.temporary.cleanup()

    def run_script(self, script, *args, scenario=""):
        result = subprocess.run(
            ["bash", str(ROOT / script), *args],
            cwd=self.directory,
            env={**self.environment, "MOCK_SCENARIO": scenario},
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
        return result

    def calls(self):
        return (
            [json.loads(line) for line in self.record.read_text().splitlines()]
            if self.record.exists()
            else []
        )

    def test_unsafe_names_never_call_kubectl(self):
        for namespace in ("", "default", "Bad", "-bad", "x" * 64):
            with self.subTest(namespace=namespace):
                self.assertNotEqual(
                    self.run_script("delete-all.sh", namespace).returncode, 0
                )
        self.assertEqual(self.calls(), [])

    def test_deletion_failure_retains_manifest_and_context(self):
        for scenario in ("delete-failure", "delete-timeout"):
            with self.subTest(scenario=scenario):
                manifest = self.directory / "eos-instance-mock"
                manifest.mkdir(exist_ok=True)
                self.assertNotEqual(
                    self.run_script(
                        "delete-all.sh", "mock", scenario=scenario
                    ).returncode,
                    0,
                )
                self.assertTrue(manifest.is_dir())
                self.assertFalse(any("set-context" in call for call in self.calls()))
        call = next(call for call in self.calls() if "delete" in call)
        self.assertIn("--ignore-not-found", call)
        self.assertIn("--wait=true", call)
        self.assertIn("--timeout=120s", call)

    def test_deletion_is_idempotent_and_pv_feature_is_preserved(self):
        manifest = self.directory / "eos-instance-mock"
        manifest.mkdir()
        self.assertEqual(self.run_script("delete-all.sh", "mock", "-d").returncode, 0)
        self.assertFalse(manifest.exists())
        patches = [call for call in self.calls() if "patch" in call]
        self.assertEqual(len(patches), 2)
        self.assertEqual(
            self.run_script("delete-all.sh", "mock", "-d", scenario="gone").returncode,
            0,
        )

    def test_pv_api_failure_prevents_deletion(self):
        result = self.run_script("delete-all.sh", "mock", "-d", scenario="pv-failure")
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(any("delete" in call for call in self.calls()))

    def test_deleted_namespace_has_successful_empty_collection(self):
        self.assertEqual(
            self.run_script(
                "collect_logs.sh", "mock", "logs", scenario="gone"
            ).returncode,
            0,
        )
        self.assertEqual(len(self.calls()), 1)

    def test_collection_propagates_api_errors(self):
        for scenario in (
            "namespace-api-failure",
            "discovery-failure",
            "logs-api-failure",
            "exec-api-failure",
            "events-api-failure",
        ):
            with self.subTest(scenario=scenario):
                self.assertNotEqual(
                    self.run_script(
                        "collect_logs.sh", "mock", "logs", scenario=scenario
                    ).returncode,
                    0,
                )

    def test_missing_optional_logs_succeed(self):
        for scenario in ("missing-optional", "missing-previous", "missing-shell"):
            with self.subTest(scenario=scenario):
                self.assertEqual(
                    self.run_script(
                        "collect_logs.sh", "mock", "logs", scenario=scenario
                    ).returncode,
                    0,
                )
        self.assertTrue((self.directory / "logs" / "pods.json").is_file())

    def test_every_container_and_init_log_has_diagnostics(self):
        result = self.run_script("collect_logs.sh", "mock", "logs")
        self.assertEqual(result.returncode, 0, result.stderr)
        logs = self.directory / "logs"
        for pod in ("eos-mgm1", "eos-fst10", "eos-qdb", "eos-cli1", "support"):
            name = (
                pod + "-deployment-abc-123" if pod != "support" else "support-abc-123"
            )
            for container in ("eos-storage", "sidecar", "seed"):
                self.assertTrue((logs / f"{name}.{container}.stdout.log").is_file())
            self.assertTrue((logs / f"{name}.eos-storage.previous.log").is_file())
            self.assertTrue((logs / f"{name}.describe.log").is_file())
        self.assertEqual(
            (logs / "eos-mgm1.log").read_text(), "normal-log\nrestart-log\n"
        )
        self.assertEqual(
            (
                logs / "eos-mgm1-deployment-abc-123.eos-storage.eos-logs.tar.gz"
            ).read_bytes(),
            b"mock archive",
        )
        self.assertTrue((logs / "eos-fst10.log").is_file())
        mgm = next(
            call
            for call in self.calls()
            if "for log" in call[-1] and "mgm/xrdlog" in call[-1]
        )
        self.assertIn("/var/log/eos/xrdlog.mgm", mgm[-1])
        self.assertTrue(all("--request-timeout=20s" in call for call in self.calls()))

    def test_one_two_and_three_mgm_configuration_and_account_order(self):
        original = (ROOT / "templates/eos-fst.template.yaml").read_bytes()
        original_config = (ROOT / "templates/configmap.template.yaml").read_text()
        for count in (1, 2, 3):
            with self.subTest(count=count):
                self.record.unlink(missing_ok=True)
                namespace = f"fixture{count}"
                result = self.run_script(
                    "create-all.sh",
                    "-n",
                    namespace,
                    "-m",
                    str(count),
                    "-f",
                    "2",
                    scenario="create",
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                directory = self.directory / f"eos-instance-{namespace}"
                namespace_config = yaml.safe_load(
                    (directory / f"namespace-{namespace}.yaml").read_text()
                )
                self.assertNotIn(
                    "eos.cern.ch/ha-test", namespace_config["metadata"].get("labels", {})
                )
                expected = original.decode()
                for placeholder, value in (
                    ("NAMESPACE", namespace),
                    ("NUMBER", "1"),
                    ("IMAGE_REPO", "gitlab-registry.cern.ch/dss/eos/eos-ci"),
                    ("IMAGE_TAG", "4.8.57"),
                ):
                    expected = expected.replace(f"%%%{placeholder}%%%", value)
                self.assertEqual((directory / "eos-fst1.yaml").read_text(), expected)
                self.assertFalse((directory / ".fst-ha.template.yaml").exists())
                calls = self.calls()
                accounts = [
                    index
                    for index, call in enumerate(calls)
                    if "getent passwd admin1" in call[-1]
                ]
                daemons = [
                    index
                    for index, call in enumerate(calls)
                    if call[-1] == "./eos_mgm_setup.sh"
                ]
                self.assertEqual(len(daemons), count)
                if count == 1:
                    self.assertEqual(accounts, [])
                else:
                    self.assertEqual(len(accounts), count)
                    self.assertLess(max(accounts), min(daemons))
                    for number in range(1, count + 1):
                        expected_config = original_config.replace(
                            "%%%NAMESPACE%%%", namespace
                        )
                        expected_config = expected_config.replace(
                            "configure-container", f"configure-container-mgm{number}"
                        )
                        config_path = directory / f"configmap-mgm{number}.yaml"
                        self.assertEqual(config_path.read_text(), expected_config)
                        script = yaml.safe_load(config_path.read_text())["data"][
                            "configure.sh"
                        ]
                        self.assertNotIn("EOS_MGM_MASTER1=", script)
                        self.assertNotIn("EOS_MGM_MASTER2=", script)
                        self.assertEqual(
                            subprocess.run(
                                ["bash", "-n"],
                                input=script,
                                text=True,
                                capture_output=True,
                                check=False,
                            ).returncode,
                            0,
                        )
                self.assertEqual(
                    (ROOT / "templates/eos-fst.template.yaml").read_bytes(), original
                )


if __name__ == "__main__":
    unittest.main()
