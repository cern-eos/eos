#!/usr/bin/env python3
# Copyright (C) 2026 CERN/Switzerland. GPL-3.0-or-later.
"""Exercise CI helper contracts without contacting Docker or Kubernetes."""

import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import tempfile
import time
import unittest


SOURCE = Path(__file__).resolve().parents[1]
NAMESPACE = "k8s-mgm-ha-71-81"


class HelpersTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="eos-ci-helper-test-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.ci = self.root / "gitlab-ci"
        self.ci.mkdir()
        for name in ("utilities_func_for_tests.sh", "run_mgm_ha_ci.sh",
                     "after_script_k8s_test.sh", "before_script_k8s_test.sh"):
            shutil.copyfile(SOURCE / name, self.ci / name)
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.env = dict(os.environ, PATH=f"{self.bin}{os.pathsep}{os.environ['PATH']}",
                        UTILITY=str(self.ci / "utilities_func_for_tests.sh"),
                        EXECUTOR="k8s", NAMESPACE=NAMESPACE, K8S_NAMESPACE=NAMESPACE,
                        CI_JOB_ID="71", CI_PIPELINE_ID="81", CI_JOB_NAME="k8s_mgm_ha",
                        KUBECONFIG=str(self.root / "kubeconfig"),
                        K8S_CONFIG=str(self.root / "kubeconfig"), EOS_K8S_HA_TEST="1",
                        MOCK_CALLS=str(self.root / "calls"))
        self.tool("kubectl", """#!/usr/bin/env python3
import json, os, sys
args = sys.argv[1:]
with open(os.environ['MOCK_CALLS'], 'a') as output:
    output.write(json.dumps(['kubectl'] + args) + '\\n')
if args[:2] == ['get', 'pods']:
    if os.environ.get('MOCK_PODS_EXIT'):
        sys.exit(int(os.environ['MOCK_PODS_EXIT']))
    print('unready-pod  Running False')
    print('ready-pod  Running True')
    print('terminating-pod 2026-10-12T10:00:00Z Running True')
    print('pending-pod  Pending True')
    print('completed-pod  Succeeded True')
if 'exec' in args or 'cp' in args:
    sys.exit(int(os.environ.get('MOCK_EXEC_STATUS', '0')))
if 'label' in args:
    sys.exit(int(os.environ.get('MOCK_LABEL_STATUS', '0')))
""")
        self.tool("docker", """#!/usr/bin/env python3
import json, os, sys
with open(os.environ['MOCK_CALLS'], 'a') as output:
    output.write(json.dumps(['docker'] + sys.argv[1:]) + '\\n')
sys.exit(int(os.environ.get('MOCK_EXEC_STATUS', '0')))
""")
        self.tool("timeout", """#!/usr/bin/env python3
import json, os, sys
args = sys.argv[1:]
with open(os.environ['MOCK_CALLS'], 'a') as output:
    output.write(json.dumps(['timeout'] + args) + '\\n')
assert args[:2] == ['-s', 'KILL']
if os.environ.get('MOCK_TIMEOUT') in args[4:] and os.environ.get('MOCK_TIMEOUT'):
    sys.exit(137)
os.execvp(args[3], args[3:])
""")

    def tool(self, name, body):
        path = self.bin / name
        path.write_text(body)
        path.chmod(0o700)

    def calls(self):
        path = self.root / "calls"
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []

    def shell(self, command, **overrides):
        return subprocess.run(
            ["bash", "-c", 'set -uo pipefail; source "$UTILITY" --type "$EXECUTOR" "$NAMESPACE"; ' + command],
            cwd=self.root, env=dict(self.env, **overrides), capture_output=True,
            text=True, timeout=8)

    def script(self, name, **overrides):
        return subprocess.run(["bash", str(self.ci / name)], cwd=self.root,
                              env=dict(self.env, **overrides), capture_output=True,
                              text=True, timeout=8)

    def fixture(self):
        fixture = self.root / "eos-on-k8s"
        fixture.mkdir()
        (self.root / "namespace-present").touch()
        (fixture / "collect_logs.sh").write_text("""#!/bin/bash
[[ -e namespace-present ]] || exit 0
printf '["collect"]\\n' >> "$MOCK_CALLS"
mkdir -p "$2"
printf 'daemon log\\n' > "$2/mock.log"
exit "${COLLECT_EXIT:-0}"
""")
        (fixture / "delete-all.sh").write_text("""#!/bin/bash
[[ -e namespace-present ]] || exit 0
printf '["delete"]\\n' >> "$MOCK_CALLS"
if [[ ${DELETE_EXIT:-0} != 0 ]]; then exit "$DELETE_EXIT"; fi
rm namespace-present
printf '["delete-after"]\\n' >> "$MOCK_CALLS"
""")
        (self.ci / "test_mgm_ha.sh").write_text("""#!/bin/bash
printf '["test"]\\n' >> "$MOCK_CALLS"
printf 'mock HA trace\\n'
if [[ ${MOCK_SLEEP:-0} != 0 ]]; then sleep "$MOCK_SLEEP"; fi
exit "${TEST_EXIT:-0}"
""")
        self.tool("tee", """#!/bin/bash
/usr/bin/tee "$@"
exit "${TEE_EXIT:-0}"
""")

    def test_argv_execution_preserves_arguments_and_status(self):
        arguments = ["a b", "quote'\"", "$literal", "", "--flag"]
        for executor in ("k8s", "docker"):
            with self.subTest(executor=executor):
                (self.root / "calls").unlink(missing_ok=True)
                result = subprocess.run(
                    ["bash", "-c", 'set -uo pipefail; source "$UTILITY" --type "$EXECUTOR" "$NAMESPACE"; exec_cmd_argv unready-pod printf "$@"',
                     "test", *arguments], cwd=self.root,
                    env=dict(self.env, EXECUTOR=executor, MOCK_EXEC_STATUS="17"),
                    capture_output=True, text=True, timeout=8)
                self.assertEqual(result.returncode, 17, result.stderr)
                expected = (["kubectl", "--namespace", NAMESPACE, "exec", "unready-pod", "--", "printf"]
                            if executor == "k8s" else ["docker", "exec", "-i", "unready-pod", "printf"])
                self.assertEqual(self.calls()[-1], expected + arguments)
                self.assertFalse(any("get" in call for call in self.calls()))
                if executor == "k8s":
                    self.assertEqual(self.calls()[0][:4], ["timeout", "-s", "KILL", "30"])

    def test_legacy_executor_copy_and_ready_selector(self):
        result = self.shell('exec_cmd eos-mgm1 "printf \'a b\'"; cp_to_local_cmd eos-mgm1:/tmp/log ./log; get_podname eos-mgm1')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "ready-pod\n")
        calls = self.calls()
        self.assertIn(["kubectl", "exec", f"--namespace={NAMESPACE}", "ready-pod", "--", "/bin/bash", "-lc", "printf 'a b'"], calls)
        self.assertIn(["kubectl", "cp", f"{NAMESPACE}/ready-pod:/tmp/log", "./log"], calls)
        result = self.shell('exec_cmd c "printf \'a b\'"; cp_to_local_cmd c:/tmp/log ./log', EXECUTOR="docker")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(["docker", "exec", "-i", "c", "/bin/bash", "-lic", "printf 'a b'"], self.calls())
        self.assertIn(["docker", "cp", "c:/tmp/log", "./log"], self.calls())

    def test_ready_selector_propagates_api_failure(self):
        result = self.shell('get_podname eos-mgm1', MOCK_PODS_EXIT="19")
        self.assertEqual(result.returncode, 19)
        self.assertEqual(result.stdout, "")

    def test_fixture_regressions_and_ha_label_belong_to_eos_bootstrap(self):
        fixture = self.root / "eos-on-k8s"
        fixture.mkdir()
        (fixture / "create-all.sh").write_text("""#!/bin/bash
printf '["create"]\\n' >> "$MOCK_CALLS"
exit "${CREATE_EXIT:-0}"
""")
        (fixture / "create-all.sh").chmod(0o700)
        tests = self.ci / "tests"
        tests.mkdir()
        (tests / "test_k8s_fixture.py").write_text("""import os, sys
from pathlib import Path
assert Path(os.environ['EOS_K8S_FIXTURE_DIR']) == Path.cwd() / 'eos-on-k8s'
with open(os.environ['MOCK_CALLS'], 'a') as output:
    output.write('["fixture-tests"]\\n')
sys.exit(int(os.environ.get('FIXTURE_TEST_EXIT', '0')))
""")
        self.tool("git", "#!/bin/sh\nexit 0\n")
        cases = (({}, 0, True),
                 ({"EOS_K8S_HA_TEST": "0"}, 0, False),
                 ({"FIXTURE_TEST_EXIT": "7"}, 7, False),
                 ({"CREATE_EXIT": "8"}, 8, False),
                 ({"MOCK_LABEL_STATUS": "9"}, 9, True))
        for flags, expected, marked in cases:
            with self.subTest(flags=flags):
                (self.root / "calls").unlink(missing_ok=True)
                result = self.script("before_script_k8s_test.sh",
                                     CI_REGISTRY_IMAGE="registry/eos", CI_COMMIT_SHORT_SHA="abc",
                                     OS_TAG=".el9", CLI_BASETAG="", **flags)
                self.assertEqual(result.returncode, expected, result.stderr)
                calls = self.calls()
                labels = [call for call in calls if call[0] == "kubectl" and "label" in call]
                self.assertEqual(bool(labels), marked)
                if labels:
                    self.assertIn(["create"], calls)
                    self.assertLess(calls.index(["fixture-tests"]), calls.index(["create"]))
                    self.assertLess(calls.index(["create"]), calls.index(labels[0]))
                    self.assertEqual(labels[0], ["kubectl", "--request-timeout=20s", "label",
                                                "namespace", NAMESPACE,
                                                "eos.cern.ch/ha-test=true", "--overwrite"])
                if flags.get("FIXTURE_TEST_EXIT"):
                    self.assertNotIn(["create"], calls)
                if flags.get("EOS_K8S_HA_TEST") == "0":
                    self.assertNotIn(["fixture-tests"], calls)

    def test_monitor_parser_accepts_exactly_one_nonempty_field(self):
        for output, key, expected in (("role=leader next=x", "role", "leader\n"),
                                      ("myrole=x role=a=b", "role", "a=b\n"),
                                      ("role=leader\nrole=follower", "role", None),
                                      ("role=", "role", None), ("roles=x", "role", None)):
            with self.subTest(output=output):
                result = self.shell('monitor_value "$MONITOR_OUTPUT" "$MONITOR_KEY"',
                                    MONITOR_OUTPUT=output, MONITOR_KEY=key)
                self.assertEqual(result.returncode == 0, expected is not None)
                if expected is not None:
                    self.assertEqual(result.stdout, expected)

    def test_polling_success_callbacks_and_expired_deadline(self):
        result = self.shell('count=0; progress=0; predicate() { count=$((count+1)); (( count == 2 )); }; pending() { progress=$((progress+1)); }; poll_until "$((SECONDS+4))" 1 pending predicate; printf "%s %s\\n" "$count" "$progress"')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "2 1\n")
        result = self.shell('predicate() { echo should-not-run; }; poll_until "$SECONDS" 1 "" predicate')
        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stdout, "")
        result = self.shell('poll_until "$((SECONDS+1))" 4 "" false')
        self.assertEqual(result.returncode, 1)
        result = self.shell('poll_until "$((SECONDS+1))" 0 "" true')
        self.assertEqual(result.returncode, 2)
        result = self.shell('poll_until 1')
        self.assertEqual(result.returncode, 2)

    def test_cleanup_rejects_unsafe_namespaces(self):
        for namespace in ("", "default", "bad/name", "a" * 64):
            with self.subTest(namespace=namespace):
                result = self.shell('cleanup_k8s_fixture "$CLEANUP_NAMESPACE" logs/',
                                    CLEANUP_NAMESPACE=namespace)
                self.assertEqual(result.returncode, 2)
                self.assertEqual(self.calls(), [])

    def test_finalizer_status_and_fallback_contracts(self):
        cases = (({}, 0), ({"TEST_EXIT": "7"}, 7), ({"DELETE_EXIT": "13"}, 1),
                 ({"COLLECT_EXIT": "9"}, 1),
                 ({"TEST_EXIT": "7", "DELETE_EXIT": "13"}, 7),
                 ({"MOCK_TIMEOUT": "./eos-on-k8s/collect_logs.sh"}, 1),
                 ({"MOCK_TIMEOUT": "./eos-on-k8s/delete-all.sh"}, 1),
                 ({"TEE_EXIT": "23"}, 23), ({"TEST_EXIT": "7", "TEE_EXIT": "23"}, 7))
        self.fixture()
        for flags, expected in cases:
            with self.subTest(flags=flags):
                (self.root / "namespace-present").touch()
                (self.root / "calls").unlink(missing_ok=True)
                result = self.script("run_mgm_ha_ci.sh", **flags)
                self.assertEqual(result.returncode, expected, result.stderr)
                calls = self.calls()
                self.assertIn(["test"], calls)
                self.assertTrue(any("./eos-on-k8s/delete-all.sh" in call for call in calls))
                self.assertIn("mock HA trace", (self.root / "eos-ha-71.log").read_text())
                # Calling the finalizer again is harmless once deletion succeeded.
                deleted = not (self.root / "namespace-present").exists()
                result = self.shell('cleanup_k8s_fixture "$NAMESPACE" eos-logs-71/')
                self.assertEqual(result.returncode, 0, result.stderr)
                if deleted:
                    self.assertEqual(self.calls().count(["delete"]), calls.count(["delete"]))
        result = self.script("after_script_k8s_test.sh")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse((self.root / "eos-on-k8s").exists())

    def test_fallback_removes_owned_clone_after_persistent_cleanup_failure(self):
        self.fixture()
        result = self.script("after_script_k8s_test.sh", DELETE_EXIT="13")
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse((self.root / "eos-on-k8s").exists())

    def test_signals_preserve_status_and_finalize(self):
        self.fixture()
        for received, expected in ((signal.SIGTERM, 143), (signal.SIGINT, 130)):
            with self.subTest(signal=received):
                (self.root / "namespace-present").touch()
                (self.root / "calls").unlink(missing_ok=True)
                process = subprocess.Popen(["bash", str(self.ci / "run_mgm_ha_ci.sh")],
                                           cwd=self.root, env=dict(self.env, MOCK_SLEEP="0.5"),
                                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                deadline = time.monotonic() + 3
                while ["test"] not in self.calls() and time.monotonic() < deadline:
                    time.sleep(0.01)
                self.assertIn(["test"], self.calls())
                process.send_signal(received)
                stdout, stderr = process.communicate(timeout=5)
                self.assertEqual(process.returncode, expected, (stdout, stderr))
                self.assertIn(["delete-after"], self.calls())


if __name__ == "__main__":
    unittest.main()
