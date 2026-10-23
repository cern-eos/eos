#!/usr/bin/env python3
# Copyright (C) 2026 CERN/Switzerland. GPL-3.0-or-later.
"""Run the HA script against a fake cluster to check storage readiness and cleanup."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
import unittest


SOURCE = Path(__file__).resolve().parents[1]
NAMESPACE = "ha-bootstrap-mock"
MOCK_KUBECTL = r'''#!/usr/bin/env python3
import json, os, sys
from pathlib import Path
args = sys.argv[1:]
namespace = args[1]
args = args[2:]
state_path = Path(os.environ['MOCK_STATE'])
state = json.loads(state_path.read_text())
scenario = os.environ['MOCK_SCENARIO']

def event(*values):
    with open(os.environ['MOCK_EVENTS'], 'a') as output:
        output.write(json.dumps(values) + '\n')

def finish(output='', status=0):
    state_path.write_text(json.dumps(state))
    if output:
        print(output, file=sys.stdout if status == 0 else sys.stderr)
    sys.exit(status)

def host(index):
    name = f'eos-mgm{index + 1}'
    return f'{name}.{name}.{namespace}.svc.cluster.local'

def post_boot():
    return state['hook_index'] is not None and not state['hook_dir']

if args[:2] == ['get', 'namespace']:
    finish('true')
if args[:2] == ['get', 'pods']:
    finish('\n'.join(f'{role} {role}-pod' for role in
                    ('eos-mgm1', 'eos-mgm2', 'eos-fst1', 'eos-fst2', 'eos-cli1')))
if args[:2] == ['get', 'pod']:
    finish('10.0.0.1' if args[2] == 'eos-mgm1-pod' else '10.0.0.2')
if args[:2] == ['get', 'service']:
    finish(state['run_id'] if state['alias'] is not None else '')
if args[0] == 'create':
    manifest = sys.stdin.read()
    state['run_id'] = next(line.split('"')[1] for line in manifest.splitlines()
                           if 'eos-ha-test:' in line)
    state['alias'] = 0 if 'externalName: ' + host(0) in manifest else 1
    finish()
if args[0] == 'patch':
    patch = json.loads(args[args.index('--patch') + 1])
    state['alias'] = 0 if patch['spec']['externalName'] == host(0) else 1
    finish()
if args[0] == 'delete':
    event('cleanup-alias')
    state['alias'] = None
    finish()
if args[0] == 'get':
    finish('diagnostics')
if args[0] != 'exec':
    finish('unexpected kubectl command: ' + repr(args), 91)
pod = args[2] if args[1] == '-i' else args[1]
command = args[args.index('--') + 1:]
if 'python3' in command:
    sys.stdin.read()
    event('preflight', pod)
    finish('storage ready', 1 if scenario == 'preflight-failure' else 0)
if 'mgm-ha-hook-install' in command:
    event('hook-install')
    if scenario == 'existing-hook':
        finish('refusing to replace an existing post-promotion hook', 1)
    state['hook_installed'] = True
    state['hook_index'] = 0 if pod == 'eos-mgm1-pod' else 1
    state['hook_dir'] = command[-2]
    state['hook_records'] = []
    finish()
if 'mgm-ha-hook-restore' in command:
    event('hook-restore')
    state['hook_installed'] = False
    finish()
if 'mgm-ha-hook-remove' in command:
    event('hook-remove')
    state['hook_dir'] = ''
    finish()
if 'find_mgm()' in ' '.join(command):
    action = command[-1]
    index = 0 if pod == 'eos-mgm1-pod' else 1
    event(action, index)
    was_running = state['running'][index]
    if (scenario == 'hook-stop-failure' and action in ('stop', 'stop-if-running') and
            state['hook_loaded'][index]):
        finish('MGM could not be stopped', 1)
    state['running'][index] = action == 'start'
    if action in ('stop', 'stop-if-running'):
        state['hook_loaded'][index] = False
    elif not was_running:
        state['hook_loaded'][index] = state['hook_installed'] and state['hook_index'] == index
    if action in ('stop', 'stop-if-running') and state['leader'] == index:
        state['leader'] = 1 - index if state['running'][1 - index] else None
    if action == 'start' and (state['leader'] is None or not state['running'][state['leader']]):
        state['leader'] = index
        if state['hook_installed'] and state['hook_index'] == index:
            event('boot-hook', index)
            identity = host(1 - index if scenario == 'boot-hook-wrong-identity' else index) + ':1094'
            record = '\t' + identity
            if scenario != 'boot-hook-missing':
                state['hook_records'].append(record)
            if scenario == 'boot-hook-duplicate':
                state['hook_records'].append(record)
    if scenario == 'boot-hook-start-after-spawn' and action == 'start' and state['hook_loaded'][index]:
        finish('MGM start failed after spawning xrootd', 1)
    finish()
command = command[command.index('mgm-ha') + 1:]
if command[0] == 'cat' and command[1].endswith('/invocations'):
    event('hook-read')
    finish('\n'.join(state['hook_records']))
if command[0] == 'hostname':
    finish(host(0 if pod == 'eos-mgm1-pod' else 1))
if command[0] == 'getent':
    finish('10.0.0.' + str(state['alias'] + 1) + ' STREAM mock')
if command[0] == 'xrdfs':
    index = state['alias'] if pod == 'eos-cli1-pod' else (0 if pod == 'eos-mgm1-pod' else 1)
    if index == state['leader']:
        finish('leader')
    finish('[3011] No such file or directory', 2)
if command[0] == 'eos':
    position = next(i for i, value in enumerate(command) if value.startswith('root://'))
    operation = command[position + 1:]
    if operation[:2] == ['ns', 'stat']:
        index = 0 if pod == 'eos-mgm1-pod' else 1
        role = 'leader' if index == state['leader'] else 'follower'
        master = host(state['leader']) + ':1094'
        finish(f'ns.mgm.local={host(index)}:1094 ns.mgm.role={role} '
               f'ns.mgm.leader={master} is_master={str(role == "leader").lower()} master_id={master}')
    if operation[:4] == ['attr', '-V', 'get', 'user.mgm_ha']:
        event('metadata-visible', command[position])
        value = state['run_id']
        alias_url = f'root://eos-mgm.{namespace}.svc.cluster.local:1094'
        if post_boot():
            if scenario == 'post-boot-metadata-error':
                finish('metadata read failed', 17)
            if scenario == 'post-boot-metadata-mismatch':
                value += '-corrupted'
        if scenario in ('metadata-prefix', 'metadata-suffix') or (
                scenario in ('alias-metadata-prefix', 'alias-metadata-suffix') and
                command[position] == alias_url):
            value = 'corrupted-' + value if scenario.endswith('prefix') else value + '-corrupted'
        finish(value)
    if operation[:2] == ['attr', 'get']:
        finish('value-only attribute option missing', 91)
    if operation[0] == 'mkdir' and '/alias-' in operation[-1]:
        event('metadata', operation[-1].rsplit('/alias-', 1)[1])
        if scenario == 'mkdir-failure':
            finish('metadata write failed', 17)
    if operation[0] == 'rm':
        state['removed'].append(operation[-1])
        if '/follower-' not in operation[-1]:
            event('cleanup-path')
    if operation[0] == 'ls' and operation[-1] in state['removed']:
        finish('(errc=2) No such file or directory', 2)
    if operation[0] == 'ls' and '/payload-' in operation[-1]:
        filename = operation[-1].rsplit('/', 1)[1]
        event('payload-visible', filename, command[position])
        if (scenario == 'missing-later-payload' and filename == 'payload-first' and
                command[position] == f'root://{host(0)}:1094'):
            finish('(errc=2) No such file or directory', 2)
    finish()
if command[0] == 'mktemp':
    finish('/tmp/eos-mgm-ha.abcdefgh')
if command[0] == 'sha256sum':
    event('checksum', 'download' if command[1].endswith('/download') else 'source')
    download = command[1].endswith('/download')
    if download and post_boot() and scenario == 'post-boot-checksum-command-error':
        finish('a' * 64 + '  ' + command[1], 19)
    bad_digest = download and (scenario == 'checksum-failure' or
                              post_boot() and scenario == 'post-boot-checksum-mismatch')
    digest = 'b' * 64 if bad_digest else 'a' * 64
    finish(digest + '  ' + command[1])
if command[0] == 'xrdcp':
    if command[-2].startswith('root://'):
        filename = command[-2].rsplit('/', 1)[1]
        event('download', filename)
        unavailable = '[3014] Unable to open file /mock/payload; Network is unreachable'
        if post_boot():
            attempt = state['downloads'].get(filename, 0) + 1
            state['downloads'][filename] = attempt
            event('post-boot-download', filename, attempt)
            if (scenario == 'post-boot-transient' and filename == 'payload-initial' and attempt <= 2 or
                    scenario == 'post-boot-persistent'):
                finish(unavailable, 54)
            errors = {
                'post-boot-wrong-status': (unavailable, 13),
                'post-boot-wrong-code': (unavailable.replace('[3014]', '[3011]'), 54),
                'post-boot-missing-code': (unavailable.replace('[3014] ', ''), 54),
                'post-boot-missing-open': (unavailable.replace('Unable to open file ', ''), 54),
                'post-boot-wrong-message': (unavailable.replace('Network is unreachable', 'No servers available'), 54),
                'post-boot-enoent': ('[3011] Unable to open file; No such file or directory', 54),
                'post-boot-denied': ('[3014] Unable to open file; Permission denied', 54),
                'post-boot-timeout': (unavailable, 137),
            }
            if scenario in errors:
                finish(*errors[scenario])
        elif (scenario == 'initial-read-unavailable' or
              scenario == 'runtime-read-unavailable' and not all(state['running'])):
            finish(unavailable, 54)
        finish()
    phase = command[-1].rsplit('payload-', 1)[1]
    attempt = state['uploads'].get(phase, 0) + 1
    state['uploads'][phase] = attempt
    event('upload', phase, attempt)
    message = '[3009] Unable to get free physical space'
    if phase == 'initial':
        if scenario == 'transient' and attempt <= 2 or scenario == 'persistent':
            finish(message, 54)
        if scenario == 'wrong-status':
            finish(message, 13)
        if scenario == 'missing-code':
            finish('Unable to get free physical space', 54)
        if scenario == 'wrong-message':
            finish('[3009] Permission denied', 54)
        if scenario == 'timeout':
            finish(message, 137)
    if phase == 'first' and scenario == 'post-failover':
        finish(message, 54)
    finish()
if command[0] == 'rm':
    event('cleanup-temp')
finish()
'''
TIMEOUT = '''#!/usr/bin/env python3
import os, sys
args = sys.argv[1:]
assert args[:2] == ['-s', 'KILL']
os.execvp(args[3], args[3:])
'''


class BootstrapReadinessTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="eos-ha-bootstrap-test-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.ci = self.root / "gitlab-ci"
        self.ci.mkdir()
        for name in ("test_mgm_ha.sh", "utilities_func_for_tests.sh", "check_mgm_ha_storage.py"):
            shutil.copyfile(SOURCE / name, self.ci / name)
        self.bin = self.root / "bin"
        self.bin.mkdir()
        for name, body in (("kubectl", MOCK_KUBECTL), ("timeout", TIMEOUT)):
            tool = self.bin / name
            tool.write_text(body)
            tool.chmod(0o700)
        self.state = self.root / "state.json"
        self.events_file = self.root / "events.jsonl"
        self.env = dict(os.environ, PATH=f"{self.bin}{os.pathsep}{os.environ['PATH']}",
                        K8S_NAMESPACE=NAMESPACE, KUBECONFIG="/no-real-cluster",
                        MOCK_STATE=str(self.state), MOCK_EVENTS=str(self.events_file))

    def run_scenario(self, scenario):
        self.state.write_text(json.dumps(dict(leader=0, running=[True, True],
                                             alias=None, run_id="", uploads={}, downloads={}, removed=[],
                                             hook_installed=False, hook_index=None,
                                             hook_dir="", hook_records=[], hook_loaded=[False, False])))
        self.events_file.unlink(missing_ok=True)
        if scenario in ("persistent", "post-boot-persistent", "metadata-prefix", "metadata-suffix",
                        "missing-later-payload") or scenario.startswith("boot-hook-"):
            # Shorten only the copied harness script; production keeps its 180s deadline.
            script = self.ci / "test_mgm_ha.sh"
            script.write_text(script.read_text().replace("phase_timeout=180", "phase_timeout=2"))
        start = time.monotonic()
        result = subprocess.run(["bash", str(self.ci / "test_mgm_ha.sh")], cwd=self.root,
                                env=dict(self.env, MOCK_SCENARIO=scenario),
                                capture_output=True, text=True, timeout=25)
        self.elapsed = time.monotonic() - start
        self.events = [json.loads(line) for line in self.events_file.read_text().splitlines()]
        self.cluster = json.loads(self.state.read_text())
        return result

    def uploads(self, phase):
        return [event for event in self.events if event[:2] == ["upload", phase]]

    def post_boot_downloads(self, filename=None):
        return [event for event in self.events if event[0] == "post-boot-download" and
                (filename is None or event[1] == filename)]

    def assert_boot_fixture_restored(self):
        self.assertEqual(self.events.count(["boot-hook", 0]), 1)
        self.assertIn(["hook-restore"], self.events)
        self.assertIn(["hook-remove"], self.events)
        self.assertEqual(self.cluster["running"], [True, True])
        self.assertFalse(self.cluster["hook_installed"])
        self.assertEqual(self.cluster["hook_dir"], "")
        for name in ("cleanup-path", "cleanup-temp", "cleanup-alias"):
            self.assertIn([name], self.events)

    def assert_cleanup_without_failover(self):
        self.assertFalse(any(event[0] == "stop" for event in self.events))
        for name in ("cleanup-path", "cleanup-temp", "cleanup-alias"):
            self.assertIn([name], self.events)

    def test_two_matching_placement_failures_then_success(self):
        result = self.run_scenario("transient")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(self.uploads("initial")), 3)
        first_stop = next(i for i, event in enumerate(self.events) if event[0] == "stop")
        self.assertIn(["checksum", "download"], self.events[:first_stop])
        follower_url = f"root://eos-mgm2.eos-mgm2.{NAMESPACE}.svc.cluster.local:1094"
        self.assertIn(["metadata-visible", follower_url], self.events[:first_stop])
        self.assertIn(["payload-visible", "payload-initial", follower_url], self.events[:first_stop])
        self.assertEqual(self.events.count(["download", "payload-initial"]), 4)
        self.assertEqual(self.events.count(["metadata", "initial"]), 1)
        self.assertEqual(sum(event[0] == "preflight" for event in self.events[:first_stop]), 2)
        self.assertEqual(len(self.uploads("first")), 1)
        self.assertEqual(len(self.uploads("reverse")), 1)
        self.assertIn('PASS:', result.stdout)

    def test_persistent_placement_failure_is_bounded_and_cleans_up(self):
        result = self.run_scenario("persistent")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('initial payload has writable replica placement timed out', result.stderr)
        self.assertGreaterEqual(len(self.uploads("initial")), 1)
        self.assertLessEqual(len(self.uploads("initial")), 2)
        self.assertLess(self.elapsed, 8)
        self.assertEqual(sum(event[0] == "preflight" for event in self.events), 2)
        self.assert_cleanup_without_failover()

    def test_other_errors_and_timeout_fail_without_retry(self):
        for scenario in ("wrong-status", "missing-code", "wrong-message", "timeout"):
            with self.subTest(scenario=scenario):
                result = self.run_scenario(scenario)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn('initial payload upload failed', result.stderr)
                self.assertEqual(len(self.uploads("initial")), 1)
                self.assert_cleanup_without_failover()

    def test_checksum_failure_prevents_first_stop(self):
        result = self.run_scenario("checksum-failure")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('payload checksum mismatch', result.stderr)
        self.assert_cleanup_without_failover()

    def test_mismatched_attribute_prevents_first_stop(self):
        for scenario in ("metadata-prefix", "metadata-suffix",
                         "alias-metadata-prefix", "alias-metadata-suffix"):
            with self.subTest(scenario=scenario):
                result = self.run_scenario(scenario)
                self.assertNotEqual(result.returncode, 0)
                expected = ('metadata changed across failover' if scenario.startswith('alias-')
                            else 'test metadata not visible')
                self.assertIn(expected, result.stderr)
                self.assertEqual(len(self.uploads("initial")), 1)
                self.assert_cleanup_without_failover()

    def test_post_failover_placement_failure_is_not_retried(self):
        result = self.run_scenario("post-failover")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(len(self.uploads("initial")), 1)
        self.assertEqual(len(self.uploads("first")), 1)
        self.assertEqual(self.uploads("reverse"), [])
        self.assertEqual(sum(event[0] == "stop" for event in self.events), 1)
        self.assertIn(["start", 0], self.events)
        self.assertIn(["cleanup-alias"], self.events)

    def test_rejoining_follower_must_see_latest_payload(self):
        result = self.run_scenario("missing-later-payload")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('payload metadata for payload-first not visible', result.stderr)
        self.assertEqual(len(self.uploads("first")), 1)
        self.assertEqual(self.uploads("reverse"), [])
        self.assertEqual(sum(event[0] == "stop" for event in self.events), 1)
        original_url = f"root://eos-mgm1.eos-mgm1.{NAMESPACE}.svc.cluster.local:1094"
        self.assertIn(["payload-visible", "payload-initial", original_url], self.events)
        self.assertIn(["payload-visible", "payload-first", original_url], self.events)
        self.assertIn(["start", 0], self.events)
        for name in ("cleanup-path", "cleanup-temp", "cleanup-alias"):
            self.assertIn([name], self.events)

    def test_preflight_failure_prevents_upload(self):
        result = self.run_scenario("preflight-failure")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('storage preflight failed', result.stderr)
        self.assertEqual(self.uploads("initial"), [])
        self.assert_cleanup_without_failover()

    def test_metadata_failure_prevents_upload(self):
        result = self.run_scenario("mkdir-failure")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.uploads("initial"), [])
        self.assert_cleanup_without_failover()

    def test_boot_master_hook_runs_once_and_restores_the_fixture(self):
        result = self.run_scenario("success")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.events.count(["boot-hook", 0]), 1)
        self.assertIn(["hook-read"], self.events)
        self.assertIn(["hook-restore"], self.events)
        self.assertIn(["hook-remove"], self.events)
        self.assertEqual(self.cluster["running"], [True, True])
        self.assertFalse(self.cluster["hook_installed"])
        self.assertEqual(self.cluster["hook_dir"], "")

    def test_bad_boot_hook_records_fail_and_restore_both_mgms(self):
        for scenario in ("boot-hook-missing", "boot-hook-duplicate", "boot-hook-wrong-identity"):
            with self.subTest(scenario=scenario):
                result = self.run_scenario(scenario)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn('boot post-promotion hook completes exactly once timed out', result.stderr)
                self.assertIn(["hook-restore"], self.events)
                self.assertIn(["hook-remove"], self.events)
                self.assertEqual(self.cluster["running"], [True, True])
                self.assertFalse(self.cluster["hook_installed"])
                self.assertEqual(self.cluster["hook_dir"], "")

    def test_existing_post_hook_is_not_replaced(self):
        result = self.run_scenario("existing-hook")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('refusing to replace an existing post-promotion hook', result.stderr)
        self.assertEqual(sum(event[0] == "stop" for event in self.events), 2)
        self.assertEqual(self.cluster["running"], [True, True])

    def test_failed_start_after_spawn_is_stopped_before_hook_files_are_removed(self):
        result = self.run_scenario("boot-hook-start-after-spawn")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('MGM start failed after spawning xrootd', result.stderr)
        stopped = self.events.index(["stop-if-running", 0])
        self.assertLess(stopped, self.events.index(["hook-remove"]))
        self.assertEqual(self.cluster["running"], [True, True])
        self.assertEqual(self.cluster["hook_loaded"], [False, False])
        self.assertEqual(self.cluster["hook_dir"], "")

    def test_failed_stop_retains_files_for_the_process_still_using_the_hook(self):
        result = self.run_scenario("hook-stop-failure")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('retaining its owned files', result.stdout)
        self.assertIn(["stop-if-running", 0], self.events)
        self.assertNotIn(["hook-remove"], self.events)
        self.assertEqual(self.cluster["running"], [True, True])
        self.assertEqual(self.cluster["hook_loaded"], [True, False])
        self.assertFalse(self.cluster["hook_installed"])
        self.assertNotEqual(self.cluster["hook_dir"], "")

    def test_post_boot_replica_unavailability_is_retried_then_all_payloads_are_hashed(self):
        result = self.run_scenario("post-boot-transient")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(self.post_boot_downloads("payload-initial")), 3)
        for filename in ("payload-first", "payload-reverse"):
            self.assertEqual(len(self.post_boot_downloads(filename)), 1)
        final_reads = next(i for i, event in enumerate(self.events)
                           if event[0] == "post-boot-download")
        self.assertEqual(self.events[final_reads:].count(["checksum", "download"]), 3)
        self.assertEqual([len(self.uploads(phase)) for phase in ("initial", "first", "reverse")],
                         [1, 1, 1])
        self.assertIn('Ready: post-boot payload replicas are readable', result.stdout)
        self.assert_boot_fixture_restored()

    def test_persistent_post_boot_replica_unavailability_is_bounded_and_cleans_up(self):
        result = self.run_scenario("post-boot-persistent")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('post-boot payload replicas are readable with matching checksums timed out',
                      result.stderr)
        self.assertGreaterEqual(len(self.post_boot_downloads()), 1)
        self.assertLessEqual(len(self.post_boot_downloads()), 2)
        self.assertIn('rc=54', result.stderr)
        self.assertIn('[3014]', result.stderr)
        self.assertLess(self.elapsed, 15)
        self.assert_boot_fixture_restored()

    def test_other_post_boot_read_errors_and_timeout_fail_without_retry(self):
        for scenario in ("post-boot-wrong-status", "post-boot-wrong-code", "post-boot-missing-code",
                         "post-boot-missing-open", "post-boot-wrong-message", "post-boot-enoent",
                         "post-boot-denied", "post-boot-timeout"):
            with self.subTest(scenario=scenario):
                result = self.run_scenario(scenario)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn('payload read failed', result.stderr)
                self.assertNotIn('matching checksums timed out', result.stderr)
                self.assertEqual(len(self.post_boot_downloads()), 1)
                self.assert_boot_fixture_restored()

    def test_post_boot_metadata_errors_are_fatal_without_read_retry(self):
        for scenario, message in (("post-boot-metadata-error", 'could not read test metadata'),
                                  ("post-boot-metadata-mismatch", 'metadata changed across failover')):
            with self.subTest(scenario=scenario):
                result = self.run_scenario(scenario)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(message, result.stderr)
                self.assertEqual(self.post_boot_downloads(), [])
                self.assert_boot_fixture_restored()

    def test_post_boot_checksum_errors_are_fatal_without_read_retry(self):
        for scenario, message in (("post-boot-checksum-command-error", 'could not hash downloaded'),
                                  ("post-boot-checksum-mismatch", 'payload checksum mismatch')):
            with self.subTest(scenario=scenario):
                result = self.run_scenario(scenario)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(message, result.stderr)
                self.assertEqual(len(self.post_boot_downloads()), 1)
                self.assert_boot_fixture_restored()

    def test_replica_unavailability_before_boot_remains_a_strict_read_failure(self):
        for scenario, reads in (("initial-read-unavailable", 1), ("runtime-read-unavailable", 2)):
            with self.subTest(scenario=scenario):
                result = self.run_scenario(scenario)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn('payload read failed', result.stderr)
                self.assertEqual(self.events.count(["download", "payload-initial"]), reads)
                self.assertEqual(self.post_boot_downloads(), [])
                self.assertNotIn(["hook-install"], self.events)
                self.assertEqual(self.cluster["running"], [True, True])
                for name in ("cleanup-path", "cleanup-temp", "cleanup-alias"):
                    self.assertIn([name], self.events)


if __name__ == "__main__":
    unittest.main()
