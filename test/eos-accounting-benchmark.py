#!/usr/bin/env python3

import argparse
import json
import os
import re
import statistics
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed


def run_command(args, check=True, capture=True):
    stdout = subprocess.PIPE if capture else subprocess.DEVNULL
    stderr = subprocess.PIPE if capture else subprocess.DEVNULL
    proc = subprocess.run(args, stdout=stdout, stderr=stderr, text=True)

    if check and proc.returncode != 0:
        raise RuntimeError(
            "command failed rc={} cmd={} stdout={} stderr={}".format(
                proc.returncode, " ".join(args), proc.stdout, proc.stderr))

    return proc


def percentile(values, pct):
    if not values:
        return None

    ordered = sorted(values)
    idx = int(round((len(ordered) - 1) * pct / 100.0))
    return ordered[idx]


def eos_url(host, path):
    return "root://{}/{}".format(host, path)


def join_eos_path(*parts):
    cleaned = []

    for idx, part in enumerate(parts):
        if idx == 0:
            cleaned.append(part.rstrip("/"))
        else:
            cleaned.append(part.strip("/"))

    return "/".join(cleaned)


def parse_fileinfo(path):
    proc = run_command(["eos", "fileinfo", path, "-m"])
    out = proc.stdout

    def extract(name):
        match = re.search(r"{}=([^ ]+)".format(name), out)

        if not match:
            raise RuntimeError("missing {} in fileinfo output: {}".format(name, out))

        return int(match.group(1))

    return {
        "treecontainers": extract("treecontainers"),
        "treefiles": extract("treefiles"),
        "treesize": extract("treesize"),
    }


def eos_mkdir(path):
    run_command(["eos", "mkdir", "-p", path], capture=False)


def eos_rm_tree(path):
    proc = run_command(["eos", "rm", "-rF", "--no-confirmation", path],
                       check=False)

    if proc.returncode and "no such file or directory" not in proc.stderr:
        raise RuntimeError("cleanup failed for {}: {}".format(path, proc.stderr))


def eos_cp(local_file, host, path):
    return run_command(["eos", "cp", local_file, eos_url(host, path)],
                       check=False)


def wait_accounting(path, expected_files, expected_size, timeout):
    deadline = time.time() + timeout
    last = None

    while time.time() <= deadline:
        last = parse_fileinfo(path)

        if (last["treefiles"] == expected_files and
                last["treesize"] == expected_size):
            return time.time()

        time.sleep(1)

    raise RuntimeError(
        "timed out waiting for accounting on {} expected files={} size={} last={}"
        .format(path, expected_files, expected_size, last))


class OperationStats:
    def __init__(self):
        self.lock = threading.Lock()
        self.successes = 0
        self.failures = 0
        self.latencies = []
        self.operations = []

    def record(self, start, end, success):
        with self.lock:
            if success:
                self.successes += 1
            else:
                self.failures += 1

            self.latencies.append(end - start)
            self.operations.append((start, end, success))

    def snapshot_successes(self):
        with self.lock:
            return self.successes

    def snapshot(self):
        with self.lock:
            return {
                "successes": self.successes,
                "failures": self.failures,
                "latencies": list(self.latencies),
                "operations": list(self.operations),
            }


def populate_tree(args, source_size, name):
    tree = join_eos_path(args.prefix, name)
    eos_mkdir(tree)

    directories = tree_directories(args, tree)

    for directory in directories:
        eos_mkdir(directory)

    def copy_one(directory, file_idx):
        dst = join_eos_path(directory, "file_{}".format(file_idx))
        proc = eos_cp(args.source_file, args.host, dst)

        if proc.returncode != 0:
            raise RuntimeError("failed to copy {}: {}".format(dst, proc.stderr))

    futures = []
    with ThreadPoolExecutor(max_workers=args.populate_workers) as executor:
        for directory in directories:
            for file_idx in range(args.target_files):
                futures.append(executor.submit(copy_one, directory, file_idx))

        for future in as_completed(futures):
            future.result()

    expected_files = len(directories) * args.target_files
    expected_size = expected_files * source_size
    return tree, expected_files, expected_size, activity_write_dirs(args, directories)


def tree_directories(args, root):
    if args.tree_layout == "wide":
        return [join_eos_path(root, str(directory))
                for directory in range(args.target_dirs)]

    directories = []
    current = root

    for directory in range(args.tree_depth):
        current = join_eos_path(current, str(directory))
        directories.append(current)

    return directories


def activity_write_dirs(args, directories):
    if args.tree_layout == "deep":
        return [directories[-1]]

    return directories


def benchmark_directory_count(args):
    if args.tree_layout == "deep":
        return args.tree_depth

    return args.target_dirs


def planned_setup(args, source_size):
    directories_per_tree = benchmark_directory_count(args)
    files_per_tree = directories_per_tree * args.target_files

    return {
        "tree_layout": args.tree_layout,
        "directories_per_benchmark_tree": directories_per_tree,
        "files_per_benchmark_tree": files_per_tree,
        "benchmark_trees": 2,
        "benchmark_root_directories": 1,
        "tree_root_directories": 2,
        "data_directories": directories_per_tree * 2,
        "total_initial_directories": 1 + 2 + (directories_per_tree * 2),
        "total_initial_files": files_per_tree * 2,
        "total_initial_size": files_per_tree * 2 * source_size,
        "activity_phase_files": "depends on runtime and writer throughput",
    }


def writer_worker(args, write_dirs, worker_id, stop_event, stats, phase_name):
    seq = 0
    worker_dir = write_dirs[worker_id % len(write_dirs)]

    while not stop_event.is_set():
        dst = join_eos_path(worker_dir, "{}_w{}_file_{}".format(
            phase_name, worker_id, seq))
        start = time.time()
        proc = eos_cp(args.source_file, args.host, dst)
        end = time.time()
        stats.record(start, end, proc.returncode == 0)
        seq += 1


def summarize_latencies_ms(latencies):
    if not latencies:
        return {
            "count": 0,
            "avg": None,
            "p50": None,
            "p95": None,
            "max": None,
        }

    return {
        "count": len(latencies),
        "avg": statistics.mean(latencies) * 1000.0,
        "p50": percentile(latencies, 50) * 1000.0,
        "p95": percentile(latencies, 95) * 1000.0,
        "max": max(latencies) * 1000.0,
    }

def summarize_recompute_results(results):
    elapsed = [r["elapsed"] for r in results]
    failures = [r for r in results if r["rc"] != 0]
    summary = summarize_latencies_ms(elapsed)

    return {
        "iterations": len(results),
        "failures": len(failures),
        "avg": summary["avg"],
        "p50": summary["p50"],
        "p95": summary["p95"],
        "max": summary["max"],
        "failed_iterations": [
            {
                "iteration": r["iteration"],
                "rc": r["rc"],
                "elapsed_ms": r["elapsed"] * 1000.0,
            }
            for r in failures
        ],
    }


def summarize_eos_cp_results(snapshot):
    return {
        "commands": len(snapshot["latencies"]),
        "failures": snapshot["failures"],
        **summarize_latencies_ms(snapshot["latencies"]),
    }


def run_recompute_iterations(args, path):
    results = []

    for iteration in range(args.iterations):
        print("-- recompute {} iteration {}/{}".format(
            path, iteration + 1, args.iterations))
        start = time.time()
        proc = run_command(["eos", "ns", "recompute_tree_size", path],
                           check=False)
        end = time.time()
        results.append({
            "iteration": iteration,
            "rc": proc.returncode,
            "elapsed": end - start,
        })
        print("OK: recompute rc={} elapsed={:.3f}s".format(
            proc.returncode, end - start))

        if iteration + 1 < args.iterations:
            time.sleep(args.between_iterations)

    return results


def run_activity_phase(args, name, activity_path, activity_dirs, source_size,
                       baseline_files, baseline_size, recompute_path=None):
    stop_event = threading.Event()
    stats = OperationStats()
    workers = []
    recompute_results = []

    print("-- Starting phase: {}".format(name))
    print("-- Activity path: {}".format(activity_path))

    if recompute_path:
        print("-- Recompute path: {}".format(recompute_path))
    else:
        print("-- Recompute path: none")

    for worker_id in range(args.activity_workers):
        thread = threading.Thread(target=writer_worker,
                                  args=(args, activity_dirs, worker_id,
                                        stop_event, stats, name))
        thread.daemon = True
        workers.append(thread)
        thread.start()

    if recompute_path:
        recompute_results = run_recompute_iterations(args, recompute_path)
    else:
        time.sleep(args.baseline_seconds)

    stop_event.set()

    for worker in workers:
        worker.join()

    expected_files = baseline_files + stats.snapshot_successes()
    expected_size = baseline_size + (stats.snapshot_successes() * source_size)
    catchup_start = time.time()
    final_info = None

    while time.time() - catchup_start <= args.catchup_timeout:
        final_info = parse_fileinfo(activity_path)

        if (final_info["treefiles"] >= expected_files and
                final_info["treesize"] >= expected_size):
            break

        time.sleep(args.sample_interval)

    snapshot = stats.snapshot()
    final_lag_files = max(expected_files - final_info["treefiles"], 0)
    final_lag_size = max(expected_size - final_info["treesize"], 0)

    return {
        "scenario": name,
        "expected_files_after_phase": expected_files,
        "expected_size_after_phase": expected_size,
        "final_lag_files": final_lag_files,
        "final_lag_size": final_lag_size,
        "recompute_tree_size_ms": summarize_recompute_results(
            recompute_results),
        "eos_cp_ms": summarize_eos_cp_results(snapshot),
    }


def main():
    parser = argparse.ArgumentParser(
        description="Benchmark accounting lag and recompute_tree_size impact.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("prefix", metavar="EOS_PATH",
                        help="EOS root path where benchmark subtrees are "
                             "created")
    parser.add_argument("--host", default="localhost",
                        help="EOS MGM host")
    parser.add_argument("--label", default="benchmark",
                        help="label printed in the benchmark log")
    parser.add_argument("--source-file", default="/etc/passwd",
                        help="local file copied by benchmark writers")
    parser.add_argument("--target-dirs", type=int, default=50,
                        help="sibling directories created in wide tree layout")
    parser.add_argument("--tree-layout", choices=("wide", "deep"),
                        default="wide",
                        help="benchmark tree shape")
    parser.add_argument("--tree-depth", type=int, default=1,
                        help="directory chain depth in deep tree layout")
    parser.add_argument("--target-files", type=int, default=10,
                        help="files initially created per benchmark directory")
    parser.add_argument("--activity-workers", "--target-workers",
                        dest="activity_workers", type=int, default=4,
                        help="writers creating files in the measured activity "
                             "tree")
    parser.add_argument("--noise-workers", dest="activity_workers", type=int,
                        help=argparse.SUPPRESS)
    parser.add_argument("--populate-workers", type=int, default=16,
                        help="parallel workers used during initial population")
    parser.add_argument("--iterations", type=int, default=5,
                        help="number of recompute_tree_size commands to run "
                             "per recompute phase")
    parser.add_argument("--baseline-seconds", "--warmup-seconds",
                        dest="baseline_seconds", type=float, default=5.0,
                        help="seconds to run the no-recompute baseline phase")
    parser.add_argument("--between-iterations", type=float, default=1.0,
                        help="seconds to wait between recompute iterations")
    parser.add_argument("--sample-interval", type=float, default=1.0,
                        help="seconds between final accounting catch-up checks")
    parser.add_argument("--accounting-timeout", type=int, default=60,
                        help="seconds to wait for initial accounting catch-up")
    parser.add_argument("--catchup-timeout", type=int, default=60,
                        help="seconds to wait for final accounting catch-up")
    parser.add_argument("--describe-only", action="store_true",
                        help="print planned setup size and exit without "
                             "modifying EOS")
    parser.add_argument("--keep", action="store_true",
                        help="Keep benchmark data after completion")
    args = parser.parse_args()

    args.prefix = args.prefix.rstrip("/")

    if args.target_dirs < 1:
        raise ValueError("--target-dirs must be at least 1")

    if args.tree_depth < 1:
        raise ValueError("--tree-depth must be at least 1")

    if args.target_files < 0:
        raise ValueError("--target-files must be non-negative")

    if args.activity_workers < 1:
        raise ValueError("--activity-workers must be at least 1")

    source_size = os.stat(args.source_file).st_size
    setup_plan = planned_setup(args, source_size)

    if args.describe_only:
        print(json.dumps(setup_plan, indent=2, sort_keys=True))
        return 0

    print("=== EOS Accounting Benchmark ===")
    print("label={}".format(args.label))
    print("prefix={}".format(args.prefix))
    print("host={}".format(args.host))
    print("source_file={} source_size={}".format(args.source_file, source_size))
    print("tree_layout={} tree_depth={} target_dirs={} target_files={}".format(
        args.tree_layout, args.tree_depth, args.target_dirs, args.target_files))
    print("planned_initial_directories={} planned_initial_files={}".format(
        setup_plan["total_initial_directories"],
        setup_plan["total_initial_files"]))

    eos_rm_tree(args.prefix)
    eos_mkdir(args.prefix)

    print("-- Populating measured activity subtree")
    activity, activity_files, activity_size, activity_dirs = populate_tree(
        args, source_size, "activity")
    print("OK: activity={} files={} size={}".format(
        activity, activity_files, activity_size))

    print("-- Populating separate recompute subtree")
    other_recompute, other_files, other_size, _ = populate_tree(
        args, source_size, "recompute-other")
    print("OK: recompute-other={} files={} size={}".format(
        other_recompute, other_files, other_size))

    print("-- Waiting for initial accounting before benchmark")
    wait_accounting(activity, activity_files, activity_size,
                    args.accounting_timeout)
    wait_accounting(other_recompute, other_files, other_size,
                    args.accounting_timeout)
    print("OK: initial accounting caught up")

    phases = []
    current_files = activity_files
    current_size = activity_size

    phase = run_activity_phase(args, "no_recompute", activity, activity_dirs,
                               source_size, current_files, current_size)
    phases.append(phase)
    current_files = phase["expected_files_after_phase"]
    current_size = phase["expected_size_after_phase"]

    phase = run_activity_phase(args, "recompute_different_tree", activity,
                               activity_dirs, source_size, current_files,
                               current_size, other_recompute)
    phases.append(phase)
    current_files = phase["expected_files_after_phase"]
    current_size = phase["expected_size_after_phase"]

    phase = run_activity_phase(args, "recompute_same_tree", activity,
                               activity_dirs, source_size, current_files,
                               current_size, activity)
    phases.append(phase)

    result = {
        "scenarios": [
            {
                "scenario": phase["scenario"],
                "recompute_tree_size_ms": phase["recompute_tree_size_ms"],
                "eos_cp_ms": phase["eos_cp_ms"],
            }
            for phase in phases
        ],
    }

    print("=== Summary ===")
    print(json.dumps(result, indent=2, sort_keys=True))

    if not args.keep:
        print("-- Cleaning up benchmark data")
        eos_rm_tree(args.prefix)
        print("OK: cleanup completed")

    if any(phase["final_lag_files"] or phase["final_lag_size"]
           for phase in phases):
        return 3

    return 0


if __name__ == "__main__":
    sys.exit(main())
