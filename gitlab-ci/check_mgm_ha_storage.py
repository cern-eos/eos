#!/usr/bin/env python3
# Copyright (C) 2026 CERN/Switzerland. GPL-3.0-or-later.
"""Check actual HA fixture storage before exercising replicated writes."""

import os
from pathlib import Path
import pwd
import sys
import tempfile


def capacity(path):
    stats = os.statvfs(path)
    if not stats.f_blocks:
        raise RuntimeError(f"{path}: filesystem has no reported capacity")
    available = stats.f_frsize * stats.f_bavail
    free_percent = 100.0 * stats.f_bfree / stats.f_blocks
    print(
        f"[mgm-ha] actual storage path={path} available_bytes={available} "
        f"free_percent={free_percent:.2f} available_inodes={stats.f_favail}",
        flush=True,
    )
    return available, free_percent, stats.f_favail


def main(data_path):
    var_available, var_percent, var_inodes = capacity("/var")
    if var_available < 32 * 1024 * 1024 or var_percent < 10 or var_inodes < 128:
        raise RuntimeError("/var lacks headroom above the unchanged 5% FST guard")
    data_available, _, data_inodes = capacity(data_path)
    if data_available < 16 * 1024 * 1024 or data_inodes < 128:
        raise RuntimeError("actual FST data filesystem lacks space or inodes")

    daemon = pwd.getpwnam("daemon")
    if os.geteuid() == 0:
        os.initgroups(daemon.pw_name, daemon.pw_gid)
        os.setgid(daemon.pw_gid)
        os.setuid(daemon.pw_uid)
    elif os.geteuid() != daemon.pw_uid:
        raise RuntimeError("data probe must run as the FST daemon account")

    descriptor, filename = tempfile.mkstemp(prefix=".eos-ha-probe-", dir=data_path)
    probe = Path(filename)
    try:
        payload = b"EOS MGM HA storage preflight\n"
        with os.fdopen(descriptor, "wb") as output:
            output.write(payload)
            output.flush()
            os.fsync(output.fileno())
        os.setxattr(probe, "user.eos.ha_probe", b"verified")
        if os.getxattr(probe, "user.eos.ha_probe") != b"verified":
            raise RuntimeError("daemon data xattr did not round-trip")
        if probe.read_bytes() != payload:
            raise RuntimeError("daemon data write did not round-trip")
    finally:
        probe.unlink()
    print("[mgm-ha] daemon data write/read/xattr preflight passed", flush=True)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: check_mgm_ha_storage.py FST_DATA_PATH")
    try:
        main(sys.argv[1])
    except (OSError, RuntimeError, KeyError) as error:
        raise SystemExit(f"[mgm-ha] ERROR: storage preflight failed: {error}")
