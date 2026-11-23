#!/usr/bin/env python3
# Copyright (C) 2026 CERN/Switzerland. GPL-3.0-or-later.
"""Convert CMake warnings in an RPM build log to GitLab Code Quality."""

from collections import Counter
import hashlib
import json
from pathlib import Path
import re
import sys


WARNING = re.compile(
    r"^CMake (?P<kind>Deprecation Warning|Warning)(?: \((?P<category>[^)]+)\))?"
    r"(?: at (?P<path>.+):(?P<line>\d+) \((?P<command>[^)]+)\)| in (?P<file>.+))?"
    r":[ \t]*$\n?(?P<message>(?:^[ \t]+\S[^\n]*\n?|^[ \t]*\n(?=[ \t]+\S))*)",
    re.MULTILINE,
)


def source_location(warning, root):
    source = warning["path"] or warning["file"]
    if source:
        # CMake may report a path inside rpmbuild's extracted source tarball.
        parts = Path(source).parts
        for start in range(len(parts)):
            relative = Path(*parts[start:])
            if not relative.is_absolute() and ".." not in relative.parts:
                if (root / relative).is_file():
                    return relative.as_posix(), int(warning["line"] or 1)
    return "CMakeLists.txt", 1


def code_quality(log, root):
    occurrences = Counter()
    report = []
    for warning in WARNING.finditer(log):
        path, line = source_location(warning, root)
        category = warning["category"] or (
            "deprecated" if warning["kind"] == "Deprecation Warning" else "warning"
        )
        check = f"cmake-{category}"
        message = " ".join(warning["message"].split()) or warning["kind"]
        command = warning["command"]
        identity = (check, path, command, message)
        occurrences[identity] += 1
        # Keep fingerprints stable when a warning moves to a different line.
        fingerprint = hashlib.sha256(
            repr((identity, occurrences[identity])).encode("utf-8")
        ).hexdigest()
        report.append({
            "check_name": check,
            "description": f"{command}: {message}" if command else message,
            "fingerprint": fingerprint,
            "location": {"path": path, "lines": {"begin": line}},
            "severity": "minor",
        })
    return report


if __name__ == "__main__":
    log_path, report_path = map(Path, sys.argv[1:])
    log = log_path.read_text(encoding="utf-8", errors="replace")
    report = code_quality(log, Path(__file__).resolve().parents[1])
    report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"CMake warnings in the RPM build: {len(report)}")
