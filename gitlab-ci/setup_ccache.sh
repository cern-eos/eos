#!/bin/bash

# Exit immediately if a command fails
set -e
set -x

# Establish the cache directory cleanly
export CCACHE_DIR="$(pwd)/ccache"

# Determine the build base directory depending on the distribution.
# On RPM-based systems, builds happen under %_builddir; on Debian-based
# systems, dpkg-buildpackage builds in place so the base dir is just pwd.
if command -v rpm >/dev/null 2>&1; then
    # Robust and fast CCACHE_BASEDIR extraction using pure Bash
    # Find the first src.rpm without forking 'ls'
    SRC_RPM=$(first_match=(build/SRPMS/*.src.rpm); echo "${first_match[0]}")

    if [ -f "$SRC_RPM" ]; then
        # Get just the filename (e.g., app-5.4.3-1.src.rpm)
        RPM_FILE=$(basename "$SRC_RPM")
        # Strip off '.src.rpm' and the release/version suffix safely
        # Adjust the parameter expansion below if your naming convention varies
        RPM_NAME="${RPM_FILE%%-*}"
        export CCACHE_BASEDIR="$(rpm --eval %_builddir)/$RPM_NAME"
    else
        # Fallback if no SRPM exists yet
        export CCACHE_BASEDIR="$(rpm --eval %_builddir)"
    fi
else
    export CCACHE_BASEDIR="$(pwd)"
fi

# Optimize Sloppiness to fix 0% Direct Mode Hit Rate
# 'time_macros' allows ccache to ignore the dynamic -DRELEASE timestamp in Direct Mode
export CCACHE_SLOPPINESS="pch_defines,time_macros"

# This explicitly tells ccache to ignore the value of -DRELEASE in the compiler flags
export CCACHE_IGNOREOPTIONS="-DRELEASE=*"

# Performance tweaks
export CCACHE_NOHASHDIR=true
export CCACHE_MAXSIZE="4G"

# Zero out the old statistics so we only track *this* build's performance
ccache -z

# Optional: Print configuration at the start to verify it looks correct
ccache -p
set +x
