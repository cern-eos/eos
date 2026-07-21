#!/bin/bash

# Exit immediately if a command fails
set -e
set -x

# Establish the cache directory cleanly
export CCACHE_DIR="$(pwd)/ccache"

# Normalize paths below the build root so release-specific build directories do
# not prevent otherwise identical compiler invocations from sharing entries.
if command -v rpm >/dev/null 2>&1; then
    export CCACHE_BASEDIR="$(rpm --eval %_builddir)"
else
    export CCACHE_BASEDIR="$(pwd)"
fi

# Performance tweaks
export CCACHE_NOHASHDIR=true
export CCACHE_MAXSIZE="4G"

# Zero out the old statistics so we only track *this* build's performance
ccache -z

# Optional: Print configuration at the start to verify it looks correct
ccache -p
set +x
