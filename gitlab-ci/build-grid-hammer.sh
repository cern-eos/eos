#!/bin/bash
set -euo pipefail

# The published grid-hammer RPM links against XRootD 5. Build the XRootD-only
# helper with the test image's client headers and libraries instead.
readonly revision=8576682a83d5327e303ccb39f949a82d299631df
readonly output_dir=${1:?usage: build-grid-hammer.sh OUTPUT_DIR}
build_dir=$(mktemp -d)
trap 'rm -rf "$build_dir"' EXIT

git clone --quiet https://gitlab.cern.ch/lcgdm/grid-hammer.git "$build_dir/source"
git -C "$build_dir/source" checkout --quiet --detach "$revision"
test "$(git -C "$build_dir/source" rev-parse HEAD)" = "$revision"

mkdir -p "$output_dir"
c++ -std=c++11 -O2 -g -pthread \
  -I "$build_dir/source/deps" -isystem /usr/include/xrootd \
  "$build_dir/source/src/hammer-xroot.cc" \
  "$build_dir/source/src/OperationExecutor.cc" \
  "$build_dir/source/src/XrdClExecutor.cc" \
  -lXrdCl -lboost_thread -o "$output_dir/hammer-xroot"

# These deprecated imports were removed in Python 3.12/3.13; distutils is
# unused, and shlex.quote provides the same quoting used by the runner.
sed -e '/^import distutils.spawn$/d' \
  -e 's/^from pipes import quote$/from shlex import quote/' \
  "$build_dir/source/hammer-runner.py" > "$output_dir/hammer-runner.py"
