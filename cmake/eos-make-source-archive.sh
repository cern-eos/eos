#!/bin/bash
# ----------------------------------------------------------------------
# File: eos-make-source-archive.sh
# ----------------------------------------------------------------------
#
# Build a source tarball of the EOS project (including all git submodules)
# using `git archive`.
#
# Usage:
#   eos-make-source-archive.sh <source_dir> <output_file> <prefix> [<spec_file>]
#
#   source_dir   absolute path to the top-level git working tree
#   output_file  absolute path of the .tar.gz to produce
#   prefix       top-level directory inside the archive (e.g. eos-5.4.4-1)
#   spec_file    optional path to a generated eos.spec to embed at <prefix>/
# ----------------------------------------------------------------------

set -euo pipefail

if [ $# -lt 3 ]; then
  echo "Usage: $0 <source_dir> <output_file> <prefix> [<spec_file>]" >&2
  exit 1
fi

SOURCE_DIR="$1"
OUTPUT="$2"
PREFIX="$3"
SPEC_FILE="${4:-}"

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

ARCHIVE="$TMP/archive.tar"

cd "$SOURCE_DIR"

# Top-level repository.
# --worktree-attributes ensures .gitattributes (with export-ignore entries)
# is honored even if the file isn't yet committed.
git archive --worktree-attributes \
            --format=tar --prefix="${PREFIX}/" -o "$ARCHIVE" HEAD

# Submodules (recursive). Each produces its own tar which is then
# concatenated into the main archive. $displaypath is the submodule
# path relative to the superproject root.
export EOS_ARCHIVE_TMP="$TMP"
export EOS_ARCHIVE_PREFIX="$PREFIX"

git submodule foreach --recursive --quiet '
  safe=$(echo "$displaypath" | tr "/" "_")
  git archive --format=tar \
    --prefix="${EOS_ARCHIVE_PREFIX}/${displaypath}/" \
    -o "${EOS_ARCHIVE_TMP}/sub-${safe}.tar" HEAD
'

shopt -s nullglob
for sub in "$TMP"/sub-*.tar; do
  tar --concatenate --file="$ARCHIVE" "$sub"
  rm -f "$sub"
done
shopt -u nullglob

# Embed the generated spec file (not tracked in git) at <prefix>/eos.spec
if [ -n "$SPEC_FILE" ] && [ -f "$SPEC_FILE" ]; then
  STAGE=$(mktemp -d)
  mkdir -p "${STAGE}/${PREFIX}"
  cp "$SPEC_FILE" "${STAGE}/${PREFIX}/$(basename "$SPEC_FILE")"
  tar --append --file="$ARCHIVE" -C "$STAGE" "${PREFIX}/$(basename "$SPEC_FILE")"
  rm -rf "$STAGE"
fi

# Compress (use -n for reproducible output: no timestamp / original name)
gzip -n -c "$ARCHIVE" > "$OUTPUT"

echo "Created source archive: $OUTPUT"
