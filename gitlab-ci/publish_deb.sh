#!/usr/bin/env bash

#-------------------------------------------------------------------------------
# Publish debian artifacts from the CERN Gitlab CI.
#
# Usage: $0 <build_type> <version>
#
# where <build_type> can be "tag" or "commit" and <version> is the version of
# the eos packages being published, either as major.minor or as a full version
# out of which only major.minor is used.
#
# The build jobs store their artifacts under ${CI_JOB_NAME}, so this script sees
# one directory per build. Everything after the first "-" in the name is a
# runner variant, not a separate distribution:
#
#   ./ubuntu-jammy      -> jammy
#   ./ubuntu-noble      -> noble
#   ./ubuntu-noble-arm  -> noble      (arm64 runner, same release)
#   ./ubuntu-resolute   -> resolute
#
# Packages are published as one reprepro distribution per Ubuntu release, split
# into components by build type and by the major.minor version of the eos
# packages being imported:
#
#   Codename:   jammy
#   Components: jammy/tag/5.3 jammy/tag/5.4 jammy/commit/5.4 ...
#
# so that several major.minor versions can live side by side and a client picks
# the one it wants:
#
#   deb http://storage-ci.web.cern.ch/storage-ci/debian/eos/diopside jammy jammy/tag/5.4 jammy/deps/5.4
#
# The distribution name is repeated inside the component on purpose: the pool
# lives under pool/<component>/, and the same version built for two releases
# produces identically named .deb files with different content, which reprepro
# refuses to keep in one pool directory.
#
# Within one component the stock reprepro keeps the newest version of every
# package, which is all that is needed once the series are separated: a new
# 5.4.10 replaces 5.4.9 in jammy/tag/5.4 while jammy/tag/5.3 is left alone.
#-------------------------------------------------------------------------------
set -ex

if [[ $# -ne 2 ]]; then
  echo "error: wrong number of parameters, please specify <build_type> <version>"
  exit 1
fi

BUILD_TYPE="$1"
EOS_VERSION="$2"

# Build type needs to be either tag or commit
if [[ ! $BUILD_TYPE =~ ^(tag|commit)$ ]]; then
  echo "error: unknown <build_type> given"
  exit 2
fi

# Version is major.minor.patch for tags and major.minor.patch-<date>git<hash>
# for commits, only the first two fields select the component.
if [[ ! $EOS_VERSION =~ ^[0-9]+\.[0-9]+ ]]; then
  echo "error: <version> must start with major.minor"
  exit 2
fi

EOS_CODENAME="diopside"
MAJOR_MINOR="$(echo ${EOS_VERSION} | cut -d '.' -f1,2)"
STCI_ROOT_PATH="/eos/project/s/storage-ci/www/ubuntu"
EXPORT_REPO="${STCI_ROOT_PATH}/eos/${EOS_CODENAME}"

for RELEASE in "jammy" "noble" "noble-arm" "resolute"; do
  if [ -d ./ubuntu-${RELEASE} ]; then
    RELEASE_LTS="$(echo ${RELEASE} | cut -d '-' -f1)"
    COMPONENT="${RELEASE_LTS}/${BUILD_TYPE}/${MAJOR_MINOR}"
    echo "info: Publishing for: ${RELEASE} version: ${EOS_VERSION} in location: ${EXPORT_REPO}/pool/${COMPONENT}/e/eos/"
    reprepro -C ${COMPONENT} -Vb ${EXPORT_REPO} includedeb ${RELEASE_LTS} ./ubuntu-${RELEASE}/*.deb
  fi
done

exit 0
