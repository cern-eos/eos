#!/usr/bin/env bash

#-------------------------------------------------------------------------------
# Publish debian artifacts from the CERN Gitlab CI.
#
# Usage: $0 <build_type>
#
# where <build_type> can be "tag" or "commit"
#-------------------------------------------------------------------------------
set -ex

if [[ $# -ne 1 ]]; then
  echo "error: no parameter given, please specify <build_type>"
  exit 1
fi

BUILD_TYPE="$1"

# Build type needs to be either tag or commit
if [[ ! $BUILD_TYPE =~ ^(tag|commit)$ ]]; then
  echo "error: unknown <build_type> given"
  exit 2
fi

EOS_CODENAME="diopside"
STCI_ROOT_PATH="/eos/project/s/storage-ci/www/debian"

for RELEASE in "jammy" "noble"; do
  if [ -d ./ubuntu-${RELEASE} ]; then
    EXPORT_REPO="${STCI_ROOT_PATH}/eos/${EOS_CODENAME}"
    mkdir -p ${EXPORT_REPO} || true
    echo "info: Publishing for: ${RELEASE} in location: ${EXPORT_REPO}"
    reprepro -C ${RELEASE}/${BUILD_TYPE} -Vb ${EXPORT_REPO} includedeb ${RELEASE} ./ubuntu-${RELEASE}/*.deb
  fi
done

exit 0
