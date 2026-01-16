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

for RELEASE in "jammy" "noble" "noble-arm" "plucky"; do
  if [ -d ./ubuntu-${RELEASE} ]; then
    EXPORT_REPO="${STCI_ROOT_PATH}/eos/${EOS_CODENAME}"
    RELEASE_LTS="$(echo ${RELEASE} | cut -d '-' -f1)"
    mkdir -p ${EXPORT_REPO}/pool/${RELEASE_LTS}/${BUILD_TYPE}/e/eos/ || true
    echo "info: Publishing for: ${RELEASE} in location: ${EXPORT_REPO}/pool/${RELEASE_LTS}/${BUILD_TYPE}/e/eos/"

    if ! reprepro -C ${RELEASE_LTS}/${BUILD_TYPE} -Vb ${EXPORT_REPO} includedeb ${RELEASE_LTS} ./ubuntu-${RELEASE}/*.deb; then
        # Delete offending package and retry
        for file in ./ubuntu-${RELEASE}/*.deb; do
            file=$(basename "$file")
            PKG_NAME=$(echo -n "$file" | cut -d '_' -f1)
            PKG_VERSION=$(echo -n "$file" | cut -d '_' -f2)
            PKG_ARCH=$(echo -n "$file" | cut -d '_' -f3 | cut -d '.' -f 1)
            reprepro --keepdirectories -Vb ${EXPORT_REPO} removefilter ${RELEASE_LTS} \
                     'Package (=='${PKG_NAME}'), $Version (=='${PKG_VERSION}'), $Architecture (=='${PKG_ARCH}')'
        done
        # Retry
        mkdir -p ${EXPORT_REPO}/pool/${RELEASE_LTS}/${BUILD_TYPE}/e/eos/ || true
        reprepro -C ${RELEASE_LTS}/${BUILD_TYPE} -Vb ${EXPORT_REPO} includedeb ${RELEASE_LTS} ./ubuntu-${RELEASE}/*.deb
    fi
  fi
done

exit 0
