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

# rerepro can not handle the case when a debian package with the same name
# already exists in the repo but with a different contents and will throw
# an error such as:
# ERROR: './ubuntu-jammy/eos-testkeytab_5.2.24-20240726170436git30a72cf26_amd64.deb'
#        cannot be included as 'pool/jammy/commit/e/eos/eos-testkeytab_5.2.24-20240726170436git30a72cf26_amd64.deb'.
# Already existing files can only be included again, if they are the same, but:
# md5 expected: bf50a1c0a6f7d22cdd3e91f1a0095ded, got: 434617b59e9fdf63ca196b833e8244de
# sha1 expected: 605132874877eaa79093ce4b593a489abc33c5b5, got: 4c9707ad3ce7f5511d595a53374a4ea1f2fa4997
#
# Therefore we check upfront if any of the files are already in the repo and we
# skip the publishing step if this is the case.

for RELEASE in "jammy" "noble"; do
  if [ -d ./ubuntu-${RELEASE} ]; then
    EXPORT_REPO="${STCI_ROOT_PATH}/eos/${EOS_CODENAME}"
    mkdir -p ${EXPORT_REPO} || true
    echo "info: Check if packages exist already"
    found=false

    for DEB_PKG in ./ubuntu-${RELEASE}/*.deb; do
      if [ -f ${DEB_PKG} ]; then
        FN=$(basename ${DEB_PKG})
        # Check that find output is not empty  
        if [[ -n "$(find ${EXPORT_REPO}/pool/${RELEASE} -name ${FN} -type f)" ]]; then  
          echo "notice: file already exists ${DEB_PKG}"
          found=true
          break
        fi
      fi
    done

    if [[ "${found}" = true ]]; then
      echo "notice: skip publising for already existing packages"
      continue
    fi

    echo "info: Publishing for: ${RELEASE} in location: ${EXPORT_REPO}"
    reprepro -C ${RELEASE}/${BUILD_TYPE} -Vb ${EXPORT_REPO} includedeb ${RELEASE} ./ubuntu-${RELEASE}/*.deb
  fi
done

exit 0
