#!/bin/bash
set -e

############################################################
# Choose the EOS fuse SELinux policy
# Program receives as argument the target directory location
############################################################

WORKDIR=$(dirname "$0")
TARGET_DIR=$1

if [[ ! -d "${TARGET_DIR}" ]]; then
  echo "error: could not find target directory ${TARGET_DIR}"
  exit 1
fi

# Set default distribution as CC7
DIST=7

if [[ -f "/usr/lib/rpm/redhat/dist.sh" ]]; then
  DIST=$(/usr/lib/rpm/redhat/dist.sh --distnum)
fi

if [[ -f "${WORKDIR}/eosfuse-${DIST}.pp" ]]; then
  cp "${WORKDIR}/eosfuse-${DIST}.pp" "${TARGET_DIR}/eosfuse.pp"
else
  cp "${WORKDIR}/eosfuse-7.pp" "${TARGET_DIR}/eosfuse.pp"
fi
