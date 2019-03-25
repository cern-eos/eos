#!/bin/bash

#-------------------------------------------------------------------------------
# This script is used to publish EOS RPMS built in the CI
# against the latest XRootD-testing RPMS.
#-------------------------------------------------------------------------------

set -x

BRANCH=$1
BUILD_TYPE=$2
PATH_PREFIX=$3

# Upload RPMS
STORAGE_PATH_CC7=${PATH_PREFIX}/${BRANCH}/${BUILD_TYPE}/el-7/x86_64
mkdir -p $STORAGE_PATH_CC7
cp cc7_artifacts_xrd_testing/RPMS/* $STORAGE_PATH_CC7
createrepo --update -q $STORAGE_PATH_CC7

# Upload SRPMS
STORAGE_PATH_CC7_SRPM=${PATH_PREFIX}/${BRANCH}/${BUILD_TYPE}/el-7/SRPMS
mkdir -p $STORAGE_PATH_CC7_SRPM
cp cc7_artifacts_xrd_testing/SRPMS/* $STORAGE_PATH_CC7_SRPM
createrepo --update -q $STORAGE_PATH_CC7_SRPM

exit 0
