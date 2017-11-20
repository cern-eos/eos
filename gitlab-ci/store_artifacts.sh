#! /bin/bash
set -ex

BRANCH=$1
BUILD_TYPE=$2
PATH_PREFIX=$3

STORAGE_PATH_CC7=${PATH_PREFIX}/${BRANCH}/${BUILD_TYPE}/el-7/x86_64
mkdir -p $STORAGE_PATH_CC7
cp cc7_artifacts/RPMS/* $STORAGE_PATH_CC7
createrepo --update -q $STORAGE_PATH_CC7
STORAGE_PATH_CC7_SRPM=${PATH_PREFIX}/${BRANCH}/${BUILD_TYPE}/el-7/SRPMS
mkdir -p $STORAGE_PATH_CC7_SRPM
cp cc7_artifacts/SRPMS/* $STORAGE_PATH_CC7_SRPM
createrepo --update -q $STORAGE_PATH_CC7_SRPM

STORAGE_PATH_SLC6=${PATH_PREFIX}/${BRANCH}/${BUILD_TYPE}/el-6/x86_64
mkdir -p $STORAGE_PATH_SLC6
cp slc6_artifacts/RPMS/* $STORAGE_PATH_SLC6
createrepo --update -q $STORAGE_PATH_SLC6
STORAGE_PATH_SLC6_SRPM=${PATH_PREFIX}/${BRANCH}/${BUILD_TYPE}/el-6/SRPMS
mkdir -p $STORAGE_PATH_SLC6_SRPM
cp slc6_artifacts/SRPMS/* $STORAGE_PATH_SLC6_SRPM
createrepo --update -q $STORAGE_PATH_SLC6_SRPM

# Allow failures from now on, since the builds for
# these platforms are allowed to fail
set +e

STORAGE_PATH_FCRH=${PATH_PREFIX}/${BRANCH}/${BUILD_TYPE}/fc-rawhide/x86_64
mkdir -p $STORAGE_PATH_FCRH
cp fcrawhide_artifacts/RPMS/* $STORAGE_PATH_FCRH
createrepo --update -q $STORAGE_PATH_FCRH
STORAGE_PATH_FCRH_SRPM=${PATH_PREFIX}/${BRANCH}/${BUILD_TYPE}/fc-rawhide/SRPMS
mkdir -p $STORAGE_PATH_FCRH_SRPM
cp fcrawhide_artifacts/SRPMS/* $STORAGE_PATH_FCRH_SRPM
createrepo --update -q $STORAGE_PATH_FCRH_SRPM

STORAGE_PATH_FCOLD=${PATH_PREFIX}/${BRANCH}/${BUILD_TYPE}/fc-27/x86_64
mkdir -p $STORAGE_PATH_FCOLD
cp fcold_artifacts/RPMS/* $STORAGE_PATH_FCOLD
createrepo --update -q $STORAGE_PATH_FCOLD
STORAGE_PATH_FCOLD_SRPM=${PATH_PREFIX}/${BRANCH}/${BUILD_TYPE}/fc-27/SRPMS
mkdir -p $STORAGE_PATH_FCOLD_SRPM
cp fcold_artifacts/SRPMS/* $STORAGE_PATH_FCOLD_SRPM
createrepo --update -q $STORAGE_PATH_FCOLD_SRPM

STORAGE_PATH_MACOS=${PATH_PREFIX}/${BRANCH}/${BUILD_TYPE}/osx/x86_64
mkdir -p $STORAGE_PATH_MACOS
cp osx_artifacts/* $STORAGE_PATH_MACOS

exit 0
