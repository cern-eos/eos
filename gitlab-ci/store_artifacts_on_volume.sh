#! /bin/bash

BRANCH=$1
BUILD_TYPE=$2

if [ "$BUILD_TYPE" == "tag" ];
then
  STORAGE_PATH_CC7=/mnt/eos_repositories/eos/${BRANCH}/${BUILD_TYPE}/el-7/x86_64
  mkdir -p $STORAGE_PATH_CC7
  cp cc7_artifacts/* $STORAGE_PATH_CC7
  createrepo -q $STORAGE_PATH_CC7

  STORAGE_PATH_SLC6=/mnt/eos_repositories/eos/${BRANCH}/${BUILD_TYPE}/el-6/x86_64
  cp slc6_artifacts/* $STORAGE_PATH_SLC6
  createrepo -q $STORAGE_PATH_SLC6

  STORAGE_PATH_FCRH=/mnt/eos_repositories/eos/${BRANCH}/${BUILD_TYPE}/fc-rawhide/x86_64
  mkdir -p $STORAGE_PATH_FCRH
  cp fcrawhide_artifacts/* $STORAGE_PATH_FCRH
  createrepo -q $STORAGE_PATH_FCRH

  STORAGE_PATH_FCOLD=/mnt/eos_repositories/eos/${BRANCH}/${BUILD_TYPE}/fc-24/x86_64
  mkdir -p $STORAGE_PATH_FCOLD
  cp fcold_artifacts/* $STORAGE_PATH_FCOLD
  createrepo -q $STORAGE_PATH_FCOLD

  STORAGE_PATH_MACOS=/mnt/eos_repositories/eos/${BRANCH}/${BUILD_TYPE}/osx/x86_64
  mkdir -p $STORAGE_PATH_MACOS
  cp osx_artifacts/* $STORAGE_PATH_MACOS
fi

exit 0
