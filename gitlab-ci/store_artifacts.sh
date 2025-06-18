#!/bin/bash

#-------------------------------------------------------------------------------
# Publish artifacts from CERN Gitlab CI.
# The script will only upload artifacts from builds found in the buildmap.
#
# To add a new build type, register it in the buildmap together
# with the repo name at the storage endpoint.
#
# E.g: cc7 --> el-7
#      storage endpoint: /eos/project/s/storage-ci/www/eos/citrine/commit/el-7/
#-------------------------------------------------------------------------------
set -ex

# Define a mapping between builds and repos
declare -A BUILDMAP

BUILDMAP[cc7]=el-7
BUILDMAP[el-8]=el-8
BUILDMAP[el-9]=el-9
BUILDMAP[el-10]=el-10
BUILDMAP[fc-38]=fc-38
BUILDMAP[fc-40]=fc-40
BUILDMAP[fc-rawhide]=fc-rawhide
BUILDMAP[osx]=osx
BUILDMAP[cc7_asan]=el-7-asan
BUILDMAP[cc7_tsan]=el-7-tsan
BUILDMAP[el-9-asan]=el-9-asan
BUILDMAP[el-9-tsan]=el-9-tsan
BUILDMAP[el-9-arm64]=el-9

CODENAME=$1
BUILD_TYPE=$2
PATH_PREFIX=$3

for artifacts_dir in *_artifacts; do
  build=${artifacts_dir%_*}
  repo=${BUILDMAP[${build}]}
  build_artifacts=${build}_artifacts

  # Handle only builds registered in the build map
  [ -z ${repo} ] && continue

  path="${PATH_PREFIX}/${CODENAME}/${BUILD_TYPE}/${repo}"
  tar_path="${PATH_PREFIX}/${CODENAME}/tarball/"

  # Treat OSX artifacts separately
  if [ ${build} == "osx" ]; then
    mkdir -p ${path}/x86_64/
    cp ${build_artifacts}/* ${path}/x86_64/
    continue
  fi

  # Upload RPMS
  if [ -d "${build_artifacts}/RPMS" ]; then
      if [[ -n $(find ${build_artifacts}/RPMS -name "*.aarch64.rpm") ]]; then
          mkdir -p ${path}/aarch64/
          cp ${build_artifacts}/RPMS/* ${path}/aarch64/
          createrepo --update -q ${path}/aarch64/
      elif [ "$(ls -A ${build_artifacts}/RPMS)" ]; then
          mkdir -p ${path}/x86_64/
          cp ${build_artifacts}/RPMS/* ${path}/x86_64/
          createrepo --update -q ${path}/x86_64/
      else
          echo "info: Directory ${build_artifacts}/RPMS is empty!"
      fi
  fi

  # Upload SRPMS
  if [ -d "${build_artifacts}/SRPMS" ]; then
    if [ "$(ls -A ${build_artifacts}/SRPMS)" ]; then
      mkdir -p ${path}/SRPMS/
      cp ${build_artifacts}/SRPMS/* ${path}/SRPMS/
      createrepo --update -q ${path}/SRPMS/
    else
      echo "info: Directory ${build_artifacts}/SRPMS is empty!"
    fi
  fi

  # Upload the tarball if present
  for tar_file in ${build_artifacts}/eos-*.tar.gz; do
   if [ -e ${tar_file} ]; then
     mkdir -p ${tar_path}
     cp ${tar_file} ${tar_path}
   fi
   break
  done
done

exit 0
