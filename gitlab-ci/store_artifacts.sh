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
BUILDMAP[c8]=el-8
BUILDMAP[fc-32]=fc-32
BUILDMAP[fc-rawhide]=fc-rawhide
BUILDMAP[osx]=osx

BRANCH=$1
BUILD_TYPE=$2
PATH_PREFIX=$3

for artifacts_dir in *_artifacts; do
  build=${artifacts_dir%_*}
  repo=${BUILDMAP[${build}]}

  # Handle only builds registered in the build map
  [ -z ${repo} ] && continue

  path=${PATH_PREFIX}/${BRANCH}/${BUILD_TYPE}/${repo}

  # Treat OSX artifacts separately
  if [ ${build} == "osx" ]; then
    mkdir -p ${path}/x86_64/
    cp ${build}_artifacts/* ${path}/x86_64/
    continue
  fi

  # Upload RPMS
  mkdir -p ${path}/x86_64/
  cp ${build}_artifacts/RPMS/* ${path}/x86_64/
  createrepo --update -q ${path}/x86_64/

  # Upload SRPMS
  mkdir -p ${path}/SRPMS/
  cp ${build}_artifacts/SRPMS/* ${path}/SRPMS/
  createrepo --update -q ${path}/SRPMS/

  # Upload the tarball if present
  for tar_file in ${build}_artifacts/eos-*.tar.gz; do
   if [ -e ${tar_file} ]; then
     tar_path="${PATH_PREFIX}/${BRANCH}/tarball/"
     mkdir -p ${tar_path}
     cp ${tar_file} ${tar_path}
   fi
   break
  done
done

exit 0
