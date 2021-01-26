#!/usr/bin/env bash
#-------------------------------------------------------------------------------
# This script is used to copy the testing SRPM/RPM packages of a particular EOS
# release from a predefined location eg. /eos/project/s/storage-ci/www/eos/el-6/
# testing/ and move them to the stable YUM repo which don't contain the "testing"
# word in their path.
#-------------------------------------------------------------------------------
set -ex

if [ "$#" -ne 3 ]; then
  echo "Usage: $0 <branch> <path_prefix> <tag>"
  echo "e.g.: $0 citrine /eos/project/s/storage-ci/www/eos 4.3.10"
  exit -1
fi

BRANCH=$1
PATH_PREFIX=$2
TAG=$3

for arch in "el-7" "el-8" "fc-32"; do
  # Find all the srpms matching the required tag
  YUM_REPO_DIR=""
  SEARCH_PREFIX="${PATH_PREFIX}/${BRANCH}/tag/testing/${arch}"
  DEST_PREFIX="${PATH_PREFIX}/${BRANCH}/tag/${arch}"
  array=()
  while IFS=  read -r -d $'\0'; do
    array+=("$REPLY")
  done < <(find ${SEARCH_PREFIX} -type f -name "*-${TAG}-*" -name "*.src.rpm" -print0)
  echo "SRPMS:"

  for srpm_src in "${array[@]}"; do
    # Use string substitution to get the destination path
    srpm_dst="${srpm_src/$SEARCH_PREFIX/$DEST_PREFIX}"
    cp "${srpm_src}" "${srpm_dst}"
    # Drop the file name and the repodata dir from the path
    YUM_REPO_DIR=$(dirname "${srpm_dst}")
  done

  # Rebuild the repo for SRPMS
  createrepo -q ${YUM_REPO_DIR}

  # Find all the rpms matching the required tag
  array=()
  while IFS=  read -r -d $'\0'; do
    array+=("$REPLY")
  done < <(find ${SEARCH_PREFIX} -type f -name "*-${TAG}-*" ! -name "*.src.rpm" -print0)

  echo "RPMS:"
  for rpm_src in "${array[@]}"; do
    # Use string substitution to get the destination path
    rpm_dst="${rpm_src/$SEARCH_PREFIX/$DEST_PREFIX}"
    cp "${rpm_src}" "${rpm_dst}"
    # Drop the file name and the repodata dir from the path
    YUM_REPO_DIR=$(dirname "${rpm_dst}")
  done

  # Rebuild the yum repo for RPMS
  createrepo -q ${YUM_REPO_DIR}
done

# Copy also the osx package
arch="osx"
SEARCH_PREFIX="${PATH_PREFIX}/${BRANCH}/tag/testing/${arch}"
DEST_PREFIX="${PATH_PREFIX}/${BRANCH}/tag/${arch}"
array=()
while IFS=  read -r -d $'\0'; do
  array+=("$REPLY")
done < <(find ${SEARCH_PREFIX} -type f -name "*-${TAG}.dmg" -print0)

dmg="${array[0]}"

cp "${dmg}" "${dmg/$SEARCH_PREFIX/$DEST_PREFIX}"
