#!/bin/bash

#-------------------------------------------------------------------------------
# Publish debian artifacts on CERN Gitlab CI
# Author: Jozsef Makai <jmakai@cern.ch> (11.08.2017)
#-------------------------------------------------------------------------------

set -e

script_loc=$(dirname "$0")

eos_base=/eos/project/s/storage-ci/www/debian/eos
versions=$(find $eos_base -mindepth 1 -maxdepth 1 -type d -exec basename {} \;)

for version in ${versions}; do
  dists=$(find $eos_base/$version/pool -mindepth 1 -maxdepth 1 -type d -exec basename {} \;)
  for dist in ${dists}; do
    if [ -d "${eos_base}/${version}/pool/${dist}/commit" ]; then
      find $eos_base/$version/pool/$dist/commit -type f -mtime +10 -name '*.deb' -delete
      for arch in amd64; do
        $script_loc/generate_debian_metadata.sh $eos_base/$version $dist commit $arch
      done
      if [[ -n "$CI_COMMIT_TAG" ]]; then $script_loc/sign_debian_repository.sh $eos_base/$version $dist; fi
    fi
  done
done 
