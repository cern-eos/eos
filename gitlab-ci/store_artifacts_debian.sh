#!/bin/bash

#-------------------------------------------------------------------------------
# Publish debian artifacts on CERN Gitlab CI
# Author: Jozsef Makai <jmakai@cern.ch> (11.08.2017)
#-------------------------------------------------------------------------------

set -e

script_loc=$(dirname "$0")
prefix=$1
component=$2

for artifacts_dir in *_artifacts; do
  dist=${artifacts_dir%_*}
  path=$prefix/pool/$dist/$component/e/eos/
  mkdir -p $path

  echo "Publishing for $dist -- $path"

  cp ${dist}_artifacts/*.deb $path
  $script_loc/generate_debian_metadata.sh $prefix $dist $component amd64
  if [[ -n "$CI_COMMIT_TAG" ]]; then $script_loc/sign_debian_repository.sh $prefix $dist; fi
done