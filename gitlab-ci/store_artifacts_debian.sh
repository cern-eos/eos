#!/bin/bash

#-------------------------------------------------------------------------------
# Publish debian artifacts on CERN Gitlab CI
# Author: Jozsef Makai <jmakai@cern.ch> (11.08.2017)
#-------------------------------------------------------------------------------

set -e

script_loc=$(dirname "$0")
prefix=$1
comp=$2

for dist in artful; do
  echo "Publishing for $dist"
  path=$prefix/pool/$dist/$comp/e/eos/
  mkdir -p $path
  cp $dist/*.deb $path
  $script_loc/generate_debian_metadata.sh $prefix $dist $comp amd64
  $script_loc/sign_debian_repository.sh $prefix $dist
done
