#!/bin/bash

#-------------------------------------------------------------------------------
# Publish debian artifacts on CERN Gitlab CI
# Author: Jozsef Makai <jmakai@cern.ch> (11.08.2017)
#-------------------------------------------------------------------------------

set -e

prefix=$1
dist=$2
component=$3
arch=$4

mkdir -p $prefix/dists/$dist/$component/binary-$arch/
(cd $prefix && apt-ftparchive --arch $arch packages pool/$dist/$component/ > dists/$dist/$component/binary-$arch/Packages)
gzip -c $prefix/dists/$dist/$component/binary-$arch/Packages > $prefix/dists/$dist/$component/binary-$arch/Packages.gz
