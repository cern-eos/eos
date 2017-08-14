#!/bin/bash

#-------------------------------------------------------------------------------
# Publish debian artifacts on CERN Gitlab CI
# Author: Jozsef Makai <jmakai@cern.ch> (11.08.2017)
#-------------------------------------------------------------------------------

set -e

prefix=$1
dist=$2
comp=$3
arch=$4

mkdir -p $prefix/dists/$dist/$comp/binary-$arch/
(cd $prefix && apt-ftparchive --arch $arch packages pool/$dist/$comp/ > dists/$dist/$comp/binary-$arch/Packages)
gzip -c $prefix/dists/$dist/$comp/binary-$arch/Packages > $prefix/dists/$dist/$comp/binary-$arch/Packages.gz
