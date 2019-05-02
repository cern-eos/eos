#!/bin/bash
set -e
VERSION=$1
curl http://xrootd.org/download/v$VERSION/xrootd-$VERSION.tar.gz -o xrootd-$VERSION.tar.gz
tar xvzf *.tar.gz
cd xrootd-$VERSION
mkdir build
cd build
cmake3 ../ -DCMAKE_INSTALL_PREFIX=/opt/eos/xrootd/ -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_SKIP_BUILD_RPATH=false -DCMAKE_BUILD_WITH_INSTALL_RPATH=false -DCMAKE_INSTALL_RPATH=/opt/eos/xrootd/lib64/ -DCMAKE_INSTALL_RPATH_USE_LINK_PATH=true
make -j 4 install
cd ../
rm -rf ./xrootd-$VERSION*

