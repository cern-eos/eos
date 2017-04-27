#!/bin/bash
# Build XRootD
if [ $# -ne 1 ]; then
  echo "Usage: $0 xrootd_version"
  exit 1
fi

XROOTD_VERSION=$1
mkdir xrootd_src
git clone https://github.com/xrootd/xrootd.git xrootd_src
cd xrootd_src
git checkout $XROOTD_VERSION
mkdir build; mkdir build_install
cd build
cmake .. -DCMAKE_INSTALL_PREFIX=../build_install/ -DOPENSSL_ROOT_DIR=/usr/local/opt/openssl/
make
make install
