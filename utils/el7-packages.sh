yum install --disablerepo=epel -y xrootd-server-devel xrootd-private-devel \
    xrootd-client xrootd-client-devel

yum install -y centos-release-scl
yum install -y cmake3 sparsehash-devel \
    ncurses ncurses-devel ncurses-static openssl openssl-devel openssl-static \
    readline readline-devel libuuid libuuid-devel zeromq3 zeromq3-devel \
    eos-protobuf3 eos-protobuf3-devel eos-protobuf3-compiler \
    eos-protobuf3-debuginfo leveldb leveldb-devel cppunit cppunit-devel \
    fuse-libs fuse fuse-devel libattr libattr-devel openldap openldap-devel \
    libmicrohttpd libmicrohttpd-devel zlib zlib-devel zlib-static \
    xfsprogs xfsprogs-devel e2fsprogs-devel perl-Time-HiRes json-c json-c-devel \
    jsoncpp jsoncpp-devel libcurl libcurl-devel hiredis hiredis-devel \
    libevent libevent-devel bzip2-devel bzip2-libs jemalloc jemalloc-devel \
    eos-rocksdb devtoolset-8 gtest binutils-devel eos-folly

source /opt/rh/devtoolset-6/enable
