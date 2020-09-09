yum install -y cmake3
yum --disablerepo=epel install -y xrootd-server-devel xrootd-private-devel xrootd-client-devel
yum install -y sparsehash-devel
yum install -y ncurses ncurses-devel ncurses-static openssl openssl-devel openssl-static
yum install -y readline readline-devel
yum install -y libuuid libuuid-devel
yum install -y libcurl libcurl-devel
yum install -y zeromq zeromq-devel
yum install -y protobuf protobuf-devel leveldb leveldb-devel
yum install -y cppunit cppunit-devel
yum install -y fuse-libs fuse fuse-devel
yum install -y libattr libattr-devel
yum install -y openldap openldap-devel
yum install -y libmicrohttp libmicrohttpd-devel
yum install -y zlib zlib-devel zlib-static
yum install -y xfsprogs xfsprogs-devel
yum install -y e2fsprogs-devel
yum install -y perl-Time-HiRes
yum install -y json-c json-c-devel
yum install -y jsoncpp jsoncpp-devel
yum install -y libevent libevent-devel
yum install -y git
( cd /tmp/; git clone  https://github.com/zeromq/cppzmq; cp cppzmq/zmq.hpp /usr/include/ )
( cd /etc/yum.repos.d/;  wget http://linuxsoft.cern.ch/cern/devtoolset/slc6-devtoolset.repo )
yum makecache
# get gcc 4.8.X
yum install -y devtoolset-2-gcc-c++
yum install -y devtoolset-2-binutils-devel
# enable gcc 4.8.X
scl enable devtoolset-2 bash

