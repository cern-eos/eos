[![build status](https://gitlab.cern.ch/dss/eos/badges/master/build.svg)](https://gitlab.cern.ch/dss/eos/commits/master)

# EOS

## Description

**EOS** is a software solution that aims to provide fast and reliable multi-PB
disk-only storage technology for both LHC and non-LHC use-cases at CERN. The
core of the implementation is the XRootD framework which provides feature-rich
remote access protocol. The storage system is running on commodity hardware
with disks in JBOD configuration. It is written mostly in C/C++, with some of
the extra modules in Python. Files can be accessed via native **XRootD**
protocol, a **POSIX-like FUSE** client or **HTTP(S) & WebDav** protocol.

## Documentation

The most up to date documentation can be found at:
http://eos-docs.web.cern.ch/eos-docs/

You will need to install some packages in order to generate them:

```bash
## RHEL instructions
# TODO

## Ubuntu 20.04 instructions
sudo apt install python3-sphinx doxygen
pip3 install solar_theme
```

**Doxygen** documentation of the API is available in the ``./doc`` directory
 and can be generated using the following command:

```bash
# Inside the EOS git clone directory
cd doc
doxygen
....
# Documentation generated in the ./html directory which can be accessed using any browser
# file:///eos_git_clone_dir/doc/html/index.html
```

**Sphinx** documentation of installation and application is also in the ``./doc'' directory.
This is what is published in https://eos-docs.web.cern.ch .
Documentation can be generated using the following command:
```bash
cd doc
make html
# Documentation can be found in build/html/index.html which can be viewed in a browser
# The make interface can output types as well.

```

## Project Directory Structure

* archive - Archive tool implementation in Python
* auth_plugin - Authorization delegation plugin
* authz - Authorization capability functionality
* client - GRPC clients
* cmake - CMake related scripts and functions
* common - Common helper files and classes
* console - Command line client implementation
* coverage - Test coverage config for LCOV
* doc - Doxygen documentation
* etc - Log rotation files
* fst - The Storage Server Plugin (FST)
* fuse - The FUSE mount Client (eosd low level API)
* fusex- Next generation bi-directional FUSE mount Client with high-end features
* man - Manual pages
* mgm - Meta Data Namespace and Scheduling Redirector Plugin (MGM)
* misc - systemd, sysconfig and service scripts
* mq - Message Queue Server Plugin
* namespace - Namespace Implementation
* nginx - Nginx related patches for EOS integration
* proto - Protobuf definitions for various components
* srm - SRM BestMan utility scripts
* sync - file/directory synchronization programs
* test - Instance test script with some dedicated test executables
* unit_tests - Various unit tests for individual modules
* utils - Useful utilities and the uninstall scripts
* var - Placeholder directory to create log, http and namespace directories

## Dependencies

It is recommended to set up xrootd and eos-citrine-dependency repositories,
as seen in the [documentation][1].

```bash
yum install -y git gcc cmake cmake3 readline readline-devel fuse fuse-devel \
leveldb leveldb-devel binutils-devel zlib zlib-devel zlib-static \
bzip2 bzip2-devel libattr libattr-devel libuuid libuuid-devel \
xfsprogs xfsprogs-devel sparsehash-devel e2fsprogs e2fsprogs-devel \
libmicrohttpd libmicrohttpd-devel openssl openssl-devel openssl-static \
eos-folly eos-rocksdb ncurses ncurses-devel ncurses-static protobuf3-devel \
cppunit-devel openldap-devel hiredis-devel zeromq-devel jsoncpp-devel \
xrootd xrootd-server-devel xrootd-client-devel xrootd-private-devel \
cppzmq-devel libcurl-devel libevent-devel jemalloc jemalloc-devel
```

## Build

To build **EOS**, you need **gcc (>=4.9)** with **C++14 features** and **CMake** 
installed on your system:

```bash
git submodule update --init --recursive
# Create build workdir
mkdir build
cd build
# Run CMake
cmake3 ..
# Build
make -j 4
```

## Install/Uninstall

The default behaviour is to install **EOS** at system level using `CMAKE_INSTALL_PREFIX=/usr`.
To change the default install prefix path, do the following:

```bash
# Modify the default install path
cmake ../ -DCMAKE_INSTALL_PREFIX=/other_path
# Install - might require sudo privileges
make install
# Uninstall
make uninstall
```

## Source/Binary RPM Generation

To build the source/binary RPMs run:

```bash
# Create source tarball
make dist
# Create Source RPM
make srpm
# Create RPM
make rpm
```

## Bug Reporting

You can send **EOS** bug reports to <project-eos@cern.ch>. 
The preferable way, if you have access, is use the online bug tracking 
system [Jira][2] to submit new problem reports or search for existing ones: 
https://its.cern.ch/jira/browse/EOS

## Licence

**EOS - The CERN Disk Storage System**  
**Copyright (C) 2019 CERN/Switzerland**  
This program is free software: you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation, either version 3 of the License, or (at your option) any later
version. This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY, without even the implied warranty of MERCHANTABILITY
or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

[1]: http://eos-docs.web.cern.ch/eos-docs/quickstart/setup_repo.html#eos-base-setup-repos
[2]: https://its.cern.ch/jira/secure/Dashboard.jspa
