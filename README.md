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

The most up-to-date documentation can be found at:
[eos-docs.web.cern.ch/eos-docs](http://eos-docs.web.cern.ch/eos-docs/)

You will need to install Sphinx, Doxygen and the `solar_theme` (for Sphinx) in order to
generate the docs. For up-to-date information on getting Sphinx refer to the
[Sphinx docs](https://www.sphinx-doc.org/en/master/usage/installation.html).

```bash
## RHEL instructions
# Please choose the relevant python version based on the distro
sudo yum install python-sphinx doxygen
pip install solar_theme

## Ubuntu 20.04 instructions
sudo apt install python3-sphinx doxygen
pip3 install solar_theme
```

**Doxygen** documentation of the API is available in the `./doc` directory
and can be generated using the following command:

```bash
# Inside the EOS git clone directory
cd doc
doxygen
....
# Documentation generated in the ./html directory, viewable with any browser:
#   file:///eos_git_clone_dir/doc/html/index.html
```

**Sphinx** documentation of installation and application is also in the `./doc` directory.
This is what is published at https://eos-docs.web.cern.ch.
Documentation can be generated using:
```bash
cd doc
make html
# Documentation can be found in build/html/index.html (view in a browser).
# The make interface supports other targets (e.g. latexpdf).
```

## Project directory structure

- `archive/`: Archive tool implementation in Python
- `auth_plugin/`: Authorization delegation plugin
- `authz/`: Authorization capability functionality
- `client/`: gRPC clients
- `cmake/`: CMake scripts and functions
- `common/`: Common helper files and classes
- `console/`: Command line client implementation
- `coverage/`: Test coverage config for LCOV
- `doc/`: Doxygen and Sphinx documentation
- `etc/`: Log rotation files
- `fst/`: The Storage Server Plugin (FST)
- `fusex/`: Next generation bi-directional FUSE mount client with high-end features
- `man/`: Manual pages
- `mgm/`: Metadata Namespace and Scheduling Redirector Plugin (MGM)
- `misc/`: systemd, sysconfig and service scripts
- `mq/`: Message Queue server plugin
- `namespace/`: Namespace implementation
- `nginx/`: Nginx patches for EOS integration
- `proto/`: Protobuf definitions for various components
- `test/`: Instance test scripts and dedicated test executables
- `unit_tests/`: Unit tests for individual modules
- `utils/`: Utilities and uninstall scripts

## Git submodules

Some components are maintained in separate upstream repositories and brought in as git submodules. Make sure submodules are initialized and kept up-to-date:

```bash
git submodule update --init --recursive
# To refresh later
git submodule update --recursive --remote
```

Submodules currently used:
- `quarkdb/`: QuarkDB client/server sources used by MGM for QuarkDB-backed services (e.g., QDB master, metadata/services that rely on QuarkDB).
- `common/xrootd-ssi-protobuf-interface/`: XRootD SSI + Protobuf interface headers used by EOS gRPC/SSI integrations and CTA-related workflows.

Tip: See `.gitmodules` for the authoritative list and remote URLs.

## Dependencies

Use the EOS Diopside dependency repository.
Follow the official installation instructions here:
[EOS Diopside Manual – Installation](https://eos-docs.web.cern.ch/diopside/manual/hardware-installation.html#installation).

```bash
yum install -y git gcc cmake cmake3 readline readline-devel fuse fuse-devel \
leveldb leveldb-devel binutils-devel zlib zlib-devel zlib-static \
bzip2 bzip2-devel libattr libattr-devel libuuid libuuid-devel \
xfsprogs xfsprogs-devel sparsehash-devel e2fsprogs e2fsprogs-devel \
openssl openssl-devel openssl-static eos-folly eos-rocksdb ncurses \
ncurses-devel ncurses-static protobuf3-devel openldap-devel \
hiredis-devel zeromq-devel jsoncpp-devel xrootd xrootd-server-devel \
xrootd-client-devel xrootd-private-devel cppzmq-devel libcurl-devel \
libevent-devel jemalloc jemalloc-devel
```
## Build

To build **EOS**, you need **gcc (>=7)** with **C++17 features** and **CMake**
installed on your system. If you can install ninja, **EOS** supports ninja for builds.

```bash
git submodule update --init --recursive
# Create build workdir
mkdir build-with-ninja
cd build-with-ninja
# Run CMake (pass -DCLIENT=1 if you only need the client binaries)
cmake3 -GNinja ..
# Build
ninja -j 4
```

Otherwise, standard Makefile builds are of course possible:

```bash
git submodule update --init --recursive
# Create build workdir
mkdir build
cd build
# Run CMake (pass -DCLIENT=1 if you only need the client binaries)
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
# if using ninja
ninja install
# Uninstall
ninja uninstall

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

## EOS Community

For discussions and help, there is also the eos community which brings together
users, developers & collaborators at https://eos-community.web.cern.ch/

## Licence

**EOS - The CERN Disk Storage System**  
**Copyright (C) 2025 CERN/Switzerland**
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
