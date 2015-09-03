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
http://eos.readthedocs.org/en/latest/


**Doxygent** documentaion of the API is available in the ``./doc`` directory
 and can be generated using the following command:

```bash
# Inside the EOS git clone directory
cd doc
doxygen
....
# Documentation generated in the ./html directory which can be accessed using any browser
# file:///eos_git_clone_dir/doc/html/index.html
```

## Project Directory Structure

* archive - Archive tool implementation in Python
* auth_plugin - Authorization delegation plugin
* authz - Authorization capability functionality
* cmake - CMake related scripts and functions
* common - Common helper files and classes
* console - Command line client implementation
* doc - Doxygen documentation
* etc - Service scripts, logrotation and sysconfig Files
* fst - The Storage Server Plugin (FST)
* fuse - The FUSE mount Client (eosd - low level, eosfsd - high level)
* man - Manual pages
* mgm - Meta Data Namespace and Scheduling Redirector Plugin (MGM)
* mq - Message Queue Server Plugin
* namespace - Namespace Implementation
* nginx - Nginx related patches for EOS integration
* srm - SRM BestMan utility scripts
* sync - EOSHA high availability daemon + file/directory synchronization programs
* test - Instance test script with some dedicated test executables
* utils - Usefull utilities and the uninstall scripts
* var - Placeholder directory to create log, http and namespace directories

## Dependencies

* git
* gcc (>= 4.4)
* cmake (>=2.8)
* readline, readline-devel
* fuse, fuse-devel
* leveldb, leveldb-devel
* zlib, zlib-devel
* libattr, libattr-devel
* libuuid, libuuid-devel
* xfsprogs, xfsprogs-devel
* sparsehash, sparsehash-devel
* e2fsprogs, e2fsprogs-devel
* libmicrohttpd, libmicrohttpd-devel
* openssl, openssl-devel, openssl-static
* ncurses, ncurses-devel, ncurses-static
* xrootd, xrootd-server-devel, xrootd-client-devel, xrootd-private-devel (>=3.3.6)

## Build

To build **EOS**, you need **gcc (>=4.4)** and **CMake** installed on your system:
```bash
# Create build workdir
mkdir build
cd build
# Run CMake
cmake ..
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
# Create RPMS
make rpm
```

## System specific notes

* The current production version of **EOS** in the **beryl_aquamarine** branch
which has a hard requirement on **XRootD 3.3.6**.
* The the future production version of **EOS** is the **citrine** branch and
 requires **XRootD >= 4.2.0**.

## Bug Reporting

You can send **EOS** bug reports to <project-eos@cern.ch>. The preferable way,
if you have access, is use the online bug tracking system
[Jira ](https://its.cern.ch/jira/secure/Dashboard.jspa) to submit new problem
 reports or search for existing ones: https://its.cern.ch/jira/browse/EOS

## Licence

**EOS - The CERN Disk Storage System**  
**Copyright (C) 2015 CERN/Switzerland**  
This program is free software: you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation, either version 3 of the License, or (at your option) any later
version. This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY, without even the implied warranty of MERCHANTABILITY
or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
