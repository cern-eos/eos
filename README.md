EOS
==============

************************************************************************
* EOS - the CERN Disk Storage System                                   *
* Copyright (C) 2011 CERN/Switzerland                                  *
*                                                                      *
* This program is free software: you can redistribute it and/or modify *
* it under the terms of the GNU General Public License as published by *
* the Free Software Foundation, either version 3 of the License, or    *
* (at your option) any later version.                                  *
*                                                                      *
* This program is distributed in the hope that it will be useful,      *
* but WITHOUT ANY WARRANTY; without even the implied warranty of       *
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
* GNU General Public License for more details.                         *
*                                                                      *
* You should have received a copy of the GNU General Public License    *
* along with this program.  If not, see <http://www.gnu.org/licenses/>.*
************************************************************************



Project Directory Structure
---------------------------
authz     -	Class for Capability Authorization
checksum  -	CRC32C MIT/Intel Code
common    -	Common Helper Files and Classes
console   -	Command Line Client Implementation
etc       -	Service scripts, Logrotation and Sysconfig Files
fst       -	The Storage Server Plugin (FST)
fuse      -	The FUSE mount Client (eosd low level - eosfsd high level)
man       -	Manual Pages
mgm       -	The Meta Data Namespace+Scheduling Redirector Plugin (MGM)
mq        -	The Message Queue Server Plugin
namespace -	Implementation of the Namespace
srm       -	SRM BestMan utility scripts
sync      -	EOSHA high availability daemon + file/directory synchronization programs
test      -	Instance test script with some dedicated test executables
utils     -	Usefull utilities and the uninstall scripts
var       -	Placeholder directory to create log, http and namespace directories
##########################################################################

Compilation (gcc >= 4.4)
-------------------------
mkdir build
cd build
cmake ../ [-DRELEASE=<release#>]

make -j 4 install

Dependencies
------------
- google sparse hash
- readline (devel)
- ncurses (devel + static)
- openssl (devel + static)
- libattr (devel)
- fuse (devel)
- libuuid (devel) | e2fsprogs (devel)
- libz (+static)
- git
- cmake (2.8)
- xrootd client+server+dev packages (3.3.X)

Install/Uninstall
-----------------
make install
make uninstall

Source Tarball Generation                                            
-------------------------
make dist    # packing local files
make gitdist # using git archive


Binary RPM Generation
---------------------
# cmake .. -DRELEASE=<release> must match the release number in ../eos.spec !
make rpm


Source RPM Generation 
---------------------
# cmake .. -DRELEASE=<release> must match the release number in ../eos.spec !
make srpm

