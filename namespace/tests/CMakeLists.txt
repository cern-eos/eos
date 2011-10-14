# ----------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Andreas-Joachim Peters - CERN
# ----------------------------------------------------------------------

# ************************************************************************
# * EOS - the CERN Disk Storage System                                   *
# * Copyright (C) 2011 CERN/Switzerland                                  *
# *                                                                      *
# * This program is free software: you can redistribute it and/or modify *
# * it under the terms of the GNU General Public License as published by *
# * the Free Software Foundation, either version 3 of the License, or    *
# * (at your option) any later version.                                  *
# *                                                                      *
# * This program is distributed in the hope that it will be useful,      *
# * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
# * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
# * GNU General Public License for more details.                         *
# *                                                                      *
# * You should have received a copy of the GNU General Public License    *
# * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
# ************************************************************************

include_directories( ${CPPUNIT_INCLUDE_DIRS} )

add_library (eosNamespaceTests SHARED
      ChangeLogContainerMDSvcTest.cc
      ChangeLogFileMDSvcTest.cc
      ChangeLogTest.cc
      FileSystemViewTest.cc
      HierarchicalViewTest.cc
      LogCompactingTest.cc
      OtherTests.cc
      TestHelpers.hh
      TestHelpers.cc
)

target_link_libraries( eosNamespaceTests ${CPPUNIT_LIBRARIES} eosNamespace)

add_executable(text-runner TextRunner.cc)
target_link_libraries( text-runner eosNamespaceTests dl)


if (Linux)
  set_target_properties( eosNamespaceTests PROPERTIES COMPILE_FLAGS -fPIC )
endif (Linux)
