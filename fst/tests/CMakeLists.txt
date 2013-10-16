# ----------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Elvin-Alin Sindrilaru <esindril@cern.ch> CERN
# ----------------------------------------------------------------------

# ************************************************************************
# * EOS - the CERN Disk Storage System                                   *
# * Copyright (C) 2013 CERN/Switzerland                                  *
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

include_directories(./
		    ${eos_SOURCE_DIR}
		    ${XROOTD_INCLUDE_DIR}
		    ${CPPUNIT_INCLUDE_DIRS})

link_directories(${CPPUNIT_LIBRARY}
		 ${XROOTD_LIB_DIR})

add_library(EosFstTests MODULE
	    FileTest.cc  FileTest.hh
	    TestEnv.cc   TestEnv.hh)

target_link_libraries(EosFstTests 
		      XrdCl   
		      XrdUtils
		      eosCommon	      
		      EosFstIo-Static
	              ${CPPUNIT_LIBRARIES})

if (Linux)
  set_target_properties(EosFstTests PROPERTIES
                        VERSION ${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}
			SOVERSION ${VERSION_MAJOR}
                        CLEAN_DIRECT_OUTPUT 1)
endif(Linux)

install(TARGETS EosFstTests
        LIBRARY DESTINATION ${LIB_INSTALL_DIR}
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
        ARCHIVE DESTINATION ${LIB_INSTALL_DIR})

