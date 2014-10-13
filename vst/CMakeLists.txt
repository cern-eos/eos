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

include_directories( ../ 
                     ./
                     ${XROOTD_INCLUDE_DIR} 
                     ${XROOTD_PRIVATE_INCLUDE_DIR}
                     ${SPARSEHASH_INCLUDE_DIR}
                     ${NCURSES_INCLUDE_DIR}
		     ${Z_INCLUDE_DIR}
)

link_directories( ${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR}
                  ${XROOTD_LIB_DIR} )

add_library( XrdEosVst MODULE
	     XrdVstOfs.cc                   XrdVstOfs.hh
        XrdVstOfsFile.cc
        XrdVstOfsDirectory.cc
)


target_link_libraries( XrdEosVst 
		       eosCommon
		       eosCommonServer      
		       XrdMqClient-Static      XrdCl	
		       XrdUtils                XrdOfs 
		       crypto                  ${NCURSES_LIBRARIES}
		       dl                      ${Z_LIBRARIES}  
		       ${UUID_LIBRARIES}       ${ATTR_LIBRARIES}       
		       ${RT_LIBRARIES}         ${CMAKE_THREAD_LIBS_INIT}
)


if (Linux)
  set_target_properties (XrdEosVst PROPERTIES
    VERSION ${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}
    SOVERSION ${VERSION_MAJOR}
    CLEAN_DIRECT_OUTPUT 1
  )
endif(Linux)


install( TARGETS XrdEosVst
       	 LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
         RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
         ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
)
