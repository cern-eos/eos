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

include_directories( ../ ${XROOTD_INCLUDE_DIR} ${HTTPD_INCLUDE_DIR}/httpd ${APR_INCLUDE_DIRS}/apr-1)
link_directories( /usr/${LIB_INSTALL_DIR}/ ${XROOTD_LIB_DIR})

add_library (mod_authz_key MODULE
	    mod_authz_key.c
	    )

if (Linux)
  set_target_properties (mod_authz_key PROPERTIES
    VERSION ${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}
    SOVERSION ${VERSION_MAJOR}
    CLEAN_DIRECT_OUTPUT 1
    COMPILE_FLAGS -fPIC
  )
endif(Linux)

install ( TARGETS mod_authz_key
          LIBRARY DESTINATION ${LIB_INSTALL_DIR}/httpd/modules/
          RUNTIME DESTINATION bin
          ARCHIVE DESTINATION ${LIB_INSTALL_DIR}/httpd/modules/
)

INSTALL ( FILES eos-httpd-redirector.cgi eos-httpd-redirector-suexec.cgi DESTINATION /var/www/eos/cgi-bin/ )
#INSTALL ( DIRECTORY browser DESTINATION /var/www/eos )
