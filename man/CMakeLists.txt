# ----------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Andreas-Joachim Peters - CERN
# ----------------------------------------------------------------------

# ************************************************************************
# * EOS - the CERN Disk Storage System                                   *
# * Copyright (C) 2018 CERN/Switzerland                                  *
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

add_custom_target(
  man ALL
  DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/man1/)

add_dependencies(man eos)

add_custom_command(
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/man1/
  COMMAND ${CMAKE_COMMAND} -E env "LD_LIBRARY_PATH=${CMAKE_BINARY_DIR}/common:${CMAKE_BINARY_DIR}/mq:$ENV{LD_LIBRARY_PATH}"
                                  "PATH=${CMAKE_BINARY_DIR}/console:$ENV{PATH}"
          ${CMAKE_CURRENT_SOURCE_DIR}/create_man.sh
)

install(
  CODE "FILE ( GLOB MAN_FILES \"${CMAKE_CURRENT_BINARY_DIR}/man1/*.1.gz\" )"
  CODE "FILE ( INSTALL \${MAN_FILES}
               DESTINATION \"${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_MANDIR}/man1\"
               PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ )"
)
