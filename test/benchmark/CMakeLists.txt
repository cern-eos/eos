# ----------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Elvin-Alin Sindrilaru - CERN
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

include_directories(./ 
		    ../../   
                    ${PROTOBUF_INCLUDE_DIRS}
		    ${CMAKE_CURRENT_BINARY_DIR}
		    ${XROOTD_INCLUDE_DIR} 
		    ${XROOTD_PRIVATE_INCLUDE_DIR}
                    ${UUID_INCLUDE_DIRS})

link_directories(${PROTOBUF_LIBRARIES}
		 ${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR}
		 ${XROOTD_LIB_DIR}
		 ${UUID_LIBRARIES}) 

IF(PROTBUF_FOUND)
	PROTOBUF_GENERATE_CPP(CONFIG_SRCS CONFIG_HDRS PROTOFILES ConfigProto.proto)
	PROTOBUF_GENERATE_CPP(RESULT_SRCS RESULT_HDRS PROTOFILES ResultProto.proto)

	add_executable(eosbenchmark 
		       eosbenchmark.cc     eosbenchmark.hh
	      	       Configuration.cc    Configuration.hh
	       	       DirEos.cc           DirEos.hh
               	       FileEos.cc          FileEos.hh
	       	       Result.cc           Result.hh
	       	       ProtoIo.cc          ProtoIo.hh
	       	       ../../common/StringConversion.cc
               	       ../../common/StringConversion.hh
	       	       ${CONFIG_SRCS}      ${CONFIG_HDRS}
	       	       ${RESULT_SRCS}      ${RESULT_HDR}) 

        target_link_libraries(eosbenchmark
			      XrdCl 
		      	      XrdEosFst-Static
 			      ${PROTOBUF_LIBRARIES} 
		      	      ${UUID_LIBRARIES} 
		      	      ${CMAKE_THREAD_LIBS_INIT})
ENDIF()



	       


