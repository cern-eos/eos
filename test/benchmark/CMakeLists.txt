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
		    ${XROOTD_INCLUDE_DIR} 
		    ${XROOTD_PRIVATE_INCLUDE_DIR}
            ${UUID_INCLUDE_DIRS})

link_directories(${PROTOBUF_LIBRARIES}
		 ${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR}
		 ${XROOTD_LIB_DIR}
		 ${UUID_LIBRARIES}) 

set_source_generating_rules()

ADD_EXECUTABLE(eos-benchmark 
   		       eosbenchmark.cc     eosbenchmark.hh
       	       Configuration.cc    Configuration.hh
		       DirEos.cc           DirEos.hh
       	       FileEos.cc          FileEos.hh
       	       Result.cc           Result.hh
       	       ProtoIo.cc          ProtoIo.hh
       	       ../../common/StringConversion.cc
       	       ../../common/StringConversion.hh
		       ../../fst/io/FileIoPlugin.cc		       
       	       ConfigProto.pb.cc      ConfigProto.pb.h
       	       ResultProto.pb.cc      ResultProto.pb.h) 

TARGET_LINK_LIBRARIES(eos-benchmark
             	      XrdCl 
	      	   	      EosFstIo-Static
 		     	      ${PROTOBUF_LIBRARIES} 
	      	   	      ${UUID_LIBRARIES} 
	      	   	      ${CMAKE_THREAD_LIBS_INIT})

INSTALL_PROGRAMS( ${CMAKE_INSTALL_SBINDIR}
                  FILES 
                  eos-benchmark)



	       


