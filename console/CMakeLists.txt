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
		     ${CMAKE_BINARY_DIR}/xrootd/src/include/xrootd
		     ${CMAKE_BINARY_DIR}/xrootd/src/include/
		     ${XROOTD_INCLUDE_DIR} 
		     ${SPARSEHASH_INCLUDE_DIR} )

link_directories( ${CMAKE_INSTALL_FULL_LIBDIR}
		  ${XROOTD_LIB_DIR} )


add_executable( eos
	      	ConsoleMain.cc
		ConsoleMain.hh	
		ConsolePipe.cc
		ConsolePipe.hh
#                ../common/Logging.cc
#                ../common/Fmd.cc
#                ../common/SymKeys.cc
#		../common/Attr.cc
		../fst/FmdSqlite.cc
		../fst/checksum/CheckSum.cc
		../fst/checksum/Adler.cc
		../checksum/crc32c.cc
                ../checksum/crc32ctables.cc
		commands/com_access.cc
	        commands/com_attr.cc
		commands/com_cd.cc
		commands/com_chmod.cc 
		commands/com_chown.cc 
		commands/com_clear.cc         
		commands/com_config.cc 
		commands/com_console.cc 
		commands/com_cp.cc 
		commands/com_debug.cc 
		commands/com_dropbox.cc 
		commands/com_file.cc 
		commands/com_find.cc 
		commands/com_fs.cc 
		commands/com_fsck.cc
                commands/com_fuse.cc
		commands/com_group.cc
                commands/com_io.cc
                commands/com_json.cc
		commands/com_license.cc 
		commands/com_ls.cc 
		commands/com_map.cc 
		commands/com_mkdir.cc 
		commands/com_motd.cc 
		commands/com_node.cc
		commands/com_ns.cc    
		commands/com_print.cc 
		commands/com_pwd.cc 
		commands/com_quit.cc 
		commands/com_quota.cc 
		commands/com_reconnect.cc 
		commands/com_restart.cc 
		commands/com_rm.cc 
		commands/com_rmdir.cc 
		commands/com_role.cc 
		commands/com_rtlog.cc 
		commands/com_silent.cc 
		commands/com_space.cc
		commands/com_stat.cc
		commands/com_test.cc 
		commands/com_timing.cc
		commands/com_transfer.cc 
		commands/com_verify.cc 
		commands/com_version.cc 
		commands/com_vid.cc 
		commands/com_whoami.cc   
                commands/com_who.cc 
)


add_executable( eosdropboxd
	        dropbox/eosdropboxd.cc 
)



if (CLIENT)
target_link_libraries( eos 
		       XrdPosix	        XrdCl
		       XrdUtils  	eosCommon-Static 
		       pthread 	        dl
		       crypto 		${UUID_LIBRARIES} 		
		       curses 		z    		
		       readline 
)
else (CLIENT)
target_link_libraries( eos 
		       XrdPosix	        XrdCl 
		       XrdUtils 	eosCommon-Static 
		       readline         XrdMqClient-Static
		       pthread 	        crypto 
		       ${UUID_LIBRARIES} curses 
		       z 		dl 		       
)
endif(CLIENT)

target_link_libraries( eosdropboxd eosCommon-Static XrdPosix XrdCl XrdUtils pthread )

INSTALL_PROGRAMS( ${CMAKE_INSTALL_SBINDIR} FILES eosadmin )

install( TARGETS eos eosdropboxd
       	 LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
         RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
         ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
)
