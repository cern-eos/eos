include_directories( ../ ${XROOTD_INCLUDE_DIR} ${SPARSEHASH_INCLUDE_DIR} )
link_directories( /usr/${LIB_INSTALL_DIR}/ ${XROOTD_LIB_DIR})

add_executable ( eos
	         ConsoleMain.cc
		 ConsoleMain.hh	
                 ../common/Logging.cc
                 ../common/Fmd.cc
                 ../common/SymKeys.cc
		 commands/com_access.cc
	         commands/com_attr.cc
		 commands/com_cd.cc
		 commands/com_chmod.cc 
		 commands/com_chown.cc 
		 commands/com_clear.cc         
		 commands/com_config.cc 
		 commands/com_console.cc 
		 commands/com_debug.cc 
		 commands/com_file.cc 
		 commands/com_find.cc 
		 commands/com_fs.cc 
		 commands/com_fsck.cc
                 commands/com_fuse.cc
		 commands/com_group.cc
                 commands/com_io.cc
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
		 commands/com_restart.cc 
		 commands/com_rm.cc 
		 commands/com_rmdir.cc 
		 commands/com_role.cc 
		 commands/com_rtlog.cc 
		 commands/com_silent.cc 
		 commands/com_space.cc
		 commands/com_test.cc 
		 commands/com_timing.cc
		 commands/com_transfers.cc 
		 commands/com_verify.cc 
		 commands/com_version.cc 
		 commands/com_vid.cc 
		 commands/com_whoami.cc   
                 commands/com_who.cc
)

target_link_libraries ( eos XrdClient XrdUtils pthread dl XrdMqClient-Static crypto uuid curses z readline )

INSTALL_PROGRAMS( /sbin FILES eosadmin )

install ( TARGETS eos
          LIBRARY DESTINATION ${LIB_INSTALL_DIR}
          RUNTIME DESTINATION bin
          ARCHIVE DESTINATION ${LIB_INSTALL_DIR}
)
