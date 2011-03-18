include_directories( ../ /usr/include/xrootd/ /usr/include/ )

if(CPPUNIT_FOUND)
add_subdirectory( tests )
endif(CPPUNIT_FOUND)

add_library (eosNamespace SHARED 
	    accounting/FileSystemView.hh
	    ContainerMD.hh
	    FileMD.hh
	    IContainerMDSvc.hh
	    IFileMDSvc.hh
	    IView.hh
	    MDException.hh
	    persistency/Buffer.hh
	    persistency/ChangeLogConstants.hh
	    persistency/ChangeLogContainerMDSvc.hh
	    persistency/ChangeLogFile.hh
	    persistency/ChangeLogFileMDSvc.hh
	    persistency/LogManager.hh
	    utils/DataHelper.hh
	    utils/Descriptor.hh
	    utils/DisplayHelper.hh
	    utils/PathProcessor.hh
	    utils/SmartPtrs.hh
	    views/HierarchicalView.hh
	    accounting/FileSystemView.cc
	    accounting/FileSystemView.hh
	    ContainerMD.cc
	    FileMD.cc
	    persistency/ChangeLogConstants.cc
	    persistency/ChangeLogContainerMDSvc.cc
	    persistency/ChangeLogFile.cc
	    persistency/ChangeLogFileMDSvc.cc
	    persistency/LogManager.cc
	    utils/DataHelper.cc
	    utils/Descriptor.cc
	    views/HierarchicalView.cc
	    )

add_library (eosNamespace-Static STATIC
	    accounting/FileSystemView.hh
	    ContainerMD.hh
	    FileMD.hh
	    IContainerMDSvc.hh
	    IFileMDSvc.hh
	    IView.hh
	    MDException.hh
	    persistency/Buffer.hh
	    persistency/ChangeLogConstants.hh
	    persistency/ChangeLogContainerMDSvc.hh
	    persistency/ChangeLogFile.hh
	    persistency/ChangeLogFileMDSvc.hh
	    persistency/LogManager.hh
	    utils/DataHelper.hh
	    utils/Descriptor.hh
	    utils/DisplayHelper.hh
	    utils/PathProcessor.hh
	    utils/SmartPtrs.hh
	    views/HierarchicalView.hh
	    accounting/FileSystemView.cc
	    accounting/FileSystemView.hh
	    ContainerMD.cc
	    FileMD.cc
	    persistency/ChangeLogConstants.cc
	    persistency/ChangeLogContainerMDSvc.cc
	    persistency/ChangeLogFile.cc
	    persistency/ChangeLogFileMDSvc.cc
	    persistency/LogManager.cc
	    progs/EOSLogCompact.cc
	    progs/EOSLogRepair.cc
	    utils/DataHelper.cc
	    utils/Descriptor.cc
	    views/HierarchicalView.cc
)

target_link_libraries( eosNamespace pthread z rt )
target_link_libraries( eosNamespace-Static pthread z rt )

add_executable ( eos-log-compact progs/EOSLogCompact.cc )
add_executable ( eos-log-repair  progs/EOSLogRepair.cc )

target_link_libraries ( eos-log-compact eosNamespace-Static )
target_link_libraries ( eos-log-repair eosNamespace-Static )

if (Linux)
  set_target_properties ( eosNamespace-Static PROPERTIES COMPILE_FLAGS -fPIC)
endif (Linux)

install ( TARGETS eos-log-compact eos-log-repair
          LIBRARY DESTINATION ${LIB_INSTALL_DIR}
          RUNTIME DESTINATION bin
          ARCHIVE DESTINATION ${LIB_INSTALL_DIR}
)
