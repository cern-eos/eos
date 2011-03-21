include_directories( ${CPPUNIT_INCLUDE_DIRS} )

add_library (eosNamespaceTests SHARED
      ChangeLogContainerMDSvcTest.cc
      ChangeLogFileMDSvcTest.cc
      ChangeLogTest.cc
      FileSystemViewTest.cc
      HierarchicalViewTest.cc
      LogCompactingTest.cc
      OtherTests.cc
)

target_link_libraries( eosNamespaceTests ${CPPUNIT_LIBRARIES} eosNamespace)

add_executable(text-runner TextRunner.cc)
target_link_libraries( text-runner eosNamespaceTests )


if (Linux)
  set_target_properties( eosNamespaceTests PROPERTIES COMPILE_FLAGS -fPIC )
endif (Linux)
