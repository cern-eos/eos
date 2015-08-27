add_library(persistenthashtable SHARED
    alloc.c
    persistent_hashtable.c
    util.c
)

include(FindZLIB)

if (NOT ZLIB_FOUND)
    message(FATAL_ERROR "Could not find ZLIB.")
endif()

target_include_directories(
    persistenthashtable
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR} ${ZLIB_INCLUDE_DIR}
)

target_link_libraries(
    persistenthashtable
    LINK_PUBLIC ${ZLIB_LIBRARIES}
)

include(GNUInstallDirs)
install ( TARGETS persistenthashtable
          LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
          RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR}
          ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
)
