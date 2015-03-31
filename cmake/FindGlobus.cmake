# - Locate Globus libraries
# Defines:
#
#  GLOBUS_FOUND
#  GLOBUS_INCLUDE_DIR
#  GLOBUS_INCLUDE_DIRS (not cached)
#  GLOBUS_LIBRARIES (not cached)
#  GLOBUS_xxx_LIBRARY

set( headers globus_config globus_gsi_credential )
foreach( head ${headers} )
  find_path(GLOBUS_INCLUDE_DIR_${head} NAMES ${head}.h
          HINTS ${GLOBUS_DIR}/include $ENV{GLOBUS_LOCATION}/include
                /usr/include/x86_64-linux-gnu /opt/globus/include /usr/include
          PATH_SUFFIXES gcc32 gcc32dbg gcc32pthr gcc32dbgpthr
                        gcc64 gcc64dbg gcc64pthr gcc64dbgpthr globus)
  if(GLOBUS_INCLUDE_DIR_${head})
    list(APPEND GLOBUS_INCLUDE_DIRS ${GLOBUS_INCLUDE_DIR_${head}})
  endif()
endforeach()

#message("found GLOBUS include dirs" ${GLOBUS_INCLUDE_DIRS})

set(libraries gssapi_gsi gss_assist gsi_credential common gsi_callback proxy_ssl
              gsi_sysconfig openssl_error oldgaa gsi_cert_utils
              openssl gsi_proxy_core callout)

foreach( lib ${libraries})
  find_library(GLOBUS_${lib}_LIBRARY NAMES globus_${lib} HINTS 
               ${GLOBUS_DIR}/lib $ENV{GLOBUS_LOCATION}/lib /usr/lib/x86_64-linux-gnu )
  if(GLOBUS_${lib}_LIBRARY)
    set(GLOBUS_${lib}_FOUND 1)
    list(APPEND GLOBUS_LIBRARIES ${GLOBUS_${lib}_LIBRARY})
    mark_as_advanced(GLOBUS_${lib}_LIBRARY)
  endif()
endforeach()


# handle the QUIETLY and REQUIRED arguments and set GLOBUS_FOUND to TRUE if
# all listed variables are TRUE
INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(GLOBUS DEFAULT_MSG GLOBUS_INCLUDE_DIR GLOBUS_common_LIBRARY)

mark_as_advanced(GLOBUS_FOUND GLOBUS_INCLUDE_DIR)
