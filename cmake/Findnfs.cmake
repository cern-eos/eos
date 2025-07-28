# - Locate NFS library
# Defines:
#
# NFS_FOUND         -  system has libnfs
#
# and the following imported targets
#
# NFS::NFS

find_path(NFS_INCLUDE_DIR
  NAMES nfsc/libnfs.h
  HINTS
  /usr ${NFS_ROOT} $ENV{NFS_ROOT}
  PATH_SUFFIXES include)

find_library(NFS_LIBRARY
  NAMES nfs
  HINTS /usr ${NFS_ROOT} $ENV{NFS_ROOT}
  PATH_SUFFIXES ${CMAKE_INSTALL_LIBDIR})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(nfs
  REQUIRED_VARS NFS_LIBRARY NFS_INCLUDE_DIR)
mark_as_advanced(nfs_FOUND NFS_LIBRARY NFS_INCLUDE_DIR)

if (nfs_FOUND AND NOT TARGET NFS::NFS)
  add_library(NFS::NFS UNKNOWN IMPORTED)
  set_target_properties(NFS::NFS PROPERTIES
    IMPORTED_LOCATION "${NFS_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${NFS_INCLUDE_DIR}"
    INTERFACE_COMPILE_DEFINITIONS HAVE_NFS)
else()
  message(WARNING "Notice: libnfs not found, no NFS support")
  add_library(NFS::NFS INTERFACE IMPORTED)
endif()

unset(NFS_LIBRARY)
unset(NFS_INCLUDE_DIR)
