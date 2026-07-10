# ----------------------------------------------------------------------
# Build the cern-nfs git submodule as an independent CMake project.
# ----------------------------------------------------------------------

option(BUILD_CERN_NFS "Build the cern-nfs submodule" ON)

add_library(EosCernNfsEmbed-Objects OBJECT
  ${CMAKE_SOURCE_DIR}/common/CernNfsEmbed.cc)

set_target_properties(EosCernNfsEmbed-Objects PROPERTIES
  POSITION_INDEPENDENT_CODE TRUE)

target_include_directories(EosCernNfsEmbed-Objects PRIVATE
  ${CMAKE_SOURCE_DIR})

if(NOT BUILD_CERN_NFS)
  return()
endif()

include(ExternalProject)

set(CERN_NFS_SOURCE_DIR "${CMAKE_SOURCE_DIR}/cern-nfs")
set(CERN_NFS_BINARY_DIR "${CMAKE_BINARY_DIR}/cern-nfs")

# cern-nfs vendors libnfs (and other deps) as nested submodules. A plain
# `git submodule update --init cern-nfs` from EOS is not enough; we need
# --recursive so third_party/libnfs is populated before configuring cern-nfs.
if(GIT_FOUND AND EXISTS "${CMAKE_SOURCE_DIR}/.gitmodules")
  file(READ "${CMAKE_SOURCE_DIR}/.gitmodules" _cern_nfs_gitmodules)
  if(_cern_nfs_gitmodules MATCHES "cern-nfs")
    set(_cern_nfs_needs_init FALSE)
    if(NOT EXISTS "${CERN_NFS_SOURCE_DIR}/CMakeLists.txt")
      set(_cern_nfs_needs_init TRUE)
    elseif(NOT EXISTS "${CERN_NFS_SOURCE_DIR}/third_party/libnfs/CMakeLists.txt")
      set(_cern_nfs_needs_init TRUE)
    endif()

    if(_cern_nfs_needs_init)
      message(STATUS "cern-nfs nested submodules not initialized -- running git submodule update --init --recursive cern-nfs")
      execute_process(
        COMMAND ${GIT_EXECUTABLE} submodule update --init --recursive cern-nfs
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        RESULT_VARIABLE _cern_nfs_init_rc
        ERROR_VARIABLE _cern_nfs_init_err
      )
      if(NOT _cern_nfs_init_rc EQUAL 0)
        message(WARNING
          "Failed to initialize cern-nfs submodule recursively:\n${_cern_nfs_init_err}\n"
          "Run: git submodule update --init --recursive cern-nfs")
      endif()

      if(NOT EXISTS "${CERN_NFS_SOURCE_DIR}/third_party/libnfs/CMakeLists.txt")
        message(STATUS "cern-nfs nested submodules still missing -- retrying from inside cern-nfs")
        execute_process(
          COMMAND ${GIT_EXECUTABLE} submodule update --init --recursive
          WORKING_DIRECTORY ${CERN_NFS_SOURCE_DIR}
          RESULT_VARIABLE _cern_nfs_inner_init_rc
          ERROR_VARIABLE _cern_nfs_inner_init_err
        )
        if(NOT _cern_nfs_inner_init_rc EQUAL 0)
          message(WARNING
            "Failed to initialize nested cern-nfs submodules:\n${_cern_nfs_inner_init_err}")
        endif()
      endif()
    endif()
  endif()
endif()

if(NOT EXISTS "${CERN_NFS_SOURCE_DIR}/CMakeLists.txt")
  message(STATUS "cern-nfs: submodule source not available (BUILD_CERN_NFS=ON)")
  return()
endif()

set(_cern_nfs_ep_cmake_args
  -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
  -DCMAKE_INSTALL_PREFIX=${CERN_NFS_BINARY_DIR}/install
  -DSPDLOG_BUILD_SHARED=ON
  -DCERNNFS_SKIP_EOS_MGM_VFS=ON
  -DCERNNFS_SKIP_EOS_FST_VFS=ON
  -DCERNNFS_ENABLE_REDIS=OFF
  # EOS embeds cern-nfs via ExternalProject (INSTALL_COMMAND "") and copies
  # artifacts manually; skip cern-nfs install(EXPORT) rules that fail when
  # libcernnfs links privately to FetchContent spdlog.
  -DCMAKE_SKIP_INSTALL_RULES=ON
)

if(CMAKE_GENERATOR)
  list(APPEND _cern_nfs_ep_cmake_args -G "${CMAKE_GENERATOR}")
endif()

if(CMAKE_MAKE_PROGRAM)
  list(APPEND _cern_nfs_ep_cmake_args -DCMAKE_MAKE_PROGRAM=${CMAKE_MAKE_PROGRAM})
endif()

if(DEFINED CMAKE_CXX_COMPILER)
  list(APPEND _cern_nfs_ep_cmake_args -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER})
endif()

if(DEFINED CMAKE_C_COMPILER)
  list(APPEND _cern_nfs_ep_cmake_args -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER})
endif()

# Match the XRootD installation used by EOS (/opt/eos/xrootd in production).
if(XROOTD_FOUND AND XROOTD_CL_LIBRARY)
  get_filename_component(_cern_nfs_xrootd_libdir "${XROOTD_CL_LIBRARY}" DIRECTORY)
  get_filename_component(_cern_nfs_xrootd_root "${_cern_nfs_xrootd_libdir}" DIRECTORY)
elseif(DEFINED ENV{XROOTD_ROOT} AND NOT "$ENV{XROOTD_ROOT}" STREQUAL "")
  set(_cern_nfs_xrootd_root "$ENV{XROOTD_ROOT}")
elseif(EXISTS "/opt/eos/xrootd/lib64/libXrdCl.so")
  set(_cern_nfs_xrootd_root "/opt/eos/xrootd")
else()
  set(_cern_nfs_xrootd_root "")
endif()

if(_cern_nfs_xrootd_root)
  message(STATUS "cern-nfs ExternalProject: using XRootD from ${_cern_nfs_xrootd_root}")
  list(APPEND _cern_nfs_ep_cmake_args "-DCMAKE_PREFIX_PATH=${_cern_nfs_xrootd_root}")
  foreach(_cern_nfs_xrootd_libdir lib64 lib)
    if(EXISTS "${_cern_nfs_xrootd_root}/${_cern_nfs_xrootd_libdir}/cmake/XRootD/XRootDConfig.cmake")
      list(APPEND _cern_nfs_ep_cmake_args
        "-DXRootD_DIR=${_cern_nfs_xrootd_root}/${_cern_nfs_xrootd_libdir}/cmake/XRootD")
      break()
    endif()
  endforeach()
endif()

# Prefer system libnfs when the vendored submodule checkout is unavailable.
if(NOT EXISTS "${CERN_NFS_SOURCE_DIR}/third_party/libnfs/CMakeLists.txt")
  find_package(PkgConfig QUIET)
  if(PkgConfig_FOUND)
    pkg_check_modules(_EOS_CERN_NFS_LIBNFS QUIET libnfs)
  endif()
  if(_EOS_CERN_NFS_LIBNFS_FOUND)
    message(STATUS "cern-nfs ExternalProject: using system libnfs")
    list(APPEND _cern_nfs_ep_cmake_args -DUSE_VENDORED_LIBNFS=OFF)
  else()
    message(WARNING
      "cern-nfs: vendored libnfs is missing and system libnfs was not found via pkg-config")
  endif()
endif()

set(CERN_NFS_LIBRARY "${CERN_NFS_BINARY_DIR}/lib/libcernnfs.so")
set(_CERN_NFS_SPDLOG_INCLUDE "${CERN_NFS_BINARY_DIR}/_deps/spdlog-src/include")
set(CERN_NFS_SPDLOG_LIBRARY "${CERN_NFS_BINARY_DIR}/_deps/spdlog-build/libspdlog.so")
# ExternalProject configure runs at build time, but EosCernNfsEmbed needs
# spdlog headers at EOS configure time. Populate only the FetchContent source
# tree (same layout cern-nfs uses) instead of configuring the full submodule,
# which can fail on install(EXPORT) validation before any build is needed.
if(NOT EXISTS "${_CERN_NFS_SPDLOG_INCLUDE}/spdlog/spdlog.h")
  message(STATUS "cern-nfs: populating FetchContent spdlog for configure-time headers")
  include(FetchContent)
  file(MAKE_DIRECTORY "${CERN_NFS_BINARY_DIR}/_deps")
  set(FETCHCONTENT_BASE_DIR "${CERN_NFS_BINARY_DIR}/_deps")
  FetchContent_Declare(
    spdlog
    GIT_REPOSITORY https://github.com/gabime/spdlog.git
    GIT_TAG v1.12.0
  )
  FetchContent_GetProperties(spdlog)
  if(NOT spdlog_POPULATED)
    FetchContent_Populate(spdlog)
  endif()
  if(spdlog_SOURCE_DIR)
    set(_CERN_NFS_SPDLOG_INCLUDE "${spdlog_SOURCE_DIR}/include")
  endif()
endif()
if(NOT EXISTS "${_CERN_NFS_SPDLOG_INCLUDE}/spdlog/spdlog.h")
  message(FATAL_ERROR
    "cern-nfs spdlog headers not found at ${_CERN_NFS_SPDLOG_INCLUDE} after FetchContent populate")
endif()
ExternalProject_Add(cern_nfs_ep
  SOURCE_DIR "${CERN_NFS_SOURCE_DIR}"
  BINARY_DIR "${CERN_NFS_BINARY_DIR}"
  CMAKE_ARGS ${_cern_nfs_ep_cmake_args}
  # Rebuild libcernnfs whenever the parent EOS build runs so cern-nfs source
  # edits (e.g. nfs40_server.cpp) are not missed by a stale ExternalProject.
  BUILD_ALWAYS TRUE
  # EOS embeds libcernnfs into XrdEosMgm/XrdEosFst; skip the standalone
  # cern-nfs server binary here — with CERNNFS_SKIP_EOS_*_VFS=ON the stub
  # VFS objects are omitted from libcernnfs and would leave undefined
  # EosEmbedMgmFS symbols for that executable to resolve.
  BUILD_COMMAND ${CMAKE_COMMAND} --build <BINARY_DIR> --target cernnfs cfs
  INSTALL_COMMAND ""
  BUILD_BYPRODUCTS
    "${CERN_NFS_BINARY_DIR}/exec/cern-nfs"
    "${CERN_NFS_BINARY_DIR}/exec/cfs"
    "${CERN_NFS_LIBRARY}"
    "${CERN_NFS_SPDLOG_LIBRARY}"
  USES_TERMINAL_CONFIGURE 1
  USES_TERMINAL_BUILD 1
)

# Imported target for EOS MGM/FST to link against without add_subdirectory(),
# which would collide with EOS targets (CLI11, rpm, uninstall, ...).
add_library(cern_nfs_spdlog SHARED IMPORTED GLOBAL)
add_dependencies(cern_nfs_spdlog cern_nfs_ep)
set_target_properties(cern_nfs_spdlog PROPERTIES
  IMPORTED_LOCATION "${CERN_NFS_SPDLOG_LIBRARY}"
  INTERFACE_COMPILE_DEFINITIONS SPDLOG_COMPILED_LIB
)
add_library(cernnfs::spdlog ALIAS cern_nfs_spdlog)

add_library(cernnfs SHARED IMPORTED GLOBAL)
add_library(cernnfs::cernnfs ALIAS cernnfs)
add_dependencies(cernnfs cern_nfs_ep)
set_target_properties(cernnfs PROPERTIES
  IMPORTED_LOCATION "${CERN_NFS_LIBRARY}"
  INTERFACE_INCLUDE_DIRECTORIES
    "${CERN_NFS_SOURCE_DIR}/include;${CERN_NFS_SOURCE_DIR}/lib/include;${CERN_NFS_SOURCE_DIR}/proto/include"
  INTERFACE_COMPILE_DEFINITIONS EOS_HAVE_CERN_NFS=1
  INTERFACE_LINK_LIBRARIES cernnfs::spdlog
)
target_include_directories(EosCernNfsEmbed-Objects PUBLIC
  "${_CERN_NFS_SPDLOG_INCLUDE}")
target_link_libraries(EosCernNfsEmbed-Objects PUBLIC cernnfs::cernnfs
  XROOTD::UTILS)
target_compile_definitions(EosCernNfsEmbed-Objects PUBLIC EOS_HAVE_CERN_NFS=1
  SPDLOG_COMPILED_LIB)
add_dependencies(EosCernNfsEmbed-Objects cern_nfs_ep)

add_custom_target(cern-nfs ALL DEPENDS cern_nfs_ep)

add_custom_target(cern-nfs-all
  COMMAND ${CMAKE_COMMAND} --build ${CERN_NFS_BINARY_DIR}
  DEPENDS cern_nfs_ep
  COMMENT "Building full cern-nfs project"
  USES_TERMINAL
)

set(CERN_NFS_EXECUTABLE "${CERN_NFS_BINARY_DIR}/exec/cern-nfs" CACHE FILEPATH
  "Path to the cern-nfs server binary built by the submodule")

set(EOS_HAVE_CERN_NFS TRUE CACHE BOOL "cern-nfs library is available for embedding")

list(APPEND CMAKE_BUILD_RPATH "${CERN_NFS_BINARY_DIR}/lib"
  "${CERN_NFS_BINARY_DIR}/_deps/spdlog-build")
set(CMAKE_BUILD_RPATH "${CMAKE_BUILD_RPATH}" CACHE STRING
  "Build-time RPATH for EOS binaries and plugins" FORCE)

# Install the full libcernnfs.so -> .so.0 -> .so.0.x.y symlink chain. A plain
# install(FILES ... OPTIONAL) only copied the top symlink, leaving .so.0 missing.
install(CODE "
  set(_cern_nfs_libdir \"${CERN_NFS_BINARY_DIR}/lib\")
  if(EXISTS \"\${_cern_nfs_libdir}/libcernnfs.so\")
    file(INSTALL \"\${_cern_nfs_libdir}/libcernnfs.so\"
      DESTINATION \"\${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR}\"
      TYPE SHARED_LIBRARY
      FOLLOW_SYMLINK_CHAIN)
  else()
    message(WARNING \"libcernnfs.so not found in \${_cern_nfs_libdir}; skipping install\")
  endif()
  set(_cern_nfs_spdlog \"${CERN_NFS_SPDLOG_LIBRARY}\")
  if(EXISTS \"\${_cern_nfs_spdlog}\")
    file(INSTALL \"\${_cern_nfs_spdlog}\"
      DESTINATION \"\${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR}\"
      TYPE SHARED_LIBRARY
      FOLLOW_SYMLINK_CHAIN)
  endif()
")
