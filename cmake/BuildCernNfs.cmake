# ----------------------------------------------------------------------
# Build the cern-nfs git submodule as an independent CMake project.
# ----------------------------------------------------------------------

option(BUILD_CERN_NFS "Build the cern-nfs submodule" ON)

if(NOT BUILD_CERN_NFS)
  return()
endif()

set(CERN_NFS_SOURCE_DIR "${CMAKE_SOURCE_DIR}/cern-nfs")

# Initialize the submodule when the checkout is missing but registered.
if(NOT EXISTS "${CERN_NFS_SOURCE_DIR}/CMakeLists.txt")
  if(GIT_FOUND AND EXISTS "${CMAKE_SOURCE_DIR}/.gitmodules")
    file(READ "${CMAKE_SOURCE_DIR}/.gitmodules" _cern_nfs_gitmodules)
    if(_cern_nfs_gitmodules MATCHES "cern-nfs")
      message(STATUS "cern-nfs submodule not initialized -- running git submodule update --init")
      execute_process(
        COMMAND ${GIT_EXECUTABLE} submodule update --init --recursive cern-nfs
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        RESULT_VARIABLE _cern_nfs_init_rc
        ERROR_VARIABLE _cern_nfs_init_err
      )
      if(NOT _cern_nfs_init_rc EQUAL 0)
        message(WARNING
          "Failed to initialize cern-nfs submodule:\n${_cern_nfs_init_err}\n"
          "Run: git submodule update --init --recursive cern-nfs")
      endif()
    endif()
  endif()
endif()

if(NOT EXISTS "${CERN_NFS_SOURCE_DIR}/CMakeLists.txt")
  message(STATUS "cern-nfs: submodule source not available (BUILD_CERN_NFS=ON)")
  return()
endif()

include(ExternalProject)

set(CERN_NFS_BINARY_DIR "${CMAKE_BINARY_DIR}/cern-nfs")

set(_cern_nfs_ep_cmake_args
  -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
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

ExternalProject_Add(cern_nfs_ep
  SOURCE_DIR "${CERN_NFS_SOURCE_DIR}"
  BINARY_DIR "${CERN_NFS_BINARY_DIR}"
  CMAKE_ARGS ${_cern_nfs_ep_cmake_args}
  BUILD_COMMAND ${CMAKE_COMMAND} --build <BINARY_DIR> --target cern-nfs cfs
  INSTALL_COMMAND ""
  BUILD_BYPRODUCTS
    "${CERN_NFS_BINARY_DIR}/exec/cern-nfs"
    "${CERN_NFS_BINARY_DIR}/exec/cfs"
  USES_TERMINAL_CONFIGURE 1
  USES_TERMINAL_BUILD 1
)

# Hook into the default `make` / `all` target.
add_custom_target(cern-nfs ALL DEPENDS cern_nfs_ep)

# Full cern-nfs tree (libraries, tests, packaging helpers, ...).
add_custom_target(cern-nfs-all
  COMMAND ${CMAKE_COMMAND} --build ${CERN_NFS_BINARY_DIR}
  DEPENDS cern_nfs_ep
  COMMENT "Building full cern-nfs project"
  USES_TERMINAL
)

set(CERN_NFS_EXECUTABLE "${CERN_NFS_BINARY_DIR}/exec/cern-nfs" CACHE FILEPATH
  "Path to the cern-nfs server binary built by the submodule")
