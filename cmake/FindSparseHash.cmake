# Try to find SparseHash
# Once done, this will define
#
# SPARSEHASH_FOUND        - system has SparseHash
# SPARSEHASH_INCLUDE_DIRS - SparseHash include directories

include(FindPackageHandleStandardArgs)

if(SPARSEHASH_INCLUDE_DIRS)
  set(SPARSEHASH_FIND_QUIETLY TRUE)
else()
  find_path(
    SPARSEHASH_INCLUDE_DIR
    NAMES google/sparsehash/sparsehashtable.h
    HINTS ${SPARSEHASH_ROOT_DIR}
    PATH_SUFFIXES include)

  set(SPARSEHASH_INCLUDE_DIRS ${SPARSEHASH_INCLUDE_DIR})

  find_package_handle_standard_args(
    SparseHash
    DEFAULT_MSG SPARSEHASH_INCLUDE_DIR)

  mark_as_advanced(SPARSEHASH_INCLUDE_DIR)
endif()