# Try to find SparseHash
# Once done, this will define
#
# SPARSEHASH_FOUND        - system has SparseHash
# SPARSEHASH_INCLUDE_DIRS - SparseHash include directories
#
# and the following imported tags
#
# GOOGLE::SPARSEHASH

find_path(SPARSEHASH_INCLUDE_DIR
  NAMES google/sparsehash/sparsehashtable.h
  HINTS ${SPARSEHASH_ROOT})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(SparseHash
  REQUIRED_VARS SPARSEHASH_INCLUDE_DIR)
mark_as_advanced(SPARSEHASH_FOUND SPARSEHASH_INCLUDE_DIR)

if (SPARSEHASH_FOUND AND NOT TARGET GOOGLE::SPARSEHASH)
  add_library(GOOGLE::SPARSEHASH INTERFACE IMPORTED)
  set_target_properties(GOOGLE::SPARSEHASH PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${SPARSEHASH_INCLUDE_DIR}")
endif()

set(SPARSEHASH_INCLUDE_DIRS ${SPARSEHASH_INCLUDE_DIR})
unset(SPARSEHASH_INCLUDE_DIR)
