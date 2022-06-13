# Helper module to export the correct linker flags for charconv. Currently only invoked for clang on Linux.
# Sets target StdCharConv::charconv linker flags if additional flags are necessary. This is
# in particular mainly for clang which seems to have linker issues when using
# the default fsanitize=undefined flag. See
# https://bugs.llvm.org/show_bug.cgi?id=16404 and
# https://bugs.llvm.org/show_bug.cgi?id=28629 we add these flags only for clang
# compiler Additionally clang minor versions of 7 may not be patched with
# https://bugzilla.redhat.com/show_bug.cgi?id=1657544


# from_chars will invoke __builtin_mul_overflow to check for promoting
# integer types
macro(try_compile_from_chars
    from_chars_link_flags
    from_chars_compile_result)
  set(_from_chars_compiler_args CXX_STANDARD 17)
  if (NOT from_chars_link_flags STREQUAL "")
    list(APPEND _from_chars_compiler_args
      LINK_LIBRARIES ${from_chars_link_flags})
  endif()
  try_compile(_from_chars_compile_result
    ${CMAKE_CURRENT_BINARY_DIR}
    SOURCES "${CMAKE_CURRENT_LIST_DIR}/fromchars.cpp"
    ${_from_chars_compiler_args})
  set(${from_chars_compile_result} ${_from_chars_compile_result})
endmacro()

# For clang with  fsantize=undefined will go for 4 word multiply calling _mulodi4 which
# is only implemented by compiler-rt, see https://bugs.llvm.org/show_bug.cgi?id=16404
# and https://bugs.llvm.org/show_bug.cgi?id=28629 we add these flags only for clang compiler
# Additionally clang minor versions of 7 may not be patched with https://bugzilla.redhat.com/show_bug.cgi?id=1657544
foreach(linker_flags
    ""
    "-rtlib=compiler-rt -lgcc_s")
  try_compile_from_chars("${linker_flags}" from_chars_compiles)
  if(from_chars_compiles)
    set(_std_charconv_required_var TRUE)
    if(linker_flags STREQUAL "")
      message(STATUS "Compiler has default support for std::from_chars")
    else()
      set(CHARCONV_LINKER_FLAGS "${linker_flags}")
      message(STATUS "Compiler requires linker flags for std::from_chars: ${linker_flags}")
    endif()
  endif()
endforeach()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(CharConv
  FOUND_VAR CHARCONV_FOUND
  REQUIRED_VARS _std_charconv_required_var)

mark_as_advanced(CHARCONV_LINKER_FLAGS CHARCONV_FOUND)

if(CHARCONV_FOUND AND NOT (TARGET CHARCONV::CHARCONV))
  add_library(CHARCONV::CHARCONV INTERFACE IMPORTED)
  if(CHARCONV_LINKER_FLAGS)
    set_target_properties(CHARCONV::CHARCONV PROPERTIES
      INTERFACE_LINK_LIBRARIES ${CHARCONV_LINKER_FLAGS})
  endif()
endif()
