# Try to find python
# Once done, this will define
#
# PYTHONSITEPKG_FOUND - found python site packages directory
# PYTHONSITEPKG_PATH  - location where python modules are installed

if(PYTHONSITEPKG_FIND_REQUIRED)
  find_package(PythonInterp REQUIRED)
else()
  find_package(PythonInterp)
endif()

if(NOT PYTHONINTERP_FOUND)
  set(PYTHONSITEPKG_FOUND FALSE)
  return()
endif()

if(PYTHONSITEPKG_PATH)
  set(PYTHONSITEPKG_FIND_QUIETLY TRUE)
else()
  if((PYTHON_VERSION_MAJOR VERSION_EQUAL "3") OR (PYTHON_VERSION_MAJOR VERSION_GREATER "3"))
    set(PY_CMD "from distutils import sysconfig; print(sysconfig.get_python_lib());")
  else()
    set(PY_CMD "from __future__ import print_function; from distutils import sysconfig; print(sysconfig.get_python_lib());")
  endif()

  execute_process(
    COMMAND "${PYTHON_EXECUTABLE}" "-c" "${PY_CMD}"
    RESULT_VARIABLE _PYTHON_SUCCESS
    ERROR_VARIABLE _PYTHON_ERROR_VALUE
    OUTPUT_VARIABLE PYTHONSITEPKG_PATH
    OUTPUT_STRIP_TRAILING_WHITESPACE)

  if(NOT _PYTHON_SUCCESS MATCHES 0)
    if(PYTHONSITEPKG_FIND_REQUIRED)
      message(FATAL_ERROR "Python config failure:\n${_PYTHON_ERROR_VALUE}")
    else()
      message(STATUS "PythonSitePkg was not required")
    endif()

    set(PYTHONLIBS_FOUND FALSE)
    return()
  endif()

  string(REGEX REPLACE "\n" "" PYTHONSITEPKG_PATH ${PYTHONSITEPKG_PATH})

  include (FindPackageHandleStandardArgs)
  find_package_handle_standard_args(
    PythonSitePkg
    DEFAULT_MSG PYTHONSITEPKG_PATH)

  mark_as_advanced(PythonSitePkg PYTHONSITEPKG_PATH)
endif()
