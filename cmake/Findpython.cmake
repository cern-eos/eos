# Try to find python
# Once done, this will define
#
# EOS_PYTHON_LIBDIR - location where python modules are installed

include (FindPackageHandleStandardArgs)

if(EOS_PYTHON_LIBDIR)
  set(PYTHON_FIND_QUIETLY TRUE)
else()
  execute_process(
    COMMAND python -c "from __future__ import print_function; from distutils import sysconfig; print(sysconfig.get_python_lib(True))"
    OUTPUT_VARIABLE EOS_PYTHON_LIBDIR)

  string(REGEX REPLACE "\n" "" EOS_PYTHON_LIBDIR ${EOS_PYTHON_LIBDIR})
  message(STATUS "EOS_PYTHON_LIBDIR = ${EOS_PYTHON_LIBDIR}")

  find_package_handle_standard_args(
    python
    DEFAULT_MSG EOS_PYTHON_LIBDIR)

  mark_as_advanced(EOS_PYTHON_LIBDIR)
endif()
