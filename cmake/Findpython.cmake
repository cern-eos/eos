# Try to find python
# The following variables are set if python is found.
#
# EOS_PYTHON_LIBDIR

# Be silent if path already cached
IF(EOS_PYTHON_LIBDIR)
  SET(PYTHON_FIND_QUIETLY TRUE)
ENDIF (EOS_PYTHON_LIBDIR)

EXECUTE_PROCESS(COMMAND python -c "from distutils import sysconfig; print sysconfig.get_python_lib()"
                OUTPUT_VARIABLE EOS_PYTHON_LIBDIR)
STRING(REGEX REPLACE "\n" "" EOS_PYTHON_LIBDIR ${EOS_PYTHON_LIBDIR})
MESSAGE(STATUS "EOS_PYTHON_LIBDIR = ${EOS_PYTHON_LIBDIR}")

include (FindPackageHandleStandardArgs)
find_package_handle_standard_args (python DEFAULT_MSG EOS_PYTHON_LIBDIR)
