# Locate help2man executable
# Defines:
#
#  HELP2MAN_FOUND        -  system has help2man
#  HELP2MAN_EXECUTABLE   -  help2man executable

include(FindPackageHandleStandardArgs)

find_program(HELP2MAN_EXECUTABLE
  NAMES help2man
  HINTS /usr $ENV{HELP2MAN_DIR}
  PATH_SUFFIXES bin)

find_package_handle_standard_args(
  help2man
  DEFAULT_MSG
  HELP2MAN_EXECUTABLE)

mark_as_advanced(HELP2MAN_EXECUTABLE)
