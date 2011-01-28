AC_INIT([eos apmon], [1.0.0])
AC_PREREQ(2.53)
AC_CONFIG_SRCDIR([etc/init.d/eosapmond])

AC_CANONICAL_HOST
AM_INIT_AUTOMAKE([1.6.3 subdir-objects])

# Checks for programs.
AC_PROG_CC
AM_PROG_CC_C_O
AC_PROG_INSTALL
AC_PROG_LN_S

# Check for typedefs, structures and compiler characteristics.
AC_CHECK_HEADERS([stdlib.h])
AC_CHECK_HEADERS([unistd.h])

AC_FUNC_FORK
AC_FUNC_SETVBUF_REVERSED

AC_HEADER_STDC

AC_TYPE_PID_T

AC_CONFIG_FILES([Makefile])
AC_OUTPUT
