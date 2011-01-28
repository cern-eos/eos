#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.57)
AC_INIT(eos-fuse, 0.0.2, [andreas.joachim.peters@cern.ch])
AC_CANONICAL_HOST
AC_CANONICAL_TARGET
AC_CONFIG_SRCDIR([eosd.c])

AM_INIT_AUTOMAKE(1.6 foreign)

LIBGCCS="-lgcc_s"
case "$target_os" in
   tiger*)
     CXXFLAGS +=-D__FreeBSD__=10
     export CXXFLAGS
     LIBGCCS="-lgcc_s.1"
     AC_DEFINE(__macos__)
     ;;
   darwin*)
     CXXFLAGS +=-D__FreeBSD__=10
     export CXXFLAGS
     LIBGCCS="-lgcc_s.1"
     AC_DEFINE(__macos__)
     ;;
   apple*)
     CXXFLAGS +=-D__FreeBSD__=10
     export CXXFLAGS
     LIBGCCS="-lgcc_s.1"
     AC_DEFINE(__macos__)
     ;;
   linux*)
     # TODO: check why two different macros are used for this
     AC_DEFINE(__linux__)
     AC_DEFINE(__linux)
     ;;
   solaris*)
     AC_DEFINE(__sun)
     CC_PREFERENCE="cc gcc"
     CXX_PREFERENCE="CC g++"
     DFLT_OPT="-O2"
     ;;
   *)
     AC_MSG_WARN([untested operating system])
   ;;
esac

echo $libdir | grep lib64 >& /dev/null
if test "$?" = "0" ; then
   MARK64=64;
else
   MARK64="";
fi

AC_SUBST(LIBGCCS)
AC_SUBST(MARK64)

AC_ARG_WITH(xrootd,
          [ --with-xrootd=installation of xrootd version to use],
          [XROOTD_LOCATION=$withval],
          [XROOTD_LOCATION=/usr/])

AC_SUBST(XROOTD_LOCATION)

AC_ARG_WITH(fuse,
          [ --with-fuse=installation of fuse version to use],
          [FUSE_LOCATION=$withval],
          [FUSE_LOCATION=/usr/local/])

AC_SUBST(FUSE_LOCATION)

AC_DEFINE(_LARGEFILE_SOURCE)
AC_DEFINE(_FILE_OFFSET_BITS,64)
AC_SYS_LARGEFILE

# Do all testing using C++
AC_LANG([C++])

# Checks for programs.
AC_PROG_CXX

# Disable libtool 1.5 support for languages we don't use
define([AC_LIBTOOL_LANG_F77_CONFIG], [:])dnl
define([AC_LIBTOOL_LANG_GCJ_CONFIG], [:])dnl
AC_PROG_LIBTOOL

# Check for xrootd headers
if ! test -e "${XROOTD_LOCATION}/include/xrootd/XrdOuc/XrdOucString.hh"
then
   AC_MSG_ERROR([xrootd header files could not be found in prefix directory ${XROOTD_LOCATION}])
fi

# Check for typedefs, structures and compiler characteristics.
AC_C_CONST

AC_CHECK_FUNCS([memset])
AC_CHECK_FUNCS([setenv])
AC_CHECK_FUNCS([strchr])
AC_CHECK_FUNCS([strstr])
AC_CHECK_FUNCS([gettimeofday mkdir strdup])

AC_CHECK_HEADERS([fcntl.h])
AC_CHECK_HEADERS([sys/param.h])
AC_CHECK_HEADERS([sys/time.h])
AC_CHECK_HEADERS([fuse/fuse_lowlevel.h])
AC_HEADER_DIRENT
AC_HEADER_TIME
AC_HEADER_STDBOOL
AC_HEADER_STDC

AC_CHECK_MEMBERS([struct stat.st_blksize])
AC_CHECK_MEMBERS([struct stat.st_rdev])

AC_FUNC_MALLOC
AC_FUNC_REALLOC
AC_FUNC_STAT

AC_STRUCT_ST_BLOCKS

AC_TYPE_OFF_T
AC_TYPE_UID_T
AC_TYPE_MODE_T
AC_TYPE_SIZE_T

echo "=================================================="
echo "Configuring for .../lib$MARK64 library directories"
echo "=================================================="

# Generate output files.
AC_CONFIG_FILES([Makefile])
AC_OUTPUT
