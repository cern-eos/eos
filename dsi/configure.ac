#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.57)
AC_INIT(eos-dsi, 0.2.0,[andreas.joachim.peters@cern.ch])
AC_CANONICAL_HOST
AC_CANONICAL_TARGET
AC_CONFIG_SRCDIR([dsi_eos.hh])

AC_PREFIX_DEFAULT(/usr/)

AM_INIT_AUTOMAKE(1.6 foreign)


case "$target_os" in
   tiger*)
     CXXFLAGS +=-D__FreeBSD__=10
     export CXXFLAGS
     AC_DEFINE(__macos__)
     ;;
   darwin*)
     CXXFLAGS +=-D__FreeBSD__=10
     export CXXFLAGS
     AC_DEFINE(__macos__)
     ;;
   apple*)
     CXXFLAGS +=-D__FreeBSD__=10
     export CXXFLAGS
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


AC_SUBST(MARK64)

AC_ARG_WITH(xrootd,
          [  --with-xrootd=installation of xrootd version to use],
          [XROOTD_LOCATION=$withval],
          [XROOTD_LOCATION=/usr/])

AC_SUBST(XROOTD_LOCATION)

AC_ARG_WITH(globus,
          [  --with-globus=installation of globus version to use],
          [GLOBUS_LOCATION=$withval],
          [GLOBUS_LOCATION=/opt/globus/])

AC_SUBST(GLOBUS_LOCATION)


AC_ARG_WITH(globus,
          [  --with-globus-flavor=globus flavor],
          [GLOBUS_FLAVOR=$withval],
          [GLOBUS_FLAVOR=gcc64dbg])

AC_SUBST(GLOBUS_FLAVOR)

AC_DEFINE(_LARGEFILE_SOURCE)
AC_DEFINE(_FILE_OFFSET_BITS,64)
AC_SYS_LARGEFILE

# Do all testing using C++
AC_LANG([C++])

# Checks for programs.
AC_PROG_CXX

dnl Disable libtool 1.5 support for languages we don't use
define([AC_LIBTOOL_LANG_F77_CONFIG], [:])dnl
define([AC_LIBTOOL_LANG_GCJ_CONFIG], [:])dnl
AC_PROG_LIBTOOL


# Checks for header files.
AC_HEADER_STDC
AC_CHECK_HEADERS([sys/time.h])

# Check for xrootd headers
if ! test -e "${XROOTD_LOCATION}/include/xrootd/XrdOuc/XrdOucString.hh"
then
   AC_MSG_ERROR([xrootd header files could not be found in prefix directory ${XROOTD_LOCATION}])
fi


# Checks for typedefs, structures, and compiler characteristics.
AC_HEADER_STDBOOL
AC_C_CONST
AC_TYPE_MODE_T
AC_TYPE_SIZE_T
AC_HEADER_TIME

# Checks for library functions.
AC_FUNC_STAT
AC_CHECK_FUNCS([gettimeofday mkdir strdup])

AC_CONFIG_FILES( Makefile 
		 eos-dsi.spec 
)

echo "=================================================="
echo "Configuring for .../lib$MARK64 library directories"
echo "=================================================="
AC_OUTPUT
