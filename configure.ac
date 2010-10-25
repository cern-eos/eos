AC_PREREQ(2.57)
AC_INIT(eos,0.0.9,[andreas.joachim.peters@cern.ch])
AC_CANONICAL_HOST
AC_CANONICAL_TARGET
AC_CONFIG_SRCDIR([configure.ac])

AM_INIT_AUTOMAKE(1.6 foreign)

AC_PREFIX_DEFAULT(/opt/eos/)


export CXXFLAGS="-Wall -Werror -O2 -g $CXXFLAGS"
#export CXXFLAGS="-Wall -Werror -O0 -ggdb $CXXFLAGS"

case "$target_os" in
   tiger*)
     AC_DEFINE(__macos__)
     ;;
   darwin*)
     AC_DEFINE(__macos__)
     ;;
   apple*)
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
          [XROOTD=$withval],
          [XROOTD=/opt/eos/])

AC_SUBST(XROOTD)

AC_DEFINE(_LARGEFILE_SOURCE)
AC_DEFINE(_FILE_OFFSET_BITS,64)
AC_SYS_LARGEFILE

# Do all testing using C++
AC_LANG(C++)

# Checks for programs.
AC_PROG_CXX


define([AC_LIBTOOL_LANG_F77_CONFIG], [:])dnl
define([AC_LIBTOOL_LANG_GCJ_CONFIG], [:])dnl
AC_PROG_LIBTOOL


AC_HEADER_STDC
# Check for xrootd headers
if ! test -e "${XROOTD}/include/xrootd/XrdCms/XrdCmsAdmin.hh"
then
   AC_MSG_ERROR([xrootd header files could not be found in ${XROOTD}/include/ directory ${XROOTD}])
fi

# Checks for typedefs, structures, and compiler characteristics.
AC_HEADER_STDBOOL
AC_C_CONST
AC_TYPE_MODE_T
AC_TYPE_SIZE_T
AC_HEADER_TIME

# Checks for library functions.
AC_FUNC_STAT
 
AC_CONFIG_FILES(Makefile
	        XrdCapability/Makefile
		XrdFstOfs/Makefile
		XrdMgmOfs/Makefile
		XrdMqOfs/Makefile
		XrdCommon/Makefile
		Console/Makefile
		Console/Commands/Makefile
		Sync/Makefile
		eos.spec
		Namespace/Makefile
		Namespace/tests/Makefile
		)

XROOTD_INCDIR="${XROOTD}/include/xrootd/"
XROOTD_LIBDIR="${XROOTD}/lib${MARK64}/"


EOS_INCDIR="${prefix}/include/"


AC_SUBST(XROOTD_INCDIR)
AC_SUBST(XROOTD_LIBDIR)
AC_SUBST(EOS_INCDIR)
	
# Check for readline
export LDFLAGS_BUP=$LDFLAGS
export LDFLAGS=-lcurses
AC_CHECK_HEADERS([readline/readline.h], ,
    AC_MSG_ERROR( "Please install libreadline-dev" ))

AC_CHECK_LIB(readline, readline, , AC_MSG_ERROR( "Please install lib readline & curses" ) )
AC_CHECK_LIB(rt, clock_gettime, , AC_MSG_ERROR( "The system does not have clock_gettime" ) )
AC_CHECK_LIB(z, crc32, , AC_MSG_ERROR( "Please install zlib" ) )
AC_CHECK_LIB(termcap, tputs, ,AC_MSG_ERROR( "Please install libtermcap" ) )
AC_CHECK_LIB(ncurses, wclear, ,AC_MSG_ERROR( "Please install libncurses" ) )

export LDFLAGS=$LDFLAGS_BUP 

# check for cppunit
ifdef([AM_PATH_CPPUNIT], [AM_PATH_CPPUNIT([1.12.1])], [])

echo "=================================================="
echo "Configuring for .../lib$MARK64 library directories"
echo "Include Dir     : $XROOTD_INCDIR"
echo "Lib Dir         : $XROOTD_LIBDIR"
echo "=================================================="
AC_OUTPUT
