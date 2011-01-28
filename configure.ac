AC_PREREQ(2.57)
AC_INIT(eos,0.0.9,[andreas.joachim.peters@cern.ch])
AC_CANONICAL_HOST
AC_CANONICAL_TARGET
AC_CONFIG_SRCDIR([configure.ac])

AM_INIT_AUTOMAKE(1.6 foreign)

AC_PREFIX_DEFAULT(/opt/eos/)

export CXXFLAGS="-Wall -Werror -O2 -g $CXXFLAGS"

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
          [ --with-xrootd=installation of xrootd version to use],
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

# Check for xrootd headers
if ! test -e "${XROOTD}/include/xrootd/XrdCms/XrdCmsAdmin.hh"
then
   AC_MSG_ERROR([xrootd header files could not be found in ${XROOTD}/include/ directory ${XROOTD}])
fi

XROOTD_INCDIR="${XROOTD}/include/xrootd/"
XROOTD_LIBDIR="${XROOTD}/lib${MARK64}/"

EOS_INCDIR="${prefix}/include/"

AC_SUBST(XROOTD_INCDIR)
AC_SUBST(XROOTD_LIBDIR)
AC_SUBST(EOS_INCDIR)

# check for typedefs, structures and compiler characteristics.
AC_C_CONST

AC_CHECK_FUNCS([clock_gettime])
AC_CHECK_FUNCS([endgrent])
AC_CHECK_FUNCS([ftruncate])
AC_CHECK_FUNCS([gettimeofday])
AC_CHECK_FUNCS([memset])
AC_CHECK_FUNCS([mkdir])
AC_CHECK_FUNCS([munmap])
AC_CHECK_FUNCS([putenv])
AC_CHECK_FUNCS([setenv])
AC_CHECK_FUNCS([socket])
AC_CHECK_FUNCS([sqrt])
AC_CHECK_FUNCS([strchr])
AC_CHECK_FUNCS([strdup])
AC_CHECK_FUNCS([strerror])
AC_CHECK_FUNCS([strtol])
AC_CHECK_FUNCS([strtoul])
AC_CHECK_FUNCS([strtoull])

AC_CHECK_HEADERS([arpa/inet.h])
AC_CHECK_HEADERS([fcntl.h])
AC_CHECK_HEADERS([netdb.h])
AC_CHECK_HEADERS([netinet/in.h])
AC_CHECK_HEADERS([sys/param.h])
AC_CHECK_HEADERS([sys/socket.h])
AC_CHECK_HEADERS([sys/time.h])
AC_CHECK_HEADERS([sys/vfs.h])
AC_CHECK_HEADERS([utime.h])
AC_HEADER_DIRENT
AC_HEADER_TIME
AC_HEADER_STDBOOL
AC_HEADER_STDC

AC_CHECK_MEMBERS([struct stat.st_blksize])
AC_CHECK_MEMBERS([struct stat.st_rdev])

AC_FUNC_CHOWN
AC_FUNC_CLOSEDIR_VOID
AC_FUNC_LSTAT
AC_FUNC_MALLOC
AC_FUNC_MEMCMP
AC_FUNC_MMAP
AC_FUNC_REALLOC
AC_FUNC_STAT
AC_FUNC_STRFTIME
AC_FUNC_VPRINTF

AC_STRUCT_ST_BLOCKS
AC_STRUCT_TM

AC_TYPE_OFF_T
AC_TYPE_SIGNAL
AC_TYPE_UID_T
AC_TYPE_MODE_T
AC_TYPE_SIZE_T

# Check for required headers and libraries.
SAVE_LDFLAGS=$LDFLAGS

AC_CHECK_LIB(rt, clock_gettime, , AC_MSG_ERROR( "The system does not have clock_gettime" ) )

AC_CHECK_HEADER([zlib.h], ,
    [AC_MSG_ERROR([missing zlib.h, install zlib-devel])])
AC_CHECK_LIB([z], [crc32], ,
    [AC_MSG_ERROR([missing crc32() in -lz, install zlib-devel])])

AC_CHECK_LIB([ncurses], [wclear], ,
    [AC_MSG_ERROR([missing wclear() in -lncurses, install ncurses-devel])])

AC_CHECK_HEADER([readline/readline.h], ,
    [AC_MSG_ERROR([missing readline/readline.h, install readline-devel])])
AC_CHECK_LIB([readline], [rl_completion_matches], ,
    [AC_MSG_ERROR([missing uuid_generate() in -lreadline, install e2fsprogs-devel])])

AC_CHECK_HEADER([openssl/evp.h], ,
    [AC_MSG_ERROR([missing openssl/evp.h, install openssl-devel])])

AC_CHECK_HEADER([uuid/uuid.h], ,
    [AC_MSG_ERROR([missing uuid/uuid.h, install e2fsprogs-devel])])
AC_CHECK_LIB([uuid], [uuid_generate], ,
    [AC_MSG_ERROR([missing uuid_generate() in -luuid, install e2fsprogs-devel])])

AC_CHECK_LIB([termcap], [tputs], ,
    [AC_MSG_ERROR([missing tputs() in -ltermcap, install libtermcap-devel])])

export LDFLAGS=$LDFLAGS_SAVE

# Check for cppunit.
ifdef([AM_PATH_CPPUNIT], [AM_PATH_CPPUNIT([1.12.1])], [])

echo "=================================================="
echo "Configuring for .../lib$MARK64 library directories"
echo "Include Dir     : $XROOTD_INCDIR"
echo "Lib Dir         : $XROOTD_LIBDIR"
echo "=================================================="

# Generate output files.
AC_CONFIG_FILES([Makefile                  \
                 XrdCapability/Makefile    \
                 XrdFstOfs/Makefile        \
                 XrdMgmOfs/Makefile        \
                 XrdMqOfs/Makefile         \
                 XrdCommon/Makefile        \
                 Console/Makefile          \
                 Console/Commands/Makefile \
                 Sync/Makefile             \
                 Namespace/Makefile        \
                 Namespace/tests/Makefile])
AC_OUTPUT
