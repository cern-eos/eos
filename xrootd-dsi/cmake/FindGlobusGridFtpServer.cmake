# Try to find leveldb
# Once done, this will define
#
# GLOBUSGRIDFTPSERVER_FOUND       - system has GlobusGridFtpServer
# GLOBUSGRIDFTPSERVER_INCLUDE_DIR - the GlobusGridFtpServer include directory
# GLOBUSGRIDFTPSERVER_LIB_DIR     - the GlobusGridFtpServer library directory
#
# GLOBUSGRIDFTPSERVER_DIR may be defined as a hint for where to look

FIND_PATH(GLOBUSGRIDFTPSERVER_INCLUDE_DIR globus_gridftp_server.h
  HINTS
  ${GLOBUSGRIDFTPSERVER_DIR}
  $ENV{GLOBUSGRIDFTPSERVER_DIR}
  /usr
  /usr/local
  /opt/globus
  PATH_SUFFIXES include/globus include/gcc64/
  PATHS /opt
)

FIND_PATH(GLOBUSGRIDFTPSERVER_INCLUDE_DIR2 globus_config.h
  HINTS
  ${GLOBUSGRIDFTPSERVER_DIR}
  $ENV{GLOBUSGRIDFTPSERVER_DIR}
  /usr
  /usr/local
  /usr/lib64
  /opt/globus
  PATH_SUFFIXES globus/include include/globus/ include/gcc64/
  PATHS /opt
)

set(GLOBUSGRIDFTPSERVER_INCLUDE_DIR ${GLOBUSGRIDFTPSERVER_INCLUDE_DIR};${GLOBUSGRIDFTPSERVER_INCLUDE_DIR2})

FIND_LIBRARY(GLOBUSGRIDFTPSERVER_LIB globus_gridftp_server_gcc64
  HINTS
  ${GLOBUSGRIDFTPSERVER_DIR}
  $ENV{GLOBUSGRIDFTPSERVER_DIR}
  /usr
  /usr/local
  /opt/globus
  PATH_SUFFIXES lib
)
FIND_LIBRARY(GLOBUSGRIDFTPSERVER_LIB
  NAMES
  libglobus_gridftp_server.so libglobus_gridftp_server.so.6
  HINTS
  ${GLOBUSGRIDFTPSERVER_DIR}
  $ENV{GLOBUSGRIDFTPSERVER_DIR}
  /usr
  /usr/local
  /opt/globus
  PATH_SUFFIXES lib64
)
GET_FILENAME_COMPONENT( GLOBUSGRIDFTPSERVER_LIB_DIR ${GLOBUSGRIDFTPSERVER_LIB} PATH )

INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(GlobusGridFtpServer DEFAULT_MSG GLOBUSGRIDFTPSERVER_LIB_DIR GLOBUSGRIDFTPSERVER_INCLUDE_DIR )
