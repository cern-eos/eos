# ----------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Elvin-Alin Sindrilaru <esindril@cern.ch>
# ----------------------------------------------------------------------

# ************************************************************************
# * EOS - the CERN Disk Storage System                                   *
# * Copyright (C) 2011 CERN/Switzerland                                  *
# *                                                                      *
# * This program is free software: you can redistribute it and/or modify *
# * it under the terms of the GNU General Public License as published by *
# * the Free Software Foundation, either version 3 of the License, or    *
# * (at your option) any later version.                                  *
# *                                                                      *
# * This program is distributed in the hope that it will be useful,      *
# * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
# * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
# * GNU General Public License for more details.                         *
# *                                                                      *
# * You should have received a copy of the GNU General Public License    *
# * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
# ************************************************************************

#-------------------------------------------------------------------------------
# Search for dependencies
#-------------------------------------------------------------------------------
option(PACKAGEONLY "Build without dependencies" OFF)
option(CLIENT "Build only client packages" OFF)
option(BUILD_XRDCL_RAIN_PLUGIN "Enable XrdCl RAIN plugin" OFF)

if(NOT PACKAGEONLY)
  set(THREADS_PREFER_PTHREAD_FLAG TRUE)
  find_package(Threads REQUIRED)
  find_package(PythonSitePkg REQUIRED)
  find_package(CURL REQUIRED)
  find_package(XRootD REQUIRED)
  find_package(fuse REQUIRED)
  find_package(Threads REQUIRED)
  find_package(ZLIB REQUIRED)
  find_package(readline REQUIRED)
  find_package(uuid REQUIRED)
  find_package(procps REQUIRED)
  find_package(OpenSSL REQUIRED)
  find_package(ncurses REQUIRED)
  find_package(ZMQ REQUIRED)
  find_package(krb5 REQUIRED)
  find_package(SparseHash REQUIRED)
  find_package(jsoncpp REQUIRED)
  find_package(Libevent REQUIRED)
  find_package(bz2 REQUIRED)
  find_package(absl REQUIRED)
  find_package(fmt REQUIRED)
  find_package(RocksDB REQUIRED)
  find_package(jemalloc)
  find_package(EosGrpcGateway)
  find_package(libmicrohttpd)
  find_package(Sphinx)
  find_package(fuse3)
  find_package(isal_crypto)
  find_package(isal)
  find_package(xxhash)
  find_package(libbfd)
  find_package(davix)
  find_package(Scitokens)
  find_package(Protobuf3 REQUIRED)

  if (Linux)
    # Clang Linux build requires libatomic & special flags for charconv
    if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
      find_package(Atomic REQUIRED)
      find_package(CharConv REQUIRED)
    endif()

    find_package(help2man)
    find_package(glibc REQUIRED)
    find_package(xfs REQUIRED)
    find_package(GRPC REQUIRED)

    if (GRPC_FOUND AND XROOTD_FOUND)
      # Library paths for Protobuf, grpc and xrootd needs to be added to the
      # RPATH of the libraries and binaries built since they are not installed
      # in the usual system location.
      set(CMAKE_SKIP_RPATH FALSE)
      set(CMAKE_SKIP_BUILD_RPATH FALSE)
      # TODO: To be removed in the future when CMAKE properly handles RPATH.
      # Currently without this option the koji builds fail with error:
      # file RPATH_CHANGE could not write new RPATH
      set(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)
      set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
      get_filename_component(EOS_XROOTD_RPATH ${XROOTD_UTILS_LIBRARY} DIRECTORY)
      get_filename_component(EOS_GRPC_RPATH ${GRPC_GRPC++_LIBRARY} DIRECTORY)
      set(CMAKE_INSTALL_RPATH "${EOS_GRPC_RPATH};${EOS_XROOTD_RPATH}")
      message(STATUS "Info CMAKE_INSTALL_RPATH=${CMAKE_INSTALL_RPATH}")
    else()
      message(FATAL_ERROR "One of the mandatory dependecies: GPRC(Protobuf) or XRootD not found")
    endif()
  else ()
    # Add dummy targets for APPLE to simplify the cmake file using these targets
    add_library(GLIBC::DL    INTERFACE IMPORTED)
    add_library(GLIBC::RT    INTERFACE IMPORTED)
    add_library(GLIBC::M     INTERFACE IMPORTED)
  endif()

  # The server build also requires
  if (NOT CLIENT)
    find_package(eosfolly REQUIRED)
    find_package(ldap REQUIRED)
  endif()
else()
  message(STATUS "Running CMake in package only mode.")
  # Fake function for building the SRPMS in build system
  function(PROTOBUF_GENERATE_CPP SRCS HDRS)
    # This is just a hack to be able to run cmake >= 3.11 with -DPACKAGEONLY
    # enabled. Otherwise the protobuf libraries built using add_library will
    # complain as they have no SOURCE files.
    set(${SRCS} "${CMAKE_SOURCE_DIR}/common/Logging.cc" PARENT_SCOPE)
    set(${HDRS} "${CMAKE_SOURCE_DIR}/common/Logging.hh" PARENT_SCOPE)
    return()
  endfunction()

  # Fake targets
  add_library(ZLIB::ZLIB                   UNKNOWN IMPORTED)
  add_library(UUID::UUID                   UNKNOWN IMPORTED)
  add_library(PROCPS::PROCPS               UNKNOWN IMPORTED)
  add_library(XROOTD::SERVER               UNKNOWN IMPORTED)
  add_library(XROOTD::CL                   UNKNOWN IMPORTED)
  add_library(XROOTD::SSI                  UNKNOWN IMPORTED)
  add_library(XROOTD::HTTP                 UNKNOWN IMPORTED)
  add_library(XROOTD::UTILS                UNKNOWN IMPORTED)
  add_library(XROOTD::POSIX                UNKNOWN IMPORTED)
  add_library(XROOTD::PRIVATE              UNKNOWN IMPORTED)
  add_library(PROTOBUF::PROTOBUF           UNKNOWN IMPORTED)
  add_library(NCURSES::NCURSES             UNKNOWN IMPORTED)
  add_library(NCURSES::NCURSES_STATIC      UNKNOWN IMPORTED)
  add_library(READLINE::READLINE           UNKNOWN IMPORTED)
  add_library(JSONCPP::JSONCPP             UNKNOWN IMPORTED)
  add_library(FOLLY::FOLLY                 UNKNOWN IMPORTED)
  add_library(ZMQ::ZMQ                     UNKNOWN IMPORTED)
  add_library(KRB5::KRB5                   UNKNOWN IMPORTED)
  add_library(OpenSSL::SSL                 UNKNOWN IMPORTED)
  add_library(OpenSSL::Crypto              UNKNOWN IMPORTED)
  add_library(LDAP::LDAP                   UNKNOWN IMPORTED)
  add_library(GRPC::grpc                   UNKNOWN IMPORTED)
  add_library(GRPC::grpc++                 UNKNOWN IMPORTED)
  add_library(GRPC::grpc++_reflection      UNKNOWN IMPORTED)
  add_library(LIBMICROHTTPD::LIBMICROHTTPD UNKNOWN IMPORTED)
  add_library(CURL::libcurl                UNKNOWN IMPORTED)
  add_library(ATOMIC::ATOMIC               UNKNOWN IMPORTED)
  add_library(LIBEVENT::LIBEVENT           UNKNOWN IMPORTED)
  add_library(FUSE::FUSE                   UNKNOWN IMPORTED)
  add_library(FUSE3::FUSE3                 UNKNOWN IMPORTED)
  add_library(GLIBC::DL                    UNKNOWN IMPORTED)
  add_library(GLIBC::RT                    UNKNOWN IMPORTED)
  add_library(GLIBC::M                     UNKNOWN IMPORTED)
  add_library(LIBBFD::LIBBFD               UNKNOWN IMPORTED)
  add_library(LIBBFD::IBERTY               UNKNOWN IMPORTED)
  add_library(RICHACL::RICHACL             UNKNOWN IMPORTED)
  add_library(DAVIX::DAVIX                 UNKNOWN IMPORTED)
  add_library(ROCKSDB::ROCKSDB             UNKNOWN IMPORTED)
  add_library(SCITOKENS::SCITOKENS         UNKNOWN IMPORTED)
  add_library(ABSL::ABSL                   UNKNOWN IMPORTED)
  add_library(BZ2::BZ2                     UNKNOWN IMPORTED)
  add_library(ZSTD::ZSTD                   UNKNOWN IMPORTED)
  add_library(LZ4::LZ4                     UNKNOWN IMPORTED)
  add_library(Snappy::snappy               UNKNOWN IMPORTED)
  add_library(XFS::XFS                     INTERFACE IMPORTED)
  add_library(GOOGLE::SPARSEHASH           INTERFACE IMPORTED)
  add_library(ISAL::ISAL                   STATIC IMPORTED)
  add_library(ISAL::ISAL_CRYPTO            STATIC IMPORTED)
  add_library(XXHASH::XXHASH               STATIC IMPORTED)
  add_library(JEMALLOC::JEMALLOC           UNKNOWN IMPORTED)
  add_library(CHARCONV::CHARCONV           INTERFACE IMPORTED)
  add_library(EosGrpcGateway::EosGrpcGateway UNKNOWN IMPORTED)
  add_library(fmt::fmt-header-only         UNKNOWN IMPORTED)
endif()
