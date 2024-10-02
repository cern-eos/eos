// -----------------------------------------------------------------------------
// File: EosAuthOfs.hh
// Author: Elvin-Alin Sindrilaru - CERN
// -----------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2013 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#ifndef __EOSAUTH_OFS_HH__
#define __EOSAUTH_OFS_HH__

#include <XrdOfs/XrdOfs.hh>
#include "Namespace.hh"
#include "common/ConcurrentQueue.hh"
#include <zmq.hpp>
#include <string>

//! Forward declaration
class EosAuthOfsDirectory;
class EosAuthOfsFile;

namespace eos
{
namespace auth
{
  class ResponseProto;
}
}

namespace google
{
namespace protobuf
{
class Message;
}
}

EOSAUTHNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class EosAuthOfs built on top of XrdOfs
/*! Decription: The libEosAuthOfs.so is inteded to be used as an OFS library
    plugin with a vanilla XRootD server. What it does is to connect using ZMQ
    sockets to onel MGM node.
    a slave MGM). The endpoint is read from the configuration file, by default
    it will connect to localhost:1094 !
    The EosAuthOfs plugin
    then tries to replay all the requests it receives from the clients to the
    master MGM node. It does this by marshalling the request and identity of the
    client using ProtocolBuffers and sends this request using ZMQ to the configured
    MGM node.

    There are several tunable parameters for this configuration (auth + MGMs):

    AUTH - configuration
    ====================
    - eosauth.mgm - contain the hostname and the
        port to which ZMQ will connect so that it can forward
        requests and receive responses. 
    - eosauth.numsockets - once a clients wants to send a request the thread
        allocated to him in XRootD will require a socket to send the request
        to the MGM node. Therefore, we set up a pool of sockets from the
        begining which can be used to send/receiver requests/responses.
        The default size is 10 sockets.

    MGM - configuration
    ===================
    - mgmofs.auththreads - since we now receive requests using ZMQ, we no longer
        use the default thread pool from XRootD and we need threads for dealing
        with the requests. This parameter sets the thread pool size when starting
        the MGM node.
    - mgmofs.authport - this is the endpoint where the MGM listens for ZMQ
        requests from any EosAuthOfs plugins. This port needs to be opened also
        in the firewall.

    - mgmofs.localhost true|false - by default the ZMQ endpoint will listen on 
        all interfaces, but often the front-end will run on the same node and
        for security we want only to have localhost connections

    In case of a master <=> slave switch the EosAuthOfs plugin adapts
    automatically based on the information provided by the slave MGM which
    should redirect all clients with write requests to the master node. Care
    should be taken when specifying the two endpoints since the switch is done
    ONLY IF the redirection HOST matches one of the two endpoints specified in
    the configuration  of the authentication plugin (namely eosauth.instance).
    Once the switch is done all requests be them read or write are sent to the
    new master MGM node.
*/
//------------------------------------------------------------------------------
class EosAuthOfs: public XrdOfs, public eos::common::LogId
{
  friend class EosAuthOfsDirectory;
  friend class EosAuthOfsFile;

public:
  //--------------------------------------------------------------------------
  //! Constuctor
  //--------------------------------------------------------------------------
  EosAuthOfs();

  //--------------------------------------------------------------------------
  //! Destructor
  //--------------------------------------------------------------------------
  virtual ~EosAuthOfs();

  //--------------------------------------------------------------------------
  //! Configure routine
  //--------------------------------------------------------------------------
  virtual int Configure(XrdSysError& error, XrdOucEnv* envP);

  //--------------------------------------------------------------------------
  //! Get directory object
  //--------------------------------------------------------------------------
  XrdSfsDirectory* newDir(char* user = 0, int MonID = 0);

  //--------------------------------------------------------------------------
  // Get file object
  //--------------------------------------------------------------------------
  XrdSfsFile* newFile(char* user = 0, int MonID = 0);

  //--------------------------------------------------------------------------
  //! Stat function
  //--------------------------------------------------------------------------
  int stat(const char* path,
           struct stat* buf,
           XrdOucErrInfo& error,
           const XrdSecEntity* client,
           const char* opaque = 0);

  //--------------------------------------------------------------------------
  //! Stat function to retrieve mode
  //--------------------------------------------------------------------------
  int stat(const char* name,
           mode_t& mode,
           XrdOucErrInfo& out_error,
           const XrdSecEntity* client,
           const char* opaque = 0);

  //--------------------------------------------------------------------------
  //! Execute file system command !!! fsctl !!!
  //--------------------------------------------------------------------------
  int fsctl(const int cmd,
            const char* args,
            XrdOucErrInfo& out_error,
            const XrdSecEntity* client);

  //--------------------------------------------------------------------------
  //! Execute file system command !!! FSctl !!!
  //--------------------------------------------------------------------------
  int FSctl(const int cmd,
            XrdSfsFSctl& args,
            XrdOucErrInfo& error,
            const XrdSecEntity* client = 0);

  //--------------------------------------------------------------------------
  //! Chmod function
  //--------------------------------------------------------------------------
  int chmod(const char* path,
            XrdSfsMode mopde,
            XrdOucErrInfo& error,
            const XrdSecEntity* client,
            const char* opaque = 0);

  //--------------------------------------------------------------------------
  //! Chksum function
  //--------------------------------------------------------------------------
  int chksum(csFunc func,
             const char* csName,
             const char* path,
             XrdOucErrInfo& error,
             const XrdSecEntity* client = 0,
             const char* opaque = 0);

  //--------------------------------------------------------------------------
  //! Exists function
  //--------------------------------------------------------------------------
  int exists(const char* path,
             XrdSfsFileExistence& exists_flag,
             XrdOucErrInfo& error,
             const XrdSecEntity* client,
             const char* opaque = 0);

  //--------------------------------------------------------------------------
  //! Create directory
  //--------------------------------------------------------------------------
  int mkdir(const char* dirName,
            XrdSfsMode Mode,
            XrdOucErrInfo& out_error,
            const XrdSecEntity* client,
            const char* opaque = 0);

  //--------------------------------------------------------------------------
  //! Remove directory
  //--------------------------------------------------------------------------
  int remdir(const char* path,
             XrdOucErrInfo& error,
             const XrdSecEntity* client,
             const char* opaque = 0);

  //--------------------------------------------------------------------------
  //! Rem file
  //--------------------------------------------------------------------------
  int rem(const char* path,
          XrdOucErrInfo& error,
          const XrdSecEntity* client,
          const char* opaque = 0);

  //--------------------------------------------------------------------------
  //! Rename file
  //--------------------------------------------------------------------------
  int rename(const char* oldName,
             const char* newName,
             XrdOucErrInfo& error,
             const XrdSecEntity* client,
             const char* opaqueO = 0,
             const char* opaqueN = 0);

  //--------------------------------------------------------------------------
  //! Prepare request
  //--------------------------------------------------------------------------
  int prepare(XrdSfsPrep& pargs,
              XrdOucErrInfo& error,
              const XrdSecEntity* client = 0);

  //--------------------------------------------------------------------------
  //! Truncate file
  //--------------------------------------------------------------------------
  int truncate(const char* path,
               XrdSfsFileOffset fileOffset,
               XrdOucErrInfo& error,
               const XrdSecEntity* client = 0,
               const char* opaque = 0);

  //--------------------------------------------------------------------------
  //! getStats function - fake an ok response HERE i.e. do not build and sent
  //! a request to the real MGM
  //--------------------------------------------------------------------------
  int getStats(char* buff, int blen);

  //--------------------------------------------------------------------------
  //! Process a proto error response and configure 
  //! a collpasing redirection if requested/possible
  //--------------------------------------------------------------------------
  void ProcessError(eos::auth::ResponseProto* resp_func, XrdOucErrInfo& error, const char* path);

private:

  pthread_t proxy_tid; ///< id of the proxy thread
  zmq::context_t* mZmqContext; ///< ZMQ context
  zmq::socket_t* mFrontend; ///< proxy socket facing the clients
  XrdSysMutex mMutexMaster;
  int mSizePoolSocket; ///< maximum size of the client socket pool
  eos::common::ConcurrentQueue<zmq::socket_t*>
  mPoolSocket; ///< ZMQ client socket pool
  ///! MGM endpoints to which requests can be dispatched and the corresponding sockets
  std::pair<std::string, zmq::socket_t*> mBackend;
  std::string mManagerIp; ///< auth ip address
  int mPort;   ///< port on which the current auth server runs
  int mCollapsePort; ///< port to which a redirect gets collapsed on
  int mLogLevel; ///< log level value 0 -7 (LOG_EMERG - LOG_DEBUG)

  //--------------------------------------------------------------------------
  //! Authentication proxy thread which forwards requests form the clients
  //! to the proper MGM intance.
  //--------------------------------------------------------------------------
  void AuthProxyThread();

  //--------------------------------------------------------------------------
  //! Authentication proxy thread startup function
  //--------------------------------------------------------------------------
  static void* StartAuthProxyThread(void* pp);

  //--------------------------------------------------------------------------
  //! Send ProtocolBuffer object using ZMQ
  //!
  //! @param socket ZMQ socket object
  //! @param object to be sent over the wire
  //!
  //! @return true if object sent successfully, otherwise false
  //!
  //--------------------------------------------------------------------------
  bool SendProtoBufRequest(zmq::socket_t* socket,
                           google::protobuf::Message* message);

  //--------------------------------------------------------------------------
  //! Get ProtocolBuffer reply object using ZMQ
  //!
  //! @param socket ZMQ socket object
  //!
  //! @return pointer to received object, the user has the responsibility to
  //!         delete the obtained object
  //!
  //--------------------------------------------------------------------------
  google::protobuf::Message* GetResponse(zmq::socket_t*& socket);

  //--------------------------------------------------------------------------
};

//------------------------------------------------------------------------------
//! Global OFS object
//------------------------------------------------------------------------------
extern EosAuthOfs* gOFS;
EOSAUTHNAMESPACE_END
#endif //__EOSAUTH_OFS_HH__
