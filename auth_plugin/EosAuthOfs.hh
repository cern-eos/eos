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

/*----------------------------------------------------------------------------*/
#include "XrdOfs/XrdOfs.hh"
#include <string>
/*----------------------------------------------------------------------------*/
#include "zmq.hpp"
/*----------------------------------------------------------------------------*/
#include "common/ConcurrentQueue.hh"
#include "Namespace.hh"
/*----------------------------------------------------------------------------*/

//! Forward declaration
class EosAuthOfsDirectory;
class EosAuthOfsFile;

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
    plugin with a vanilla XRootD server. What is does it to connect using ZMQ
    sockets to the real MGM nodes (in general it should connect to a master and
    a slave MGM). It does this by reading out the endpoints it needs to connect
    to from the configuration file (/etc/xrd.cf.auth). These need to follow the
    format: "host:port" and the first one should be the endpoint corresponding
    to the master MGM and the second one to the slave MGM. The EosAuthOfs plugin
    then tries to replay all the requests it receives from the clients to the
    master MGM node. It does this by marshalling the request and identity of the
    client using ProtocolBuffers and sends this request using ZMQ to the master
    MGM node.

    There are reveral tunable parameters for this configuration (auth + MGMs):

    AUTH - configuration
    ====================
    - eosauth.instance - hostnames and the ports to which ZMQ can connect to 
        the MGM nodes so that it can forward requests and receive responses
    - eosauth.numsockets - once a clients wants to send a request the thread
        allocated to him in XRootD will require a socket to send the request
        to the MGM node. Therefore, we establish a pool of sockets from the
        begining which can be used to send/receiver requests/responses.
        Default is 10 sockets.

    MGM - configuration
    ===================
    - mgmofs.auththreads - since we now receive requests using ZMQ, we no longer
        use the default thread pool from XRootD and we need threads for dealing
        with the requests. This parameter sets the thread pool size when starting
        the MGM node.
    - mgmofs.authport - this is the endpoint where the MGM listens for ZMQ
        requests from any EosAuthOfs plugins. This port needs to be opened also
        in the firewall.

    In case of a master <=> slave switch the EosAuthOfs plugin adapts
    automatically based on the information provided by the slave MGM which
    should redirect all clients with write requests to the master node. Care
    should be taken when specifying the two endpoints since the switch is done
    ONLY IF the redirection HOST matches one of the two endpoints specified in
    the config of the authentication plugin.
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
    virtual int Configure(XrdSysError& error);


    //--------------------------------------------------------------------------
    //! Get directory object
    //--------------------------------------------------------------------------
    XrdSfsDirectory* newDir (char *user = 0, int MonID = 0);

  
    //--------------------------------------------------------------------------
    // Get file object
    //--------------------------------------------------------------------------
    XrdSfsFile* newFile (char *user = 0, int MonID = 0);


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
    int mkdir (const char *dirName,
               XrdSfsMode Mode,
               XrdOucErrInfo &out_error,
               const XrdSecEntity *client,
               const char *opaque = 0);

  
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
     int rename (const char *oldName,
                 const char *newName,
                 XrdOucErrInfo &error,
                 const XrdSecEntity *client,
                 const char *opaqueO = 0,
                 const char *opaqueN = 0);
  
  
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
    int getStats (char *buff, int blen);
 
  
  private:

    pthread_t proxy_tid; ///< id of the proxy thread
    zmq::context_t* mZmqContext; ///< ZMQ context
    zmq::socket_t* mFrontend; ///< proxy socket facing the clients
    zmq::socket_t* mMaster; ///< socket pointing to the MGM master
    XrdSysMutex mMutexMaster; ///< mutex for switching the MGM master
    int mSizePoolSocket; ///< maximum size of the client socket pool
    eos::common::ConcurrentQueue<zmq::socket_t*> mPoolSocket; ///< ZMQ client socket pool
    ///! MGM endpoints to which requests can be dispatched and the corresponding sockets
    std::vector< std::pair<std::string, zmq::socket_t*> > mBackend; 
    std::string mManagerIp; ///< the IP address of the auth instance
    int mManagerPort;   ///< port on which the current auth server runs
    int mLogLevel; ///< log level value 0 -7 (LOG_EMERG - LOG_DEBUG)


    //--------------------------------------------------------------------------
    //! Authentication proxy thread which forwards requests form the clients
    //! to the proper MGM intance.
    //--------------------------------------------------------------------------
    void AuthProxyThread();


    //--------------------------------------------------------------------------
    //! Authentication proxy thread startup function
    //--------------------------------------------------------------------------
    static void* StartAuthProxyThread(void *pp);
  

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
    google::protobuf::Message* GetResponse(zmq::socket_t* socket);


    //--------------------------------------------------------------------------
    //! Update the socket pointing to the master MGM instance
    //!
    //! @param new_master new host and port values for the master MGM
    //!                   the format is: "host:port"
    //!
    //! @return true if update was successful, false otherwise
    //!
    //--------------------------------------------------------------------------
    bool UpdateMaster(std::string& new_master);

  
};


//------------------------------------------------------------------------------
//! Global OFS object
//------------------------------------------------------------------------------
extern EosAuthOfs* gOFS;

EOSAUTHNAMESPACE_END

#endif //__EOSAUTH_OFS_HH__


