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

#ifndef __EOS_EOSAUTHOFS_HH__
#define __EOS_EOSAUTHOFS_HH__

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
//------------------------------------------------------------------------------
class EosAuthOfs: public XrdOfs, public eos::common::LogId
{
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
    //! Execute file system command
    //--------------------------------------------------------------------------
    int fsctl(const int cmd,
              const char* args,
              XrdOucErrInfo& out_error,
              const XrdSecEntity* client);


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
  
  private:

    int mSizePoolSocket; ///< maximum size of the socket pool
    zmq::context_t* mContext; ///< ZMQ context
    eos::common::ConcurrentQueue<zmq::socket_t*> mPoolSocket; ///< ZMQ socket pool
    std::string mEosInstance; ///< EOS instance to which requests are dispatched


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
};


//------------------------------------------------------------------------------
//! Global OFS object
//------------------------------------------------------------------------------
extern EosAuthOfs* gOFS;

EOSAUTHNAMESPACE_END

#endif //__EOS_EOSAUTHOFS_HH__


