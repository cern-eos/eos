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
#include "ConcurrentQueue.hh"
#include "proto/Request.pb.h"
#include "proto/Response.pb.h"
/*----------------------------------------------------------------------------*/


//------------------------------------------------------------------------------
//! Class EosAuthOfs built on top of XrdOfs
//------------------------------------------------------------------------------
class EosAuthOfs: public XrdOfs
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
             XrdOucErrInfo& out_error,
             const XrdSecEntity* client,
             const char* opaque = 0);


    //--------------------------------------------------------------------------
    //! Execute file system command
    //--------------------------------------------------------------------------
    int fsctl (const int cmd,
               const char* args,
               XrdOucErrInfo& out_error,
               const XrdSecEntity* client);


  private:

    int mSizePoolSocket; ///< maximum size of the socket pool
    zmq::context_t* mContext; ///< ZMQ context 
    ConcurrentQueue<zmq::socket_t*> mPoolSocket; ///< ZMQ socket pool
    std::string mEosInstance; ///< EOS instance to which requests are dispatched

  
    //--------------------------------------------------------------------------
    //! Convert XrdSecEntity object to ProtocolBuffers representation
    //!
    //! @param obj initial object to convert
    //! @param proto ProtocolBuffer representation
    //!
    //--------------------------------------------------------------------------
    void ConvertToProtoBuf(const XrdSecEntity* obj,
                           eos::auth::XrdSecEntityProto*& proto);


    //--------------------------------------------------------------------------
    //! Convert XrdOucErrInfo object to ProtocolBuffers representation
    //!
    //! @param obj initial object to convert
    //! @param proto ProtocolBuffer representation
    //!
    //--------------------------------------------------------------------------
    void ConvertToProtoBuf(XrdOucErrInfo* obj,
                           eos::auth::XrdOucErrInfoProto*& proto);
  

    //--------------------------------------------------------------------------
    //! Convert XrSfsFsctl object to ProtocolBuffers representation
    //!
    //! @param obj initial object to convert
    //! @param proto ProtocolBuffer representation
    //!
    //--------------------------------------------------------------------------
    void ConvertToProtoBuf(const XrdSfsFSctl* client,
                           eos::auth::XrdSfsFSctlProto*& proto);
  
  
    //--------------------------------------------------------------------------
    //! Create stat request ProtocolBuffer object
    //!
    //! @param path file path
    //! @param error client security information obj
    //! @param opaque opaque information
    //!
    //! @return request ProtoBuffer object
    //!
    //--------------------------------------------------------------------------
    eos::auth::RequestProto* GetStatRequest(const char* path,
                                            const XrdSecEntity* client,
                                            const char* opaque);


    //--------------------------------------------------------------------------
    //! Create fsctl request ProtocolBuffer object
    //!
    //! @param path file path
    //! @param error client security information obj
    //! @param opaque opaque information
    //!
    //! @return request ProtoBuffer object
    //!
    //--------------------------------------------------------------------------
    eos::auth::RequestProto* GetFsctlRequest(const int cmd,
                                             const char* args,
                                             XrdOucErrInfo& error,
                                             const XrdSecEntity* client);
    
   
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


#endif //__EOS_EOSAUTHOFS_HH__


