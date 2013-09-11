// -----------------------------------------------------------------------------
// File: EosAuthOfs.hh
// Author: Elvin-Alin Sindrilaru <esindril@cern.ch> CERN
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

/*----------------------------------------------------------------------------*/
#include <cstdio>
#include <sstream>
#include <fcntl.h>
#include <syscall.h>
#include <sys/time.h>
/*----------------------------------------------------------------------------*/
#include "EosAuthOfs.hh"
#include "ProtoUtils.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucTrace.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdOss/XrdOssApi.hh"
#include "XrdSec/XrdSecEntity.hh"
/*----------------------------------------------------------------------------*/
#include "google/protobuf/io/zero_copy_stream_impl.h"
/*----------------------------------------------------------------------------*/

// The global OFS handle
eos::auth::EosAuthOfs* eos::auth::gOFS;

extern XrdSysError OfsEroute;
extern XrdOfs* XrdOfsFS;

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
extern "C"
{
  XrdSfsFileSystem* XrdSfsGetFileSystem(XrdSfsFileSystem* native_fs,
                                        XrdSysLogger* lp,
                                        const char* configfn)
  {
    // Do the herald thing
    //
    OfsEroute.SetPrefix("AuthOfs_");
    OfsEroute.logger(lp);
    XrdOucString version = "AuthOfs (Object Storage File System) ";
    version += VERSION;
    OfsEroute.Say("++++++ (c) 2013 CERN/IT-DSS ", version.c_str());
    // Initialize the subsystems
    eos::auth::gOFS = new eos::auth::EosAuthOfs();
    eos::auth::gOFS->ConfigFN = (configfn && *configfn ? strdup(configfn) : 0);

    if (eos::auth::gOFS->Configure(OfsEroute)) return 0;

    XrdOfsFS = eos::auth::gOFS;
    return eos::auth::gOFS;
  }
}

EOSAUTHNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
EosAuthOfs::EosAuthOfs():
  XrdOfs(),
  eos::common::LogId(),
  mSizePoolSocket(5),
  mEosInstance("")
{
  // Initialise the ZMQ client
  mContext = new zmq::context_t(1);
}


//------------------------------------------------------------------------------
// Configure routine
//------------------------------------------------------------------------------
int
EosAuthOfs::Configure(XrdSysError& error)
{
  int NoGo = 0;
  int cfgFD;
  char* var;
  const char* val;
  std::string space_tkn;

  // Configure the basic XrdOfs and exit if not successful
  NoGo = XrdOfs::Configure(error);

  if (NoGo)
  {
    return NoGo;
  }

  // extract the manager from the config file
  XrdOucStream Config(&error, getenv("XRDINSTANCE"));

  // Read in the rucio configuration from the xrd.cf.rucio file
  if (!ConfigFN || !*ConfigFN)
  {
    NoGo = 1;
    error.Emsg("Configure", "no configure file");
  }
  else
  {
    // Try to open the configuration file.
    if ((cfgFD = open(ConfigFN, O_RDONLY, 0)) < 0)
      return error.Emsg("Configure", errno, "open config file fn=", ConfigFN);

    Config.Attach(cfgFD);
    std::string auth_tag = "eosauth.";

    while ((var = Config.GetMyFirstWord()))
    {
      if (!strncmp(var, auth_tag.c_str(), auth_tag.length()))
      {
        var += auth_tag.length();
        
        // Get EOS instance to which we dispatch requests. Note that the port is the one
        // waiting for authentication requests and not the usual one i.e 1094
        std::string option_tag = "instance";

        if (!strncmp(var, option_tag.c_str(), option_tag.length()))
        {
          if (!(val = Config.GetWord()))
            error.Emsg("Configure ", "No EOS instance specified e.g. eosxx.cern.ch:5555");
          else
            mEosInstance = val;
        }

        // Get number of sockets in the pool by default 10
        option_tag = "numsockets";

        if (!strncmp(var, option_tag.c_str(), option_tag.length()))
        {
          if (!(val = Config.GetWord()))
            error.Emsg("Configure ", "No EOS instance specified e.g. eosxx.cern.ch:5555");
          else
            mSizePoolSocket = atoi(val);
        }        
      }
    }

    // Check and connect to the EOS instance
    if (!mEosInstance.empty())
    {
      error.Say("Connecting to the EOS MGM instance: ", mEosInstance.c_str());
      // Create a pool of sockets
      for (int i = 0; i < mSizePoolSocket; i++)
      {
        zmq::socket_t* socket = new zmq::socket_t(*mContext, ZMQ_REQ);
        std::string endpoint = "tcp://";
        endpoint += mEosInstance;
        socket->connect(endpoint.c_str());
        mPoolSocket.push(socket);
      }
    }
    else
    {
      error.Emsg("Configure ", "No EOS instance specified e.g. eosxx.cern.ch:5555");
      NoGo = 1;
    }
  }

  // Set Logging parameters
  XrdOucString unit = "auth@localhost";

  // setup the circular in-memory log buffer
  // TODO: add configuration for the debug level
  eos::common::Logging::Init();
  eos::common::Logging::SetLogPriority(LOG_DEBUG);
  eos::common::Logging::SetUnit(unit.c_str());
  eos_info("info=\"logging configured\"");
  
  return NoGo;

}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
EosAuthOfs::~EosAuthOfs()
{
  zmq::socket_t* socket;

  while (mPoolSocket.try_pop(socket))
    delete socket;
  
  delete mContext;
}


//------------------------------------------------------------------------------
//! Stat method 
//------------------------------------------------------------------------------
int
EosAuthOfs::stat(const char*             path,
                 struct stat*            buf,
                 XrdOucErrInfo&          out_error,
                 const XrdSecEntity*     client,
                 const char*             opaque)
{
  int retc;

  // Get a socket object from the pool
  zmq::socket_t* socket;
  mPoolSocket.wait_pop(socket);
  RequestProto* req_proto = utils::GetStatRequest(path, client, opaque);
     
  if (!SendProtoBufRequest(socket, req_proto))
  {
    OfsEroute.Emsg("stat", "unable to send request");
    return SFS_ERROR;
  }

  ResponseProto* resp_stat =
      static_cast<ResponseProto*>(GetResponse(socket));
  buf = static_cast<struct stat*>(memcpy((void*)buf,
                                         resp_stat->message().c_str(),
                                         sizeof(struct stat)));
  retc = resp_stat->response();
  delete resp_stat;
  delete req_proto;

  // Put back the socket object in the pool
  mPoolSocket.push(socket);
  return retc;
}


//------------------------------------------------------------------------------
// Execute file system command
//------------------------------------------------------------------------------
int
EosAuthOfs::fsctl(const int cmd,
                  const char* args,
                  XrdOucErrInfo& error,
                  const XrdSecEntity* client)
{
  int retc;

  // Get a socket object from the pool
  zmq::socket_t* socket;
  mPoolSocket.wait_pop(socket);
  RequestProto* req_proto = utils::GetFsctlRequest(cmd, args, error, client);
     
  if (!SendProtoBufRequest(socket, req_proto))
  {
    OfsEroute.Emsg("stat", "unable to send request");
    return SFS_ERROR;
  }

  ResponseProto* resp_stat =
      static_cast<ResponseProto*>(GetResponse(socket));

  retc = resp_stat->response();
  delete resp_stat;
  delete req_proto;

  // Put back the socket object in the pool
  mPoolSocket.push(socket);
  return retc;
}






//------------------------------------------------------------------------------
// Send ProtocolBuffer object using ZMQ
//------------------------------------------------------------------------------
bool
EosAuthOfs::SendProtoBufRequest(zmq::socket_t* socket,
                                google::protobuf::Message* message)
{
  // Send the request
  int msg_size = message->ByteSize();
  zmq::message_t request(msg_size);
  google::protobuf::io::ArrayOutputStream aos(request.data(), msg_size); 

  // Use google::protobuf::io::ArrayOutputStream which is way faster than 
  // StringOutputStream as it avoids copying data
  message->SerializeToZeroCopyStream(&aos);
  return socket->send(request);
}


//------------------------------------------------------------------------------
// Get ProtocolBuffer response object using ZMQ
//------------------------------------------------------------------------------
google::protobuf::Message*
EosAuthOfs::GetResponse(zmq::socket_t* socket)
{
  zmq::message_t reply;
  socket->recv(&reply);
  std::string resp_str = std::string(static_cast<char*>(reply.data()), reply.size());
  ResponseProto* resp_stat = new ResponseProto();
  resp_stat->ParseFromString(resp_str);
  return resp_stat;
}

EOSAUTHNAMESPACE_END

