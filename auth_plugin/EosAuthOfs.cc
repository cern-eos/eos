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

/*----------------------------------------------------------------------------*/
#include <cstdio>
#include <sstream>
#include <fcntl.h>
#include <syscall.h>
#include <sys/time.h>
/*----------------------------------------------------------------------------*/
#include "EosAuthOfs.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucTrace.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdOss/XrdOssApi.hh"
#include "XrdSec/XrdSecEntity.hh"
/*----------------------------------------------------------------------------*/
#include "google/protobuf/io/zero_copy_stream_impl.h"
/*----------------------------------------------------------------------------*/

// The global OFS handle
EosAuthOfs* gOFS;

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
    gOFS = new EosAuthOfs();
    gOFS->ConfigFN = (configfn && *configfn ? strdup(configfn) : 0);

    if (gOFS->Configure(OfsEroute)) return 0;

    XrdOfsFS = gOFS;
    return gOFS;
  }
}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
EosAuthOfs::EosAuthOfs():
  XrdOfs(),
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
  eos::auth::RequestProto* req_proto = GetStatRequest(path, client, opaque);
     
  if (!SendProtoBufRequest(socket, req_proto))
  {
    OfsEroute.Emsg("stat", "unable to send request");
    return SFS_ERROR;
  }

  eos::auth::ResponseProto* resp_stat =
    static_cast<eos::auth::ResponseProto*>(GetResponse(socket));
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
  eos::auth::RequestProto* req_proto = GetFsctlRequest(cmd, args, error, client);
     
  if (!SendProtoBufRequest(socket, req_proto))
  {
    OfsEroute.Emsg("stat", "unable to send request");
    return SFS_ERROR;
  }

  eos::auth::ResponseProto* resp_stat =
    static_cast<eos::auth::ResponseProto*>(GetResponse(socket));

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
  eos::auth::ResponseProto* resp_stat = new eos::auth::ResponseProto();
  resp_stat->ParseFromString(resp_str);
  return resp_stat;
}


//------------------------------------------------------------------------------
// Convert XrdSecEntity object to ProtocolBuffers representation
//------------------------------------------------------------------------------
void
EosAuthOfs::ConvertToProtoBuf(const XrdSecEntity* obj,
                              eos::auth::XrdSecEntityProto*& proto)
{
  proto->set_prot(obj->prot);

  if (obj->name)
    proto->set_name(obj->name);
  else
    proto->set_name("");

  if (obj->host)
    proto->set_host(obj->host);
  else
    proto->set_host("");

  if (obj->vorg)
    proto->set_vorg(obj->vorg);
  else
    proto->set_vorg("");

  if (obj->role)
    proto->set_role(obj->role);
  else
    proto->set_role("");

  if (obj->grps)
    proto->set_grps(obj->grps);
  else
    proto->set_grps("");

  if (obj->endorsements)
     proto->set_endorsements(obj->endorsements);
  else
    proto->set_endorsements("");

  if (obj->creds)
    proto->set_creds(obj->creds);
  else
    proto->set_creds("");
  
  proto->set_credslen(obj->credslen);

  if (obj->moninfo)
    proto->set_moninfo(obj->moninfo);
  else
    proto->set_moninfo("");

  if (obj->tident)
    proto->set_tident(obj->tident);
  else
    proto->set_tident("");
}


//------------------------------------------------------------------------------
// Convert XrdOucErrInfo object to ProtocolBuffers representation
//------------------------------------------------------------------------------
void
EosAuthOfs::ConvertToProtoBuf(XrdOucErrInfo* obj,
                              eos::auth::XrdOucErrInfoProto*& proto)
{
  proto->set_user(obj->getErrUser());
  proto->set_code(obj->getErrInfo());
  proto->set_message(obj->getErrText());
}


//------------------------------------------------------------------------------
// Convert XrSfsFSctl object to ProtocolBuffers representation
//------------------------------------------------------------------------------
void
EosAuthOfs::ConvertToProtoBuf(const XrdSfsFSctl* obj,
                              eos::auth::XrdSfsFSctlProto*& proto)
{
  proto->set_arg1(obj->Arg1);
  proto->set_arg1len(obj->Arg1Len);
  proto->set_arg2len(obj->Arg2Len);
  proto->set_arg2(obj->Arg2);
}


//------------------------------------------------------------------------------
// Create StatProto object
//------------------------------------------------------------------------------
eos::auth::RequestProto*
EosAuthOfs::GetStatRequest(const char* path,
                           const XrdSecEntity* error,
                           const char* opaque)
{
  eos::auth::RequestProto* req_proto = new eos::auth::RequestProto();
  eos::auth::StatProto* stat_proto = req_proto->mutable_stat();
  eos::auth::XrdSecEntityProto* error_proto = stat_proto->mutable_error();
  
  ConvertToProtoBuf(error, error_proto);
  stat_proto->set_path(path);

  if (opaque)
    stat_proto->set_opaque(opaque);
  else
    stat_proto->set_opaque("");

  req_proto->set_type(eos::auth::RequestProto_OperationType_STAT);
  return req_proto;
}


//------------------------------------------------------------------------------
// Create fsctl request ProtocolBuffer object
//------------------------------------------------------------------------------
eos::auth::RequestProto*
EosAuthOfs::GetFsctlRequest(const int cmd,
                            const char* args,
                            XrdOucErrInfo& error,
                            const XrdSecEntity* client)
{
  eos::auth::RequestProto* req_proto = new eos::auth::RequestProto();
  eos::auth::FsctlProto* fsctl_proto = req_proto->mutable_fsctl1();
  eos::auth::XrdOucErrInfoProto* xoei_proto = fsctl_proto->mutable_error();
  eos::auth::XrdSecEntityProto* xse_proto = fsctl_proto->mutable_client();

  fsctl_proto->set_args(args);
  ConvertToProtoBuf(&error, xoei_proto);
  ConvertToProtoBuf(client, xse_proto);
  req_proto->set_type(eos::auth::RequestProto_OperationType_FSCTL);
  return req_proto;
}

