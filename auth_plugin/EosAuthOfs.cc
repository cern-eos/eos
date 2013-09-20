//------------------------------------------------------------------------------
// File: EosAuthOfs.cc
// Author: Elvin-Alin Sindrilaru <esindril@cern.ch> CERN
//------------------------------------------------------------------------------

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
#include "EosAuthOfsDirectory.hh"
#include "EosAuthOfsFile.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucTrace.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdOss/XrdOssApi.hh"
#include "XrdSec/XrdSecEntity.hh"
#include "XrdSys/XrdSysDNS.hh"
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
  mEosInstance(""),
  mLogLevel(LOG_INFO)
{
  // Initialise the ZMQ client
  mContext = new zmq::context_t(1);
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

  unsigned int ip = 0;

  if (XrdSysDNS::Host2IP(HostName, &ip))
  {
    char buff[1024];
    XrdSysDNS::IP2String(ip, 0, buff, 1024);
    mManagerIp = buff;
    mManagerPort = myPort;
  }
  else
  {
    return OfsEroute.Emsg("Config", errno, "convert hostname to IP address", HostName);
  }

  // extract the manager from the config file
  XrdOucStream Config(&error, getenv("XRDINSTANCE"));

  // Read in the auth configuration from the xrd.cf.auth file
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
        
        // Get log level by default LOG_INFO
        option_tag = "loglevel";

        if (!strncmp(var,option_tag.c_str(), option_tag.length()))
        {
          if (!(val = Config.GetWord()))
          {
            error.Emsg("Config", "argument for debug level invalid set to ERR.");
            mLogLevel = LOG_INFO;
          }
          else
          {
            std::string str_val(val);
            
            if (isdigit(str_val[0]))
            {
              // The level is given as a number
              mLogLevel = atoi(val);
            }
            else
            {
              // The level is given as a string
              mLogLevel = eos::common::Logging::GetPriorityByString(val);
            }
            
            error.Say("=====> eosauth.loglevel: ",
                       eos::common::Logging::GetPriorityString(mLogLevel), "");
          }
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
  eos::common::Logging::SetLogPriority(mLogLevel);
  eos::common::Logging::SetUnit(unit.c_str());
  eos_info("info=\"logging configured\"");
  return NoGo;
}



//------------------------------------------------------------------------------
// Get directory object
//------------------------------------------------------------------------------
XrdSfsDirectory*
EosAuthOfs::newDir(char *user, int MonID)
{
  return static_cast<XrdSfsDirectory*>(new EosAuthOfsDirectory(user, MonID));
}

  
//------------------------------------------------------------------------------
// Get file object
//------------------------------------------------------------------------------
XrdSfsFile*
EosAuthOfs::newFile(char *user, int MonID)
{
  return static_cast<XrdSfsFile*>(new EosAuthOfsFile(user, MonID));
}


//------------------------------------------------------------------------------
//! Stat method
//------------------------------------------------------------------------------
int
EosAuthOfs::stat(const char* path,
                 struct stat* buf,
                 XrdOucErrInfo& error,
                 const XrdSecEntity* client,
                 const char* opaque)
{
  int retc;
  eos_debug("stat path=%s", path);

  // Get a socket object from the pool
  zmq::socket_t* socket;
  mPoolSocket.wait_pop(socket);
  RequestProto* req_proto = utils::GetStatRequest(RequestProto_OperationType_STAT,
                                                  path, error, client, opaque);

  if (!SendProtoBufRequest(socket, req_proto))
  {
    OfsEroute.Emsg("stat", "unable to send request");
    return SFS_ERROR;
  }

  ResponseProto* resp_stat = static_cast<ResponseProto*>(GetResponse(socket));
  retc = resp_stat->response();

  if (resp_stat->has_error())
  {
    error.setErrInfo(resp_stat->error().code(),
                     resp_stat->error().message().c_str());
  }

  // We retrieve the struct stat if response is ok
  if ((retc == SFS_OK) && resp_stat->has_message())
  {
    buf = static_cast<struct stat*>(memcpy((void*)buf,
                                           resp_stat->message().c_str(),
                                           sizeof(struct stat)));
  }

  delete resp_stat;
  delete req_proto;

  // Put back the socket object in the pool
  mPoolSocket.push(socket);
  return retc;
}


//--------------------------------------------------------------------------
// Stat function to retrieve mode
//--------------------------------------------------------------------------
int
EosAuthOfs::stat(const char* path,
                 mode_t& mode,
                 XrdOucErrInfo& error,
                 const XrdSecEntity* client,
                 const char* opaque)
{
  int retc;
  eos_debug("statm path=%s", path);

  // Get a socket object from the pool
  zmq::socket_t* socket;
  mPoolSocket.wait_pop(socket);
  RequestProto* req_proto = utils::GetStatRequest(RequestProto_OperationType_STATM,
                                                  path, error, client, opaque);
     
  if (!SendProtoBufRequest(socket, req_proto))
  {
    OfsEroute.Emsg("statm", "unable to send request");
    return SFS_ERROR;
  }

  ResponseProto* resp_stat = static_cast<ResponseProto*>(GetResponse(socket));
  retc = resp_stat->response();

  if (resp_stat->has_error())
  {
    error.setErrInfo(resp_stat->error().code(),
                     resp_stat->error().message().c_str());
  }
  
  // We retrieve the open mode if response if ok
  if ((retc == SFS_OK) && resp_stat->has_message())
    memcpy((void*)&mode, resp_stat->message().c_str(), sizeof(mode_t));
  
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
  eos_debug("fsctl with cmd=%i, args=%s", cmd, args);
  int opcode = cmd & SFS_FSCTL_CMD;

  // For the server configuration query we asnwer with the information of the
  // authentication XRootD server i.e. don't frw it to the real MGM.
  if (opcode == SFS_FSCTL_LOCATE)
  {
    char locResp[4096];
    char rType[3], *Resp[] = {rType, locResp};
    rType[0] = 'S';
    // don't manage writes via global redirection - therefore we mark the files as 'r'
    rType[1] = 'r';
    rType[2] = '\0';
    sprintf(locResp, "[::%s]:%d ", (char*) gOFS->mManagerIp.c_str(), gOFS->mManagerPort);
    error.setErrInfo(strlen(locResp) + 3, (const char **) Resp, 2);
    return SFS_DATA;
  }
  
  // Get a socket object from the pool
  zmq::socket_t* socket;
  mPoolSocket.wait_pop(socket);
  RequestProto* req_proto = utils::GetFsctlRequest(cmd, args, error, client);
     
  if (!SendProtoBufRequest(socket, req_proto))
  {
    OfsEroute.Emsg("fsctl", "unable to send request");
    return SFS_ERROR;
  }

  ResponseProto* resp_fsctl1 = static_cast<ResponseProto*>(GetResponse(socket));
  retc = resp_fsctl1->response();

  if (resp_fsctl1->has_error())
  {
    error.setErrInfo(resp_fsctl1->error().code(),
                     resp_fsctl1->error().message().c_str());
  }

  delete resp_fsctl1;
  delete req_proto;

  // Put back the socket object in the pool
  mPoolSocket.push(socket);
  return retc;
}


//------------------------------------------------------------------------------
// Execute file system command !!! FSctl !!!
//------------------------------------------------------------------------------
int
EosAuthOfs::FSctl(const int cmd,
                  XrdSfsFSctl& args,
                  XrdOucErrInfo& error,
                  const XrdSecEntity* client)
{
  int retc;
  eos_debug("FSctl with cmd=%i", cmd);

  // Get a socket object from the pool
  zmq::socket_t* socket;
  mPoolSocket.wait_pop(socket);
  RequestProto* req_proto = utils::GetFSctlRequest(cmd, args, error, client);
     
  if (!SendProtoBufRequest(socket, req_proto))
  {
    OfsEroute.Emsg("FSctl", "unable to send request");
    return SFS_ERROR;
  }

  ResponseProto* resp_fsctl2 = static_cast<ResponseProto*>(GetResponse(socket));
  retc = resp_fsctl2->response();

  if (resp_fsctl2->has_error())
  {
    error.setErrInfo(resp_fsctl2->error().code(),
                     resp_fsctl2->error().message().c_str());
  }

  delete resp_fsctl2;
  delete req_proto;

  // Put back the socket object in the pool
  mPoolSocket.push(socket);
  return retc;
}


//------------------------------------------------------------------------------
// Chmod by client
//------------------------------------------------------------------------------
int
EosAuthOfs::chmod (const char *path,
                   XrdSfsMode mode,
                   XrdOucErrInfo &error,
                   const XrdSecEntity *client,
                   const char *opaque)
{
  int retc;
  eos_debug("chmod path=%s mode=%i", path, mode);

  // Get a socket object from the pool
  zmq::socket_t* socket;
  mPoolSocket.wait_pop(socket);
  RequestProto* req_proto = utils::GetChmodRequest(path, mode, error, client, opaque);
     
  if (!SendProtoBufRequest(socket, req_proto))
  {
    OfsEroute.Emsg("chmod", "unable to send request");
    return SFS_ERROR;
  }

  ResponseProto* resp_chmod = static_cast<ResponseProto*>(GetResponse(socket));
  retc = resp_chmod->response();

  if (resp_chmod->has_error())
  {
    error.setErrInfo(resp_chmod->error().code(),
                     resp_chmod->error().message().c_str());
  }
  
  delete resp_chmod;
  delete req_proto;

  // Put back the socket object in the pool
  mPoolSocket.push(socket);
  return retc;
}


//------------------------------------------------------------------------------
// Chksum by client
//------------------------------------------------------------------------------
int
EosAuthOfs::chksum(csFunc func,
                   const char* csName,
                   const char* path,
                   XrdOucErrInfo& error,
                   const XrdSecEntity* client,
                   const char* opaque)
{
  int retc;
  eos_debug("chksum path=%s csName=%s", path, csName);

  // Get a socket object from the pool
  zmq::socket_t* socket;
  mPoolSocket.wait_pop(socket);
  RequestProto* req_proto = utils::GetChksumRequest(func, csName, path, error,
                                                    client, opaque);
     
  if (!SendProtoBufRequest(socket, req_proto))
  {
    OfsEroute.Emsg("chksum", "unable to send request");
    return SFS_ERROR;
  }

  ResponseProto* resp_chksum = static_cast<ResponseProto*>(GetResponse(socket));
  retc = resp_chksum->response();
  eos_debug("chksum retc=%i", retc);
  
  if (resp_chksum->has_error())
  {
    error.setErrInfo(resp_chksum->error().code(),
                     resp_chksum->error().message().c_str());
  }
  
  delete resp_chksum;
  delete req_proto;

  // Put back the socket object in the pool
  mPoolSocket.push(socket);
  return retc;
}


//--------------------------------------------------------------------------
// Exists function
//--------------------------------------------------------------------------
int
EosAuthOfs::exists(const char* path,
                   XrdSfsFileExistence& exists_flag,
                   XrdOucErrInfo& error,
                   const XrdSecEntity* client,
                   const char* opaque)
{
  int retc;
  eos_debug("exists path=%s", path);

  // Get a socket object from the pool
  zmq::socket_t* socket;
  mPoolSocket.wait_pop(socket);
  RequestProto* req_proto = utils::GetExistsRequest(path, error, client, opaque);
     
  if (!SendProtoBufRequest(socket, req_proto))
  {
    OfsEroute.Emsg("exists", "unable to send request");
    return SFS_ERROR;
  }

  ResponseProto* resp_exists = static_cast<ResponseProto*>(GetResponse(socket));
  retc = resp_exists->response();
  eos_debug("exists retc=%i", retc);
  
  if (resp_exists->has_error())
  {
    error.setErrInfo(resp_exists->error().code(),
                     resp_exists->error().message().c_str());
  }

  if (resp_exists->has_message())
  {
    exists_flag = (XrdSfsFileExistence)atoi(resp_exists->message().c_str());
  }
  
  delete resp_exists;
  delete req_proto;

  // Put back the socket object in the pool
  mPoolSocket.push(socket);
  return retc;
}


//------------------------------------------------------------------------------
// Create directory
// Note: the mode set here is actually ignored if the directoy is not the top
// one. The new directory inherits the mode bits from its parent directory.
// This is typical only for EOS since in a normal XRootD server the access bits
// specified in the mkdir command are actually applied as expected.
//------------------------------------------------------------------------------
int
EosAuthOfs::mkdir (const char* path,
                   XrdSfsMode mode,  // Ignored in EOS if it has a parent dir
                   XrdOucErrInfo& error,
                   const XrdSecEntity* client,
                   const char* opaque)
{
  int retc;
  eos_debug("mkdir path=%s mode=%o", path, mode);

  // Get a socket object from the pool
  zmq::socket_t* socket;
  mPoolSocket.wait_pop(socket);
  RequestProto* req_proto = utils::GetMkdirRequest(path, mode, error, client, opaque);
     
  if (!SendProtoBufRequest(socket, req_proto))
  {
    OfsEroute.Emsg("mkdir", "unable to send request");
    return SFS_ERROR;
  }

  ResponseProto* resp_mkdir = static_cast<ResponseProto*>(GetResponse(socket));
  retc = resp_mkdir->response();
  eos_debug("mkdir retc=%i", retc);
  
  if (resp_mkdir->has_error())
  {
    error.setErrInfo(resp_mkdir->error().code(),
                     resp_mkdir->error().message().c_str());
  }
  
  delete resp_mkdir;
  delete req_proto;

  // Put back the socket object in the pool
  mPoolSocket.push(socket);
  return retc;
}


//------------------------------------------------------------------------------
// Remove directory
//------------------------------------------------------------------------------
int
EosAuthOfs::remdir(const char* path,
                   XrdOucErrInfo& error,
                   const XrdSecEntity* client,
                   const char* opaque)
{
  int retc;
  eos_debug("remdir path=%s", path);

  // Get a socket object from the pool
  zmq::socket_t* socket;
  mPoolSocket.wait_pop(socket);
  RequestProto* req_proto = utils::GetRemdirRequest(path, error, client, opaque);
     
  if (!SendProtoBufRequest(socket, req_proto))
  {
    OfsEroute.Emsg("remdir", "unable to send request");
    return SFS_ERROR;
  }

  ResponseProto* resp_remdir = static_cast<ResponseProto*>(GetResponse(socket));
  retc = resp_remdir->response();
  eos_debug("remdir retc=%i", retc);
  
  if (resp_remdir->has_error())
  {
    error.setErrInfo(resp_remdir->error().code(),
                     resp_remdir->error().message().c_str());
  }
  
  delete resp_remdir;
  delete req_proto;

  // Put back the socket object in the pool
  mPoolSocket.push(socket);
  return retc;
}


//------------------------------------------------------------------------------
// Remove file
//------------------------------------------------------------------------------
int
EosAuthOfs::rem(const char* path,
                XrdOucErrInfo& error,
                const XrdSecEntity* client,
                const char* opaque)
{
  int retc;
  eos_debug("rem path=%s", path);

  // Get a socket object from the pool
  zmq::socket_t* socket;
  mPoolSocket.wait_pop(socket);
  RequestProto* req_proto = utils::GetRemRequest(path, error, client, opaque);
     
  if (!SendProtoBufRequest(socket, req_proto))
  {
    OfsEroute.Emsg("rem", "unable to send request");
    return SFS_ERROR;
  }

  ResponseProto* resp_rem = static_cast<ResponseProto*>(GetResponse(socket));
  retc = resp_rem->response();
  eos_debug("rem retc=%i", retc);
  
  if (resp_rem->has_error())
  {
    error.setErrInfo(resp_rem->error().code(),
                     resp_rem->error().message().c_str());
  }
  
  delete resp_rem;
  delete req_proto;

  // Put back the socket object in the pool
  mPoolSocket.push(socket);
  return retc;
}


//------------------------------------------------------------------------------
// Rename file
//------------------------------------------------------------------------------
int
EosAuthOfs::rename (const char *oldName,
                    const char *newName,
                    XrdOucErrInfo &error,
                    const XrdSecEntity *client,
                    const char *opaqueO,
                    const char *opaqueN)
{
  int retc;
  eos_debug("rename oldname=%s newname=%s", oldName, newName);

  // Get a socket object from the pool
  zmq::socket_t* socket;
  mPoolSocket.wait_pop(socket);
  RequestProto* req_proto = utils::GetRenameRequest(oldName, newName, error,
                                                    client, opaqueO, opaqueN);
     
  if (!SendProtoBufRequest(socket, req_proto))
  {
    OfsEroute.Emsg("rename", "unable to send request");
    return SFS_ERROR;
  }

  ResponseProto* resp_rename = static_cast<ResponseProto*>(GetResponse(socket));
  retc = resp_rename->response();
  eos_debug("rename retc=%i", retc);
  
  if (resp_rename->has_error())
  {
    error.setErrInfo(resp_rename->error().code(),
                     resp_rename->error().message().c_str());
  }
  
  delete resp_rename;
  delete req_proto;

  // Put back the socket object in the pool
  mPoolSocket.push(socket);
  return retc;
}


//------------------------------------------------------------------------------
// Prepare request
//-----------------------------------------------------------------------------
int
EosAuthOfs::prepare(XrdSfsPrep& pargs,
                    XrdOucErrInfo& error,
                    const XrdSecEntity* client)
{
  int retc;
  eos_debug("prepare");

  // Get a socket object from the pool
  zmq::socket_t* socket;
  mPoolSocket.wait_pop(socket);
  RequestProto* req_proto = utils::GetPrepareRequest(pargs, error, client);
     
  if (!SendProtoBufRequest(socket, req_proto))
  {
    OfsEroute.Emsg("prepare", "unable to send request");
    return SFS_ERROR;
  }

  ResponseProto* resp_prepare = static_cast<ResponseProto*>(GetResponse(socket));
  retc = resp_prepare->response();
  eos_debug("prepare retc=%i", retc);
  
  if (resp_prepare->has_error())
  {
    error.setErrInfo(resp_prepare->error().code(),
                     resp_prepare->error().message().c_str());
  }
  
  delete resp_prepare;
  delete req_proto;

  // Put back the socket object in the pool
  mPoolSocket.push(socket);
  return retc;
}


//------------------------------------------------------------------------------
// Truncate file
//------------------------------------------------------------------------------
int
EosAuthOfs::truncate(const char* path,
                     XrdSfsFileOffset fileOffset,
                     XrdOucErrInfo& error,
                     const XrdSecEntity* client,
                     const char* opaque)
{
  int retc;
  eos_debug("truncate");

  // Get a socket object from the pool
  zmq::socket_t* socket;
  mPoolSocket.wait_pop(socket);
  RequestProto* req_proto = utils::GetTruncateRequest(path, fileOffset, error,
                                                      client, opaque);
     
  if (!SendProtoBufRequest(socket, req_proto))
  {
    OfsEroute.Emsg("truncate", "unable to send request");
    return SFS_ERROR;
  }

  ResponseProto* resp_truncate = static_cast<ResponseProto*>(GetResponse(socket));
  retc = resp_truncate->response();
  eos_debug("truncate retc=%i", retc);
  
  if (resp_truncate->has_error())
  {
    error.setErrInfo(resp_truncate->error().code(),
                     resp_truncate->error().message().c_str());
  }
  
  delete resp_truncate;
  delete req_proto;

  // Put back the socket object in the pool
  mPoolSocket.push(socket);
  return retc;
}



//------------------------------------------------------------------------------
// getStats function
//------------------------------------------------------------------------------
int
EosAuthOfs::getStats (char *buff, int blen)
{
  int retc;
  eos_debug("getStats");

  // Get a socket object from the pool
  zmq::socket_t* socket;
  mPoolSocket.wait_pop(socket);
  RequestProto* req_proto = utils::GetStatsRequest();
     
  if (!SendProtoBufRequest(socket, req_proto))
  {
    OfsEroute.Emsg("getStats", "unable to send request");
    return SFS_ERROR;
  }

  ResponseProto* resp_getStats = static_cast<ResponseProto*>(GetResponse(socket));
  retc = resp_getStats->response();
  eos_debug("getStats retc=%i", retc);
 
  delete resp_getStats;
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
  ResponseProto* resp = new ResponseProto();
  resp->ParseFromString(resp_str);
  return resp;
}


//------------------------------------------------------------------------------
// Create error message
//------------------------------------------------------------------------------
int
EosAuthOfsFile::Emsg(const char* pfx,
                     XrdOucErrInfo& einfo,
                     int ecode,
                     const char* op,
                     const char* target)
{
  char* etext, buffer[4096], unkbuff[64];

  // Get the reason for the error
  if (ecode < 0) ecode = -ecode;

  if (!(etext = strerror(ecode)))
  {
    sprintf(unkbuff, "reason unknown (%d)", ecode);
    etext = unkbuff;
  }

  // Format the error message
  snprintf(buffer, sizeof(buffer), "Unable to %s %s; %s", op, target, etext);
  eos_err("Unable to %s %s; %s", op, target, etext);

  // Place the error message in the error object and return
  einfo.setErrInfo(ecode, buffer);
  return SFS_ERROR;
}


EOSAUTHNAMESPACE_END

