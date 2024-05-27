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

#include <cstdio>
#include <sstream>
#include <fcntl.h>
#include <syscall.h>
#include <sys/time.h>
#include <zlib.h>
#include "EosAuthOfs.hh"
#include "ProtoUtils.hh"
#include "EosAuthOfsDirectory.hh"
#include "EosAuthOfsFile.hh"
#include "common/SymKeys.hh"
#include "XrdOuc/XrdOucTrace.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdOss/XrdOssApi.hh"
#include "XrdSec/XrdSecEntity.hh"
#include "XrdNet/XrdNetIF.hh"
#include "XrdNet/XrdNetUtils.hh"
#include "XrdNet/XrdNetAddr.hh"
#include "XProtocol/XProtocol.hh"
#include "XrdVersion.hh"
#include "google/protobuf/io/zero_copy_stream_impl.h"

// The global OFS handle
eos::auth::EosAuthOfs* eos::auth::gOFS;

extern XrdSysError OfsEroute;
extern XrdOfs* XrdOfsFS;
XrdVERSIONINFO(XrdSfsGetFileSystem2, AuthOfs);

//------------------------------------------------------------------------------
// Filesystem Plugin factory function
//------------------------------------------------------------------------------
extern "C"
{
  XrdSfsFileSystem* XrdSfsGetFileSystem2(XrdSfsFileSystem* native_fs,
                                         XrdSysLogger* lp,
                                         const char* configfn,
                                         XrdOucEnv* envP)
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

    if (eos::auth::gOFS->Configure(OfsEroute, envP)) {
      return 0;
    }

    XrdOfsFS = eos::auth::gOFS;
    return eos::auth::gOFS;
  }
}

EOSAUTHNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
EosAuthOfs::EosAuthOfs():
  XrdOfs(), eos::common::LogId(),  proxy_tid(0), mFrontend(0),
  mSizePoolSocket(5), mPort(0), mCollapsePort(0), mLogLevel(LOG_INFO)
{
  // Initialise the ZMQ client
  mZmqContext = new zmq::context_t(1);
  mBackend = std::make_pair(std::string(""), (zmq::socket_t*)0);

  // Set Logging parameters
  XrdOucString unit = "auth@localhost";
  // setup the circular in-memory log buffer
  eos::common::Logging& g_logging = eos::common::Logging::GetInstance();
  g_logging.SetLogPriority(mLogLevel);
  g_logging.SetUnit(unit.c_str());
  eos_info("info=\"logging configured\"");
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
EosAuthOfs::~EosAuthOfs()
{
  zmq::socket_t* socket;

  // Kill the auth proxy thread
  if (proxy_tid) {
    XrdSysThread::Cancel(proxy_tid);
    XrdSysThread::Join(proxy_tid, 0);
  }

  // Release memory
  while (mPoolSocket.try_pop(socket)) {
    delete socket;
  }

  delete mFrontend;
  delete mBackend.second;
  delete mZmqContext;
}


//------------------------------------------------------------------------------
// Configure routine
//------------------------------------------------------------------------------
int
EosAuthOfs::Configure(XrdSysError& error, XrdOucEnv* envP)
{
  int NoGo = 0;
  int cfgFD;
  char* var;
  const char* val;
  std::string space_tkn;
  // Configure the basic XrdOfs and exit if not successful
  NoGo = XrdOfs::Configure(error, envP);
  eos::common::Logging& g_logging = eos::common::Logging::GetInstance();

  if (NoGo) {
    return NoGo;
  }

  mPort = myPort;
  mCollapsePort = mPort; // by default we collapse on the same AUTH service port on a remote machine

  // Get the hostname
  const char* errtext = 0;
  const char* host_name = XrdNetUtils::MyHostName(0, &errtext);

  if (!host_name) {
    error.Emsg("Config", "hostname is invalid : %s", host_name);
    return 1;
  }

  XrdNetAddr* addrs  = 0;
  int         nAddrs = 0;
  const char* err    = XrdNetUtils::GetAddrs(host_name, &addrs, nAddrs,
                       XrdNetUtils::allIPv64,
                       XrdNetUtils::NoPortRaw);
  free(const_cast<char*>(host_name));

  if (err) {
    error.Emsg("Config", "hostname is invalid : %s", err);
    return 1;
  }

  if (nAddrs == 0) {
    error.Emsg("Config", "hostname is invalid");
    return 1;
  }

  char buffer[64];
  int length = addrs[0].Format(buffer, sizeof(buffer),
                               XrdNetAddrInfo::fmtAddr,
                               XrdNetAddrInfo::noPortRaw);
  delete [] addrs;

  if (length == 0) {
    error.Emsg("Config", "hostname is invalid");
    return 1;
  }

  mManagerIp.assign(buffer, length);
  // Extract the manager from the config file
  XrdOucStream Config(&error, getenv("XRDINSTANCE"));

  // Read in the auth configuration from the xrd.cf.auth file
  if (!ConfigFN || !*ConfigFN) {
    NoGo = 1;
    error.Emsg("Configure", "no configure file");
  } else {
    // Try to open the configuration file.
    if ((cfgFD = open(ConfigFN, O_RDONLY, 0)) < 0) {
      return error.Emsg("Configure", errno, "open config file fn=", ConfigFN);
    }

    Config.Attach(cfgFD);
    std::string auth_tag = "eosauth.";

    while ((var = Config.GetMyFirstWord())) {
      if (!strncmp(var, auth_tag.c_str(), auth_tag.length())) {
        var += auth_tag.length();
        // Get EOS instance to which we dispatch requests. Note that the port is the one
        // waiting for authentication requests and not the usual one i.e 1094. The presence
        // of the mgm parameter is mandatory.
        std::string mgm_instance;
        std::string option_tag = "mgm";

        if (!strncmp(var, option_tag.c_str(), option_tag.length())) {
          if ((val = Config.GetWord())) {
            mgm_instance = val;

            if (mgm_instance.find(":") != std::string::npos) {
              mBackend = std::make_pair(mgm_instance, (zmq::socket_t*)0);
            }
          } else {
            // This parameter is critical
            error.Emsg("Configure ", "No EOS mgm instance provided");
            NoGo = 1;
          }
        }

	// Get number of sockets in the pool by default 10
        option_tag = "collapseport";

        if (!strncmp(var, option_tag.c_str(), option_tag.length())) {
          if (!(val = Config.GetWord())) {
            error.Emsg("Configure ", "No collapseport specified");
          } else {
            mCollapsePort = atoi(val);
          }
        }

        // Get number of sockets in the pool by default 10
        option_tag = "numsockets";

        if (!strncmp(var, option_tag.c_str(), option_tag.length())) {
          if (!(val = Config.GetWord())) {
            error.Emsg("Configure ", "No number of sockets specified");
          } else {
            mSizePoolSocket = atoi(val);
          }
        }

        // Get log level by default LOG_INFO
        option_tag = "loglevel";

        if (!strncmp(var, option_tag.c_str(), option_tag.length())) {
          if (!(val = Config.GetWord())) {
            error.Emsg("Config", "argument for debug level invalid set to ERR.");
            mLogLevel = LOG_INFO;
          } else {
            std::string str_val(val);

            if (isdigit(str_val[0])) {
              // The level is given as a number
              mLogLevel = atoi(val);
            } else {
              // The level is given as a string
              mLogLevel = g_logging.GetPriorityByString(val);
            }

            error.Say("=====> eosauth.loglevel: ",
                      g_logging.GetPriorityString(mLogLevel), "");
          }

          // Set the new log level
          g_logging.SetLogPriority(mLogLevel);
        }
      }
    }

    // Check and connect at least to an MGM master
    if (!mBackend.first.empty()) {
      if ((XrdSysThread::Run(&proxy_tid, EosAuthOfs::StartAuthProxyThread,
                             static_cast<void*>(this), 0, "Auth Proxy Thread"))) {
        eos_err("cannot start the authentication proxy thread");
        NoGo = 1;
      }

      // Create a pool of sockets connected to the master proxy service
      for (int i = 0; i < mSizePoolSocket; i++) {
        // Set socket receive timeout to 5 seconds
        zmq::socket_t* socket = new zmq::socket_t(*mZmqContext, ZMQ_REQ);
        int timeout_mili = 5000;
        socket->set(zmq::sockopt::rcvtimeo, timeout_mili);
        int socket_linger = 0;
        socket->set(zmq::sockopt::linger, socket_linger);
        std::string endpoint = "inproc://proxyfrontend";

        // Try in a loop to connect to the proxyfrontend as it can take a while for
        // the proxy thread to do the binding, therefore connect can fail
        while (1) {
          try {
            socket->connect(endpoint.c_str());
          } catch (zmq::error_t& err) {
            eos_warning("dealing with connect exception, retrying ...");
            continue;
          }

          break;
        }

        mPoolSocket.push(socket);
      }
    } else {
      eos_err("No master MGM specified e.g. eos.master.cern.ch:15555");
      NoGo = 1;
    }

    close(cfgFD);
  }

  //----------------------------------------------------------------------------
  // Build the adler & sha1 checksum of the default keytab file
  //----------------------------------------------------------------------------
  XrdOucString keytabcks = "unaccessible";
  std::string keytab_path = "/etc/eos.keytab";
  int fd = ::open(keytab_path.c_str(), O_RDONLY);
  XrdOucString symkey = "";

  if (fd >= 0) {
    char buffer[65535];
    char keydigest[SHA_DIGEST_LENGTH + 1];
    SHA_CTX sha1;
    SHA1_Init(&sha1);
    size_t nread = ::read(fd, buffer, sizeof(buffer));

    if (nread > 0) {
      unsigned int adler;
      SHA1_Update(&sha1, (const char*) buffer, nread);
      adler = adler32(0L, Z_NULL, 0);
      adler = adler32(adler, (const Bytef*) buffer, nread);
      char sadler[1024];
      snprintf(sadler, sizeof(sadler) - 1, "%08x", adler);
      keytabcks = sadler;
    } else {
      eos_err("Failed while readling, error: %s", strerror(errno));
      close(fd);
      return 1;
    }

    SHA1_Final((unsigned char*) keydigest, &sha1);
    eos::common::SymKey::Base64Encode(keydigest, SHA_DIGEST_LENGTH, symkey);
    close(fd);
  } else {
    eos_err("Failed to open keytab file: %s", keytab_path.c_str());
    return 1;
  }

  eos_notice("AUTH_HOST=%s AUTH_PORT=%ld VERSION=%s RELEASE=%s KEYTABADLER=%s",
             mManagerIp.c_str(), myPort, VERSION, RELEASE, keytabcks.c_str());

  if (!eos::common::gSymKeyStore.SetKey64(symkey.c_str(), 0)) {
    eos_crit("unable to store the created symmetric key %s", symkey.c_str());
    NoGo = 1;
  }

  return NoGo;
}


//------------------------------------------------------------------------------
// Authentication proxy thread startup function
//------------------------------------------------------------------------------
void*
EosAuthOfs::StartAuthProxyThread(void* pp)
{
  EosAuthOfs* ofs = static_cast<EosAuthOfs*>(pp);
  ofs->AuthProxyThread();
  return 0;
}


//------------------------------------------------------------------------------
// Authentication proxt thread which forwards requests form the clients
// to the proper MGM intance.
//------------------------------------------------------------------------------
void
EosAuthOfs::AuthProxyThread()
{
  // Bind the client facing socket
  mFrontend = new zmq::socket_t(*mZmqContext, ZMQ_ROUTER);
  mFrontend->bind("inproc://proxyfrontend");
  // Connect sockets facing the MGM nodes - master and slave
  std::ostringstream sstr;
  mBackend = std::make_pair(mBackend.first,
			    new zmq::socket_t(*mZmqContext, ZMQ_DEALER));
  sstr << "tcp://" << mBackend.first;
  mBackend.second->connect(sstr.str().c_str());
  OfsEroute.Say("=====> connected to MGM: ", mBackend.first.c_str());

  // Set the master to point to the master MGM - no need for lock
  auto master = mBackend.second;
  int rc = -1;
  zmq::message_t msg;
  // Start the proxy using the first entry
  int more;
  int poll_size = 2;
  zmq::pollitem_t items[3] = {
    { (void*)* mFrontend, 0, ZMQ_POLLIN, 0},
    { (void*)* mBackend.second, 0, ZMQ_POLLIN, 0}
  };

  // Main loop in which the proxy thread accepts request from the clients and
  // then he forwards them to the current master MGM. The master MGM can change
  // at any point.
  while (true) {
    // Wait while there are either requests or replies to process
    try {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
      rc = zmq::poll(&items[0], poll_size, -1);
#pragma GCC diagnostic pop
    } catch (zmq::error_t& e) {
      eos_err("Exception thrown: %s", e.what());
    }

    if (rc < 0) {
      eos_err("error in poll");
      return;
    }

    // Process a request
    if (items[0].revents & ZMQ_POLLIN) {
      eos_debug("got frontend event");
      zmq::recv_flags rf = zmq::recv_flags::none;

      while (true) {
        if (!mFrontend->recv(msg, rf).has_value()) {
          eos_err("error while recv on frontend");
          return;
        }

        try {
          more = mFrontend->get(zmq::sockopt::rcvmore);
        } catch (zmq::error_t& err) {
          eos_err("exception in getsockopt");
          return;
        }

        // Send request to the MGM
        {
          XrdSysMutexHelper scop_lock(mMutexMaster);
          zmq::send_flags sf = zmq::send_flags::none;

          if (more) {
            sf = zmq::send_flags::sndmore;
          }

          if (!master->send(msg, sf)) {
            eos_err("error while sending to master");
            return;
          }
        }

        if (more == 0) {
          break;
        }
      }
    }

    // Process a reply from the first MGM
    if (items[1].revents & ZMQ_POLLIN) {
      eos_debug("got mBackend event");
      zmq::recv_flags rf = zmq::recv_flags::none;

      while (true) {
        if (!mBackend.second->recv(msg, rf).has_value()) {
          eos_err("error while recv on mBackend");
          return;
        }

        try {
          more = mBackend.second->get(zmq::sockopt::rcvmore);
        } catch (zmq::error_t& err) {
          eos_err("exception in getsockopt");
          return;
        }

        zmq::send_flags sf = zmq::send_flags::none;

        if (more) {
          sf = zmq::send_flags::sndmore;
        }

        if (!mFrontend->send(msg, sf)) {
          eos_err("error while send to frontend(1)");
          return;
        }

        if (more == 0) {
          break;
        }
      }
    }
  }
}


//------------------------------------------------------------------------------
// Get directory object
//------------------------------------------------------------------------------
XrdSfsDirectory*
EosAuthOfs::newDir(char* user, int MonID)
{
  return static_cast<XrdSfsDirectory*>(new EosAuthOfsDirectory(user, MonID));
}


//------------------------------------------------------------------------------
// Get file object
//------------------------------------------------------------------------------
XrdSfsFile*
EosAuthOfs::newFile(char* user, int MonID)
{
  return static_cast<XrdSfsFile*>(new EosAuthOfsFile(user, MonID));
}

void
EosAuthOfs::ProcessError(eos::auth::ResponseProto* resp_func, XrdOucErrInfo& error, const char* path)
{
  if (resp_func->has_error()) {
    if (resp_func->collapse() && path && strlen(path)) {
      // collpase redirection to remote AUTH MGM
      std::string url = "root://";
      url += resp_func->error().message();;
      url += ":";
      url += std::to_string(mCollapsePort);
      url += "/";
      url += path;
      error.setErrInfo(~(~(-1) | kXR_collapseRedir), url.c_str());
    } else {
      // one-shot redirection to remote MGM
      error.setErrInfo(resp_func->error().code(),
		       resp_func->error().message().c_str());
    }
  }
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
  int retc = SFS_ERROR;
  eos_debug("stat path=%s", path);
  // Create request object
  RequestProto* req_proto = utils::GetStatRequest(RequestProto_OperationType_STAT,
                            path, error, client, opaque);

  // Compute HMAC for request object
  if (!utils::ComputeHMAC(req_proto)) {
    eos_err("error HMAC FS stat");
    delete req_proto;
    return retc;
  }

  // Get a socket object from the pool
  zmq::socket_t* socket;
  mPoolSocket.wait_pop(socket);

  if (SendProtoBufRequest(socket, req_proto)) {
    ResponseProto* resp_stat = static_cast<ResponseProto*>(GetResponse(socket));

    if (resp_stat) {
      retc = resp_stat->response();

      ProcessError(resp_stat, error, path);

      // We retrieve the struct stat if response is ok
      if ((retc == SFS_OK) && resp_stat->has_message()) {
        buf = static_cast<struct stat*>(memcpy((void*)buf,
                                               resp_stat->message().c_str(),
                                               sizeof(struct stat)));
      }

      delete resp_stat;
    }
  }

  // Release socket and free memory
  gOFS->mPoolSocket.push(socket);
  delete req_proto;
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
  int retc = SFS_ERROR;
  eos_debug("statm path=%s", path);
  RequestProto* req_proto = utils::GetStatRequest(
                              RequestProto_OperationType_STATM,
                              path, error, client, opaque);

  // Compute HMAC for request object
  if (!utils::ComputeHMAC(req_proto)) {
    eos_err("error HMAC FS statm");
    delete req_proto;
    return retc;
  }

  // Get a socket object from the pool
  zmq::socket_t* socket;
  mPoolSocket.wait_pop(socket);

  if (SendProtoBufRequest(socket, req_proto)) {
    ResponseProto* resp_stat = static_cast<ResponseProto*>(GetResponse(socket));

    if (resp_stat) {
      retc = resp_stat->response();

      ProcessError(resp_stat, error, path);

      // We retrieve the open mode if response if ok
      if ((retc == SFS_OK) && resp_stat->has_message()) {
        memcpy((void*)&mode, resp_stat->message().c_str(), sizeof(mode_t));
      }

      delete resp_stat;
    }
  }

  // Release socket and free memory
  gOFS->mPoolSocket.push(socket);
  delete req_proto;
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
  int retc = SFS_ERROR;
  eos_debug("fsctl with cmd=%i, args=%s", cmd, args);
  int opcode = cmd & SFS_FSCTL_CMD;

  // For the server configuration query we asnwer with the information of the
  // authentication XRootD server i.e. don't frw it to the real MGM.
  if (opcode == SFS_FSCTL_LOCATE) {
    char locResp[4096];
    char rType[3], *Resp[] = {rType, locResp};
    rType[0] = 'S';
    // don't manage writes via global redirection - therefore we mark the files as 'r'
    rType[1] = 'r';
    rType[2] = '\0';
    sprintf(locResp, "[::%s]:%d ", (char*) gOFS->mManagerIp.c_str(),
            gOFS->mPort);
    error.setErrInfo(strlen(locResp) + 3, (const char**) Resp, 2);
    return SFS_DATA;
  }

  RequestProto* req_proto = utils::GetFsctlRequest(cmd, args, error, client);

  // Compute HMAC for request object
  if (!utils::ComputeHMAC(req_proto)) {
    eos_err("error HMAC FS fsctl");
    delete req_proto;
    return retc;
  }

  // Get a socket object from the pool
  zmq::socket_t* socket;
  mPoolSocket.wait_pop(socket);

  if (SendProtoBufRequest(socket, req_proto)) {
    ResponseProto* resp_fsctl1 = static_cast<ResponseProto*>(GetResponse(socket));

    if (resp_fsctl1) {
      retc = resp_fsctl1->response();

      // TODO: we can't collapse without a path ...
      ProcessError(resp_fsctl1, error, 0);

      delete resp_fsctl1;
    }
  }

  // Release socket and free memory
  gOFS->mPoolSocket.push(socket);
  delete req_proto;
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
  int retc = SFS_ERROR;
  eos_debug("FSctl with cmd=%i", cmd);
  RequestProto* req_proto = utils::GetFSctlRequest(cmd, args, error, client);

  // Compute HMAC for request object
  if (!utils::ComputeHMAC(req_proto)) {
    eos_err("error HMAC FS FSctl");
    delete req_proto;
    return retc;
  }

  // Get a socket object from the pool
  zmq::socket_t* socket;
  mPoolSocket.wait_pop(socket);

  if (SendProtoBufRequest(socket, req_proto)) {
    ResponseProto* resp_fsctl2 = static_cast<ResponseProto*>(GetResponse(socket));

    if (resp_fsctl2) {
      retc = resp_fsctl2->response();

      // TODO: we can't collapse without a path ...
      ProcessError(resp_fsctl2, error, 0);

      delete resp_fsctl2;
    }
  }

  // Release socket and free memory
  gOFS->mPoolSocket.push(socket);
  delete req_proto;
  return retc;
}


//------------------------------------------------------------------------------
// Chmod by client
//------------------------------------------------------------------------------
int
EosAuthOfs::chmod(const char* path,
                  XrdSfsMode mode,
                  XrdOucErrInfo& error,
                  const XrdSecEntity* client,
                  const char* opaque)
{
  int retc = SFS_ERROR;
  eos_debug("chmod path=%s mode=%o", path, mode);
  RequestProto* req_proto = utils::GetChmodRequest(path, mode, error, client,
                            opaque);

  // Compute HMAC for request object
  if (!utils::ComputeHMAC(req_proto)) {
    eos_err("error HMAC FS chmod");
    delete req_proto;
    return retc;
  }

  // Get a socket object from the pool
  zmq::socket_t* socket;
  mPoolSocket.wait_pop(socket);

  if (SendProtoBufRequest(socket, req_proto)) {
    ResponseProto* resp_chmod = static_cast<ResponseProto*>(GetResponse(socket));

    if (resp_chmod) {
      retc = resp_chmod->response();

      ProcessError(resp_chmod, error, path);

      delete resp_chmod;
    }
  }

  // Release socket and free memory
  gOFS->mPoolSocket.push(socket);
  delete req_proto;
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
  int retc = SFS_ERROR;
  eos_debug("chksum path=%s csName=%s", path, csName);
  RequestProto* req_proto = utils::GetChksumRequest(func, csName, path, error,
                            client, opaque);

  // Compute HMAC for request object
  if (!utils::ComputeHMAC(req_proto)) {
    eos_err("error HMAC FS chksum");
    delete req_proto;
    return retc;
  }

  // Get a socket object from the pool
  zmq::socket_t* socket;
  mPoolSocket.wait_pop(socket);

  if (SendProtoBufRequest(socket, req_proto)) {
    ResponseProto* resp_chksum = static_cast<ResponseProto*>(GetResponse(socket));

    if (resp_chksum) {
      retc = resp_chksum->response();
      eos_debug("chksum retc=%i", retc);

      ProcessError(resp_chksum, error, path);

      delete resp_chksum;
    }
  }

  // Release socket and free memory
  gOFS->mPoolSocket.push(socket);
  delete req_proto;
  return retc;
}


//------------------------------------------------------------------------------
// Exists function
//------------------------------------------------------------------------------
int
EosAuthOfs::exists(const char* path,
                   XrdSfsFileExistence& exists_flag,
                   XrdOucErrInfo& error,
                   const XrdSecEntity* client,
                   const char* opaque)
{
  int retc = SFS_ERROR;
  eos_debug("exists path=%s", path);
  RequestProto* req_proto = utils::GetExistsRequest(path, error, client, opaque);

  // Compute HMAC for request object
  if (!utils::ComputeHMAC(req_proto)) {
    eos_err("error HMAC FS exists");
    delete req_proto;
    return retc;
  }

  // Get a socket object from the pool
  zmq::socket_t* socket;
  mPoolSocket.wait_pop(socket);

  if (SendProtoBufRequest(socket, req_proto)) {
    ResponseProto* resp_exists = static_cast<ResponseProto*>(GetResponse(socket));

    if (resp_exists) {
      retc = resp_exists->response();
      eos_debug("exists retc=%i", retc);

      ProcessError(resp_exists, error, path);

      if (resp_exists->has_message()) {
        exists_flag = (XrdSfsFileExistence)atoi(resp_exists->message().c_str());
      }

      delete resp_exists;
    }
  }

  // Release socket and free memory
  gOFS->mPoolSocket.push(socket);
  delete req_proto;
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
EosAuthOfs::mkdir(const char* path,
                  XrdSfsMode mode,  // Ignored in EOS if it has a parent dir
                  XrdOucErrInfo& error,
                  const XrdSecEntity* client,
                  const char* opaque)
{
  int retc = SFS_ERROR;
  eos_debug("mkdir path=%s mode=%o", path, mode);
  RequestProto* req_proto = utils::GetMkdirRequest(path, mode, error, client,
                            opaque);

  // Compute HMAC for request object
  if (!utils::ComputeHMAC(req_proto)) {
    eos_err("error HMAC FS mkdir");
    delete req_proto;
    return retc;
  }

  // Get a socket object from the pool
  zmq::socket_t* socket;
  mPoolSocket.wait_pop(socket);

  if (SendProtoBufRequest(socket, req_proto)) {
    ResponseProto* resp_mkdir = static_cast<ResponseProto*>(GetResponse(socket));

    if (resp_mkdir) {
      retc = resp_mkdir->response();
      eos_debug("mkdir retc=%i", retc);

      ProcessError(resp_mkdir, error, path);

      delete resp_mkdir;
    }
  }

  // Release socket and free memory
  gOFS->mPoolSocket.push(socket);
  delete req_proto;
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
  int retc = SFS_ERROR;
  eos_debug("remdir path=%s", path);
  RequestProto* req_proto = utils::GetRemdirRequest(path, error, client, opaque);

  // Compute HMAC for request object
  if (!utils::ComputeHMAC(req_proto)) {
    eos_err("error HMAC FS remdir");
    delete req_proto;
    return retc;
  }

  // Get a socket object from the pool
  zmq::socket_t* socket;
  mPoolSocket.wait_pop(socket);

  if (SendProtoBufRequest(socket, req_proto)) {
    ResponseProto* resp_remdir = static_cast<ResponseProto*>(GetResponse(socket));

    if (resp_remdir) {
      retc = resp_remdir->response();
      eos_debug("remdir retc=%i", retc);

      ProcessError(resp_remdir, error, path);

      delete resp_remdir;
    }
  }

  // Release socket and free memory
  gOFS->mPoolSocket.push(socket);
  delete req_proto;
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
  int retc = SFS_ERROR;
  eos_debug("rem path=%s", path);
  RequestProto* req_proto = utils::GetRemRequest(path, error, client, opaque);

  // Compute HMAC for request object
  if (!utils::ComputeHMAC(req_proto)) {
    eos_err("error HMAC FS rem");
    delete req_proto;
    return retc;
  }

  // Get a socket object from the pool
  zmq::socket_t* socket;
  mPoolSocket.wait_pop(socket);

  if (SendProtoBufRequest(socket, req_proto)) {
    ResponseProto* resp_rem = static_cast<ResponseProto*>(GetResponse(socket));

    if (resp_rem) {
      retc = resp_rem->response();
      eos_debug("rem retc=%i", retc);

      ProcessError(resp_rem, error, path);

      delete resp_rem;
    }
  }

  // Release socket and free memory
  gOFS->mPoolSocket.push(socket);
  delete req_proto;
  return retc;
}


//------------------------------------------------------------------------------
// Rename file
//------------------------------------------------------------------------------
int
EosAuthOfs::rename(const char* oldName,
                   const char* newName,
                   XrdOucErrInfo& error,
                   const XrdSecEntity* client,
                   const char* opaqueO,
                   const char* opaqueN)
{
  int retc = SFS_ERROR;
  eos_debug("rename oldname=%s newname=%s", oldName, newName);
  RequestProto* req_proto = utils::GetRenameRequest(oldName, newName, error,
                            client, opaqueO, opaqueN);

  // Compute HMAC for request object
  if (!utils::ComputeHMAC(req_proto)) {
    eos_err("error HMAC FS rename");
    delete req_proto;
    return retc;
  }

  // Get a socket object from the pool
  zmq::socket_t* socket;
  mPoolSocket.wait_pop(socket);

  if (SendProtoBufRequest(socket, req_proto)) {
    ResponseProto* resp_rename = static_cast<ResponseProto*>(GetResponse(socket));

    if (resp_rename) {
      retc = resp_rename->response();
      eos_debug("rename retc=%i", retc);

      ProcessError(resp_rename, error, 0);

      delete resp_rename;
    }
  }

  // Release socket and free memory
  gOFS->mPoolSocket.push(socket);
  delete req_proto;
  return retc;
}


//------------------------------------------------------------------------------
// Prepare request
//------------------------------------------------------------------------------
int
EosAuthOfs::prepare(XrdSfsPrep& pargs,
                    XrdOucErrInfo& error,
                    const XrdSecEntity* client)
{
  int retc = SFS_ERROR;
  eos_debug("prepare");
  RequestProto* req_proto = utils::GetPrepareRequest(pargs, error, client);

  // Compute HMAC for request object
  if (!utils::ComputeHMAC(req_proto)) {
    eos_err("error HMAC FS prepare");
    delete req_proto;
    return retc;
  }

  // Get a socket object from the pool
  zmq::socket_t* socket;
  mPoolSocket.wait_pop(socket);

  if (SendProtoBufRequest(socket, req_proto)) {
    ResponseProto* resp_prepare = static_cast<ResponseProto*>(GetResponse(socket));

    if (resp_prepare) {
      retc = resp_prepare->response();
      eos_debug("prepare retc=%i", retc);

      ProcessError(resp_prepare, error, 0);

      delete resp_prepare;
    }
  }

  // Release socket and free memory
  gOFS->mPoolSocket.push(socket);
  delete req_proto;
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
  int retc = SFS_ERROR;
  eos_debug("truncate");
  RequestProto* req_proto = utils::GetTruncateRequest(path, fileOffset, error,
                            client, opaque);

  // Compute HMAC for request object
  if (!utils::ComputeHMAC(req_proto)) {
    eos_err("error HMAC FS truncate");
    delete req_proto;
    return retc;
  }

  // Get a socket object from the pool
  zmq::socket_t* socket;
  mPoolSocket.wait_pop(socket);

  if (SendProtoBufRequest(socket, req_proto)) {
    ResponseProto* resp_truncate = static_cast<ResponseProto*>(GetResponse(socket));

    if (resp_truncate) {
      retc = resp_truncate->response();
      eos_debug("truncate retc=%i", retc);

      ProcessError(resp_truncate, error, path);

      delete resp_truncate;
    }
  }

  // Release socket and free memory
  gOFS->mPoolSocket.push(socket);
  delete req_proto;
  return retc;
}


//------------------------------------------------------------------------------
// getStats function - not supported by EOS, fake ok response HERE i.e. do not
// build and send a request to the real MGM
//------------------------------------------------------------------------------
int
EosAuthOfs::getStats(char* buff, int blen)
{
  int retc = SFS_OK;
  eos_debug("getStats");
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
  bool sent = false;
#if GOOGLE_PROTOBUF_VERSION < 3004000
  int msg_size = message->ByteSize();
#else
  int msg_size = message->ByteSizeLong();
#endif
  zmq::message_t request(msg_size);
  google::protobuf::io::ArrayOutputStream aos(request.data(), msg_size);

  if (!message->SerializeToZeroCopyStream(&aos)) {
    eos_err("failed to serialize message");
    return sent;
  }

  zmq::send_flags sf = zmq::send_flags::dontwait;
  auto r = socket->send(request, sf);

  if (r.has_value()) {
    sent = true;
  }

  if (!sent) {
    eos_err("unable to send request using zmq");
  }

  return sent;
}


//------------------------------------------------------------------------------
// Get ProtocolBuffer response object using ZMQ
//------------------------------------------------------------------------------
google::protobuf::Message*
EosAuthOfs::GetResponse(zmq::socket_t*& socket)
{
  // It makes no sense to wait more than 1 min since the XRootD client will
  // timeout by default after 60 seconds.
  int num_retries = 12; // 1 min = 12 * 5 sec
  bool done = false;
  bool reset_socket = false;
  zmq::message_t reply;
  ResponseProto* resp = static_cast<ResponseProto*>(0);

  try {
    zmq::recv_flags rf = zmq::recv_flags::none;
    zmq::recv_result_t rr;

    do {
      rr = socket->recv(reply, rf);
      --num_retries;

      if (!rr.has_value()) {
        eos_err("ptr_socket=%p, num_retries=%i failed receive", socket,
                num_retries);
      } else {
        done = true;
      }
    } while (!rr.has_value() && (num_retries > 0));
  } catch (zmq::error_t& e) {
    eos_err("socket error: %s", e.what());
    reset_socket = true;
  }

  // We time out while waiting for a response or a fatal error occurent -
  // then we throw away the socket and create a new one
  if ((num_retries <= 0) || reset_socket) {
    eos_err("discard current socket and create a new one");
    delete socket;
    socket = new zmq::socket_t(*mZmqContext, ZMQ_REQ);
    int timeout_mili = 5000;
    socket->set(zmq::sockopt::rcvtimeo, timeout_mili);
    int socket_linger = 0;
    socket->set(zmq::sockopt::linger, socket_linger);
    std::string endpoint = "inproc://proxyfrontend";

    // Try in a loop to connect to the proxyfrontend as it can take a while for
    // the proxy thread to do the binding, therefore connect can fail
    while (1) {
      try {
        socket->connect(endpoint.c_str());
      } catch (zmq::error_t& err) {
        eos_warning("dealing with connect exception, retrying ...");
        continue;
      }

      break;
    }
  }

  if (done) {
    std::string resp_str = std::string(static_cast<char*>(reply.data()),
                                       reply.size());
    resp = new ResponseProto();
    resp->ParseFromString(resp_str);

    // If the response is a redirect we redirect to our own port number on the target
    // - this allows to failover access from one AUTH to another AUTH daemon in an HA MGM setup

    if (resp->response() == SFS_REDIRECT) {
      if (resp->has_error()) {
        std::ostringstream sstr;
        sstr << resp->error().message();
        std::string redirect_host = sstr.str();

	// Add redirect collapse
	resp->set_collapse(true);
      } else {
        eos_err("redirect message without error information - change to error");
        resp->set_response(SFS_ERROR);
      }
    }
  } else {
    eos_err("socket error/timeout during receive");
  }

  return resp;
}

EOSAUTHNAMESPACE_END
