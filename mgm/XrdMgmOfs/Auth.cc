// -----------------------------------------------------------------------------
// File: Auth.cc
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

#include "auth_plugin/ProtoUtils.hh"

//------------------------------------------------------------------------------
// Authentication master thread startup function
//------------------------------------------------------------------------------
void*
XrdMgmOfs::StartAuthWorkerThread(void* pp)
{
  XrdMgmOfs* ofs = static_cast<XrdMgmOfs*>(pp);
  ofs->AuthWorkerThread();
  return 0;
}

//------------------------------------------------------------------------------
// Authentication master thread function - accepts requests from EOS AUTH
// plugins which he then forwards to worker threads.
//------------------------------------------------------------------------------
void
XrdMgmOfs::AuthMasterThread(ThreadAssistant& assistant) noexcept
{
  // Socket facing clients
  zmq::socket_t frontend(*mZmqContext, ZMQ_ROUTER);
  int enable_ipv6 = 1;
#if ZMQ_VERSION >= ZMQ_MAKE_VERSION(4, 1, 0)
  frontend.setsockopt(ZMQ_IPV6, &enable_ipv6, sizeof(enable_ipv6));
#else
  enable_ipv6 = 0;
  frontend.setsockopt(ZMQ_IPV4ONLY, &enable_ipv6, sizeof(enable_ipv6));
#endif
  std::ostringstream sstr;
  sstr << "tcp://*:" << mFrontendPort;

  try {
    frontend.bind(sstr.str().c_str());
  } catch (zmq::error_t& err) {
    eos_static_err("failed to bind frontend socket");
    return;
  }

  // Socket facing worker threads
  zmq::socket_t backend(*mZmqContext, ZMQ_DEALER);

  try {
    backend.bind("inproc://authbackend");
  } catch (zmq::error_t& err) {
    eos_static_err("failed to bind backend socket");
    return;
  }

  // Start the proxy
#if ZMQ_VERSION_MAJOR == 2
  zmq_device(ZMQ_QUEUE, &frontend, &backend);
#else

  try {
    zmq::proxy(static_cast<void*>(frontend), static_cast<void*>(backend),
               static_cast<void*>(0));
  } catch (const zmq::error_t& e) {
    if (e.num() == ETERM) {
      eos_warning("msg=\"master termination requested\" tid=%08x",
                  XrdSysThread::ID());
      return;
    }
  }

#endif
}

//------------------------------------------------------------------------------
// Reconnect zmq::socket object
//------------------------------------------------------------------------------
bool
XrdMgmOfs::ConnectToBackend(zmq::socket_t*& socket)
{
  if (socket) {
    delete socket;
    socket = 0;
  }

  socket  = new zmq::socket_t(*mZmqContext, ZMQ_REP);
  // Try to connect to proxy thread - the bind can take a longer time so threfore
  // keep trying until it is successful
  bool connected = false;
  uint8_t tries = 0;

  while (tries <= 5) {
    try {
      socket->connect("inproc://authbackend");
    } catch (const zmq::error_t& e) {
      if (e.num() == ETERM) {
        eos_warning("msg=\"worker termination requested\" tid=%08x",
                    XrdSysThread::ID());
        return connected;
      }

      eos_debug("auth worker connection failed - retry");
      tries++;
      sleep(1);
      continue;
    }

    connected = true;
    break;
  }

  return connected;
}

//------------------------------------------------------------------------------
// Authentication worker thread function - accepts requests from the master,
// executed the proper action and replies with the result.
//------------------------------------------------------------------------------
void
XrdMgmOfs::AuthWorkerThread()
{
  using namespace eos::auth;
  int ret;
  eos_static_info("msg=\"authentication worker thread starting\"");
  zmq::socket_t* responder = 0;

  if (!ConnectToBackend(responder)) {
    eos_err("msg=\"kill thread as we could not connect to backend socket\"");
    delete responder;
    return;
  }

  bool done = false;
  std::chrono::steady_clock::time_point time_start, time_end;

  // Main loop of the worker thread
  while (true) {
    zmq::message_t request;

    // Wait for next request
    try {
      do {
        done = responder->recv(&request);
      } while (!done);
    } catch (const zmq::error_t& e) {
      if (e.num() == ETERM) {
        eos_warning("msg=\"worker termination requested\" tid=%08x",
                    XrdSysThread::ID());
        delete responder;
        return;
      }

      eos_err("msg=\"socket recv error: %s, trying to reset the socket\"",
              e.what());

      if (!ConnectToBackend(responder)) {
        eos_err("msg=\"kill thread as we could not connect to backend socket\"");
        delete responder;
        return;
      }

      continue;
    }

    // Read in the ProtocolBuffer object just received
    time_start = std::chrono::steady_clock::now();
    std::string msg_recv((char*)request.data(), request.size());
    RequestProto req_proto;
    req_proto.ParseFromString(msg_recv);
    ResponseProto resp;
    std::shared_ptr<XrdOucErrInfo> error(static_cast<XrdOucErrInfo*>(0));
    XrdSecEntity* client = 0;

    if (!ValidAuthRequest(&req_proto)) {
      eos_err("message HMAC received is not valid, dropping request");
      error.reset(new XrdOucErrInfo("admin"));
      error.get()->setErrInfo(EKEYREJECTED, "request HMAC value is wrong");
      ret = SFS_ERROR;
    } else if (req_proto.type() == RequestProto_OperationType_STAT) {
      // stat request
      struct stat buf;
      error.reset(utils::GetXrdOucErrInfo(req_proto.stat().error()));
      client = utils::GetXrdSecEntity(req_proto.stat().client());
      ret = gOFS->stat(req_proto.stat().path().c_str(), &buf,
                       *error.get(), client, req_proto.stat().opaque().c_str());
      // Fill in particular info for stat request
      resp.set_message(&buf, sizeof(struct stat));
      eos_debug("stat error msg: %s", error->getErrText());
    } else if (req_proto.type() == RequestProto_OperationType_STATM) {
      // stat mode request
      mode_t mode;
      error.reset(utils::GetXrdOucErrInfo(req_proto.stat().error()));
      client = utils::GetXrdSecEntity(req_proto.stat().client());
      ret = gOFS->stat(req_proto.stat().path().c_str(), mode,
                       *error.get(), client, req_proto.stat().opaque().c_str());
      // Fill in particular info for stat request
      resp.set_message(&mode, sizeof(mode_t));
      eos_debug("statm error msg: %s", error->getErrText());
    } else if (req_proto.type() == RequestProto_OperationType_FSCTL1) {
      // fsctl request
      error.reset(utils::GetXrdOucErrInfo(req_proto.fsctl1().error()));
      client = utils::GetXrdSecEntity(req_proto.fsctl1().client());
      ret = gOFS->fsctl(req_proto.fsctl1().cmd(), req_proto.fsctl1().args().c_str(),
                        *error.get(), client);
      eos_debug("fsctl error msg: %s", error->getErrText());
    } else if (req_proto.type() == RequestProto_OperationType_FSCTL2) {
      // FSctl request
      error.reset(utils::GetXrdOucErrInfo(req_proto.fsctl2().error()));
      client = utils::GetXrdSecEntity(req_proto.fsctl2().client());
      XrdSfsFSctl* obj = utils::GetXrdSfsFSctl(req_proto.fsctl2().args());
      ret = gOFS->FSctl(req_proto.fsctl2().cmd(), *obj, *error.get(), client);
      eos_debug("FSctl error msg: %s", error->getErrText());
      // Free memory
      free(const_cast<char*>(obj->Arg1));
      free(const_cast<char*>(obj->Arg2));
      delete obj;
    } else if (req_proto.type() == RequestProto_OperationType_CHMOD) {
      // chmod request
      error.reset(utils::GetXrdOucErrInfo(req_proto.chmod().error()));
      client = utils::GetXrdSecEntity(req_proto.chmod().client());
      ret = gOFS->chmod(req_proto.chmod().path().c_str(),
                        (XrdSfsMode)req_proto.chmod().mode(),
                        *error.get(), client, req_proto.chmod().opaque().c_str());
      eos_debug("chmod error msg: %s", error->getErrText());
    } else if (req_proto.type() == RequestProto_OperationType_CHKSUM) {
      // chksum request
      error.reset(utils::GetXrdOucErrInfo(req_proto.chksum().error()));
      client = utils::GetXrdSecEntity(req_proto.chksum().client());
      ret = gOFS->chksum((csFunc) req_proto.chksum().func(),
                         req_proto.chksum().csname().c_str(),
                         req_proto.chksum().path().c_str(),
                         *error.get(), client,
                         req_proto.chksum().opaque().c_str());
      eos_debug("chksum error msg: %s", error->getErrText());
    } else if (req_proto.type() == RequestProto_OperationType_EXISTS) {
      // exists request
      XrdSfsFileExistence exists_flag;
      error.reset(utils::GetXrdOucErrInfo(req_proto.exists().error()));
      client = utils::GetXrdSecEntity(req_proto.exists().client());
      ret = gOFS->exists(req_proto.exists().path().c_str(),
                         exists_flag, *error.get(), client,
                         req_proto.exists().opaque().c_str());
      // Set the status of the exists for the request
      std::ostringstream sstr;
      sstr << (int)exists_flag;
      resp.set_message(sstr.str().c_str());
      eos_debug("exists error msg: %s", error->getErrText());
    } else if (req_proto.type() == RequestProto_OperationType_MKDIR) {
      // mkdir request
      error.reset(utils::GetXrdOucErrInfo(req_proto.mkdir().error()));
      client = utils::GetXrdSecEntity(req_proto.mkdir().client());
      ret = gOFS->mkdir(req_proto.mkdir().path().c_str(),
                        (XrdSfsMode)req_proto.mkdir().mode(),
                        *error.get(), client, req_proto.mkdir().opaque().c_str(), 0);
      eos_debug("mkdir error msg: %s", error->getErrText());
    } else if (req_proto.type() == RequestProto_OperationType_REMDIR) {
      // remdir request
      error.reset(utils::GetXrdOucErrInfo(req_proto.remdir().error()));
      client = utils::GetXrdSecEntity(req_proto.remdir().client());
      ret = gOFS->remdir(req_proto.remdir().path().c_str(),
                         *error.get(), client, req_proto.remdir().opaque().c_str());
      eos_debug("remdir error msg: %s", error->getErrText());
    } else if (req_proto.type() == RequestProto_OperationType_REM) {
      // rem request
      error.reset(utils::GetXrdOucErrInfo(req_proto.rem().error()));
      client = utils::GetXrdSecEntity(req_proto.rem().client());
      ret = gOFS->rem(req_proto.rem().path().c_str(),
                      *error.get(), client, req_proto.rem().opaque().c_str());
      eos_debug("rem error msg: %s", error->getErrText());
    } else if (req_proto.type() == RequestProto_OperationType_RENAME) {
      // rename request
      error.reset(utils::GetXrdOucErrInfo(req_proto.rename().error()));
      client = utils::GetXrdSecEntity(req_proto.rename().client());
      ret = gOFS->rename(req_proto.rename().oldname().c_str(),
                         req_proto.rename().newname().c_str(),
                         *error.get(), client,
                         req_proto.rename().opaqueo().c_str(),
                         req_proto.rename().opaquen().c_str());
      eos_debug("rename error msg: %s", error->getErrText());
    } else if (req_proto.type() == RequestProto_OperationType_PREPARE) {
      // prepare request
      error.reset(utils::GetXrdOucErrInfo(req_proto.prepare().error()));
      client = utils::GetXrdSecEntity(req_proto.prepare().client());
      XrdSfsPrep* pargs = utils::GetXrdSfsPrep(req_proto.prepare().pargs());
      ret = gOFS->prepare(*pargs, *error.get(), client);
      eos_debug("prepare error msg: %s", error->getErrText());
      delete pargs;
    } else if (req_proto.type() == RequestProto_OperationType_TRUNCATE) {
      // truncate request
      error.reset(utils::GetXrdOucErrInfo(req_proto.truncate().error()));
      client = utils::GetXrdSecEntity(req_proto.truncate().client());
      ret = gOFS->truncate(req_proto.truncate().path().c_str(),
                           (XrdSfsFileOffset)req_proto.truncate().fileoffset(),
                           *error.get(), client,
                           req_proto.truncate().opaque().c_str());
      eos_debug("truncate error msg: %s", error->getErrText());
    } else if (req_proto.type() == RequestProto_OperationType_DIROPEN) {
      // dir open request
      mMutexDirs.Lock();

      if (mMapDirs.count(req_proto.diropen().uuid())) {
        mMutexDirs.UnLock();
        eos_debug("dir:%s is already in mapping", req_proto.diropen().name().c_str());
        ret = SFS_OK;
      } else {
        mMutexDirs.UnLock();
        XrdMgmOfsDirectory* dir = static_cast<XrdMgmOfsDirectory*>(
                                    gOFS->newDir((char*)req_proto.diropen().user().c_str(),
                                        req_proto.diropen().monid()));
        client = utils::GetXrdSecEntity(req_proto.diropen().client());
        ret = dir->open(req_proto.diropen().name().c_str(), client,
                        req_proto.diropen().opaque().c_str());

        if (ret == SFS_OK) {
          XrdSysMutexHelper scope_lock(mMutexDirs);
          mMapDirs.insert(std::make_pair(req_proto.diropen().uuid(), dir));
        } else {
          delete dir;
        }
      }
    } else if (req_proto.type() == RequestProto_OperationType_DIRFNAME) {
      // get directory name
      mMutexDirs.Lock();
      auto iter = mMapDirs.find(req_proto.dirfname().uuid());

      if (iter == mMapDirs.end()) {
        mMutexDirs.UnLock();
        eos_err("directory not found in map for reading the name");
        ret = SFS_ERROR;
      } else {
        // Fill in particular info for the directory name
        XrdMgmOfsDirectory* dir = iter->second;
        mMutexDirs.UnLock();

        if (dir->FName()) {
          resp.set_message(dir->FName(), strlen(dir->FName()));
        } else {
          resp.set_message("");
        }

        ret = SFS_OK;
      }
    } else if (req_proto.type() == RequestProto_OperationType_DIRREAD) {
      // read next entry from directory
      mMutexDirs.Lock();
      auto iter =  mMapDirs.find(req_proto.dirread().uuid());

      if (iter == mMapDirs.end()) {
        mMutexDirs.UnLock();
        eos_err("directory not found in map for reading next entry");
        ret = SFS_ERROR;
      } else {
        XrdMgmOfsDirectory* dir = iter->second;
        mMutexDirs.UnLock();
        const char* entry = dir->nextEntry();

        // Fill in particular info for next entry request
        if (entry) {
          resp.set_message(entry, strlen(entry));
          ret = SFS_OK;
        } else  {
          // If no more entries send SFS_ERROR
          ret = SFS_ERROR;
        }
      }
    } else if (req_proto.type() == RequestProto_OperationType_DIRCLOSE) {
      // close directory
      mMutexDirs.Lock();
      auto iter = mMapDirs.find(req_proto.dirclose().uuid());

      if (iter == mMapDirs.end()) {
        mMutexDirs.UnLock();
        eos_err("directory not found in map for closing it");
        ret = SFS_ERROR;
      } else {
        // close directory and remove from mapping
        XrdMgmOfsDirectory* dir = iter->second;
        mMapDirs.erase(iter);
        mMutexDirs.UnLock();
        dir->close();
        delete dir;
        ret = SFS_OK;
      }
    } else if (req_proto.type() == RequestProto_OperationType_FILEOPEN) {
      mMutexFiles.Lock();

      if (mMapFiles.count(req_proto.fileopen().uuid())) {
        mMutexFiles.UnLock();
        eos_debug("file:%s is already in mapping", req_proto.fileopen().name().c_str());
        ret = SFS_OK;
      } else {
        // file open request
        mMutexFiles.UnLock();
        XrdMgmOfsFile* file = static_cast<XrdMgmOfsFile*>(
                                gOFS->newFile((char*)req_proto.fileopen().user().c_str(),
                                              req_proto.fileopen().monid()));
        client = utils::GetXrdSecEntity(req_proto.fileopen().client());
        ret = file->open(req_proto.fileopen().name().c_str(),
                         req_proto.fileopen().openmode(),
                         (mode_t) req_proto.fileopen().createmode(),
                         client, req_proto.fileopen().opaque().c_str());
        error.reset(new XrdOucErrInfo());
        error->setErrInfo(file->error.getErrInfo(), file->error.getErrText());

        if (ret == SFS_OK) {
          XrdSysMutexHelper scope_lock(mMutexFiles);
          mMapFiles.insert(std::make_pair(req_proto.fileopen().uuid(), file));
        } else {
          // Drop the file object since we redirected to the FST node or if
          // there was an error we will not receive a close so we might as well
          // clean it up now
          delete file;
        }
      }
    } else if (req_proto.type() == RequestProto_OperationType_FILESTAT) {
      // file stat request
      struct stat buf;
      mMutexFiles.Lock();
      auto iter =  mMapFiles.find(req_proto.filestat().uuid());

      if (iter == mMapFiles.end()) {
        mMutexFiles.UnLock();
        eos_err("file not found in map for stat");
        memset(&buf, 0, sizeof(struct stat));
        ret = SFS_ERROR;
      } else {
        XrdMgmOfsFile* file = iter->second;
        mMutexFiles.UnLock();
        ret = file->stat(&buf);

        if (ret == SFS_ERROR) {
          error.reset(new XrdOucErrInfo());
          error->setErrInfo(file->error.getErrInfo(), file->error.getErrText());
        }
      }

      resp.set_message(&buf, sizeof(struct stat));
    } else if (req_proto.type() == RequestProto_OperationType_FILEFNAME) {
      // file fname request
      mMutexFiles.Lock();
      auto iter = mMapFiles.find(req_proto.filefname().uuid());

      if (iter == mMapFiles.end()) {
        mMutexFiles.UnLock();
        eos_err("file not found in map for fname call");
        ret = SFS_ERROR;
      } else {
        XrdMgmOfsFile* file = iter->second;
        mMutexFiles.UnLock();

        if (file->FName()) {
          resp.set_message(file->FName());
        } else {
          resp.set_message("");
        }

        ret = SFS_OK;
      }
    } else if (req_proto.type() == RequestProto_OperationType_FILEREAD) {
      // file read request
      mMutexFiles.Lock();
      auto iter =  mMapFiles.find(req_proto.fileread().uuid());

      if (iter == mMapFiles.end()) {
        mMutexFiles.UnLock();
        eos_err("file not found in map for read");
        ret = SFS_ERROR;
      } else {
        XrdMgmOfsFile* file = iter->second;
        mMutexFiles.UnLock();
        resp.mutable_message()->resize(req_proto.fileread().length());
        ret = file->read((XrdSfsFileOffset)req_proto.fileread().offset(),
                         (char*)resp.mutable_message()->c_str(),
                         (XrdSfsXferSize)req_proto.fileread().length());

        if (ret == SFS_ERROR) {
          error.reset(new XrdOucErrInfo());
          error->setErrInfo(file->error.getErrInfo(), file->error.getErrText());
        } else {
          resp.mutable_message()->resize(ret);
        }
      }
    } else if (req_proto.type() == RequestProto_OperationType_FILEWRITE) {
      // file write request
      mMutexFiles.Lock();
      auto iter = mMapFiles.find(req_proto.filewrite().uuid());

      if (iter == mMapFiles.end()) {
        mMutexFiles.UnLock();
        eos_err("file not found in map for write");
        ret = SFS_ERROR;
      } else {
        XrdMgmOfsFile* file = iter->second;
        mMutexFiles.UnLock();
        ret = file->write(req_proto.filewrite().offset(),
                          req_proto.filewrite().buff().c_str(),
                          req_proto.filewrite().length());
      }
    } else if (req_proto.type() == RequestProto_OperationType_FILECLOSE) {
      // close file
      mMutexFiles.Lock();
      auto iter = mMapFiles.find(req_proto.fileclose().uuid());

      if (iter == mMapFiles.end()) {
        mMutexFiles.UnLock();
        eos_err("file not found in map for closing it");
        ret = SFS_ERROR;
      } else {
        // close file and remove from mapping
        XrdMgmOfsFile* file = iter->second;
        mMapFiles.erase(iter);
        mMutexFiles.UnLock();
        ret = file->close();
        delete file;
      }
    } else {
      eos_debug("no such operation supported");
      continue;
    }

    // Free memory
    if (client) {
      utils::DeleteXrdSecEntity(client);
    }

    // Add error object only if it exists
    if (error.get()) {
      XrdOucErrInfoProto* err_proto = resp.mutable_error();
      utils::ConvertToProtoBuf(error.get(), err_proto);
    }

    // Construct and send response to the requester
    resp.set_response(ret);
    int reply_size = resp.ByteSizeLong();
    zmq::message_t reply(reply_size);
    google::protobuf::io::ArrayOutputStream aos(reply.data(), reply_size);
    resp.SerializeToZeroCopyStream(&aos);
    // Try to send out the reply
    bool reset_socket = false;
    int num_retries = 40;

    try {
      do {
        done = responder->send(reply, ZMQ_NOBLOCK);
        num_retries--;
      } while (!done && (num_retries > 0));
    } catch (zmq::error_t& e) {
      if (e.num() == ETERM) {
        eos_warning("msg=\"worker termination requested\" tid=%08x",
                    XrdSysThread::ID());
        delete responder;
        return;
      }

      eos_err("socket error: %s", e.what());
      reset_socket = true;
    }

    if ((num_retries <= 0) || reset_socket) {
      if (!ConnectToBackend(responder)) {
        eos_err("msg=\"kill thread as we could not connect to backend socket\"");
        delete responder;
        return;
      }
    }

    time_end = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>
                    (time_end - time_start);
    AuthCollectInfo(req_proto.type(), duration.count());
  }

  delete responder;
}

//------------------------------------------------------------------------------
// Check that the ProtocolBuffers message has not been tampered with
//------------------------------------------------------------------------------
bool
XrdMgmOfs::ValidAuthRequest(eos::auth::RequestProto* reqProto)
{
  std::string smsg;
  std::string recv_hmac = reqProto->hmac();
  reqProto->set_hmac("");

  // Compute hmac value of the message ignoring the hmac
  if (!reqProto->SerializeToString(&smsg)) {
    eos_static_err("unable to serialize message to string for HMAC computation");
    return false;
  }

  std::string comp_hmac = eos::common::SymKey::HmacSha1(smsg);
  XrdOucString base64hmac;
  bool do_encoding = eos::common::SymKey::Base64Encode((char*)comp_hmac.c_str(),
                     comp_hmac.length(), base64hmac);

  if (!do_encoding) {
    eos_err("unable to do base64encoding on hmac");
    return do_encoding;
  }

  eos_debug("comp_hmac=%s comp_size=%i, recv_hmac=%s, recv_size=%i key=%s",
            base64hmac.c_str(), base64hmac.length(), recv_hmac.c_str(),
            recv_hmac.length(), eos::common::gSymKeyStore.GetCurrentKey()->GetKey64());

  if (((size_t)base64hmac.length() != recv_hmac.length()) ||
      strncmp(base64hmac.c_str(), recv_hmac.c_str(), base64hmac.length())) {
    eos_err("computed HMAC different from the received one, this message"
            "has been tampered with ... ");
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Collect statistics for authentication response times
//------------------------------------------------------------------------------
void
XrdMgmOfs::AuthCollectInfo(eos::auth::RequestProto_OperationType op,
                           std::int64_t ms_duration)
{
  auto now = std::chrono::steady_clock::now();
  std::lock_guard<std::mutex> lock(mAuthStatsMutex);

  if (std::chrono::duration_cast<std::chrono::minutes>(now -
      mLastTimestamp).count() >= 1) {
    mLastTimestamp = now;

    // Push all accumulated samples
    for (auto it = mAuthSamples.begin(); it != mAuthSamples.end(); ++it) {
      if (!it->second.empty()) {
        AuthUpdateAggregate(mAuthAggregate[op], it->second);
        it->second.clear();
      }
    }

    std::string info = AuthPrintStatistics();
    eos_info("msg=\"authentication statistics\" data=\"%s\"", info.c_str());
  } else {
    // Append new measurement
    mAuthSamples[op].push_back(ms_duration);
  }
}

//------------------------------------------------------------------------------
// Compute stats for the provided samples
//------------------------------------------------------------------------------
XrdMgmOfs::AuthStats
XrdMgmOfs::AuthComputeStats(const std::list<std::int64_t>& lst_samples) const
{
  AuthStats stats;
  stats.mNumSamples = 0;
  stats.mMax = 0;
  stats.mMin = std::numeric_limits<std::int64_t>::max();
  stats.mMean = stats.mVariance = 0;
  double sum = 0, sq_sum = 0;
  std::int64_t elem;

  for (auto it = lst_samples.begin(); it != lst_samples.end(); ++it) {
    elem = *it;

    if (elem > stats.mMax) {
      stats.mMax = elem;
    }

    if (elem < stats.mMin) {
      stats.mMin = elem;
    }

    sum += elem;
    sq_sum += elem * elem;
    ++stats.mNumSamples;
  }

  if (stats.mNumSamples) {
    stats.mMean = sum / stats.mNumSamples;
    stats.mVariance = sq_sum / stats.mNumSamples - stats.mMean * stats.mMean;
  }

  return stats;
}

//------------------------------------------------------------------------------
// Update aggregate info with the latest samples
//------------------------------------------------------------------------------
void
XrdMgmOfs::AuthUpdateAggregate(AuthStats& stats,
                               const std::list<std::int64_t>& lst_samples) const
{
  if (stats.mNumSamples == 0) {
    stats = AuthComputeStats(lst_samples);
    return;
  }

  AuthStats tmp = AuthComputeStats(lst_samples);
  double new_mean = (stats.mNumSamples * stats.mMean + tmp.mNumSamples *
                     tmp.mMean) /
                    (stats.mNumSamples + tmp.mNumSamples);
  stats.mVariance =
    (stats.mNumSamples * (stats.mVariance + std::pow(stats.mMean, 2)) +
     tmp.mNumSamples * (tmp.mVariance + std::pow(tmp.mMean, 2))) /
    (stats.mNumSamples + tmp.mNumSamples) - std::pow(new_mean, 2);
  stats.mMean = new_mean;
  stats.mNumSamples += tmp.mNumSamples;

  if (stats.mMax < tmp.mMax) {
    stats.mMax = tmp.mMax;
  }

  if (stats.mMin > tmp.mMin) {
    stats.mMin = tmp.mMin;
  }
}

//----------------------------------------------------------------------------
// Print statistics about authentication performance - needs to be called
// with the mutex locked
//----------------------------------------------------------------------------
std::string
XrdMgmOfs::AuthPrintStatistics() const
{
  std::ostringstream oss;

  for (auto it = mAuthAggregate.begin(); it != mAuthAggregate.end(); ++it) {
    oss << "op=" << it->first << "&"
        << "samples=" << it->second.mNumSamples << "&"
        << "max=" << it->second.mMax << "ms&"
        << "min="  << it->second.mMin << "ms&"
        << "mean=" << it->second.mMean << "ms&"
        << "std_dev=" << std::sqrt(it->second.mVariance) << "&";
  }

  return oss.str();
}
