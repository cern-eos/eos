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


//------------------------------------------------------------------------------
// Authentication master thread startup function
//------------------------------------------------------------------------------
void*
XrdMgmOfs::StartAuthMasterThread(void *pp)
{
  XrdMgmOfs* ofs = static_cast<XrdMgmOfs*>(pp);
  ofs->AuthMasterThread();
  return 0;
}


//----------------------------------------------------------------------------
// Authentication master thread function - accepts requests from EOS AUTH
// plugins which he then forwards to worker threads.
//----------------------------------------------------------------------------
void
XrdMgmOfs::AuthMasterThread ()
{
  // Socket facing clients
  zmq::socket_t frontend(*mZmqContext, ZMQ_ROUTER);
  std::ostringstream sstr;
  sstr << "tcp://*:" << mFrontendPort;
  frontend.bind(sstr.str().c_str());

  // Socket facing worker threads
  zmq::socket_t backend(*mZmqContext, ZMQ_DEALER);
  backend.bind("inproc://authbackend");

  // Start the proxy
  zmq_device(ZMQ_QUEUE, frontend, backend);
}


//------------------------------------------------------------------------------
// Authentication worker thread startup function
//------------------------------------------------------------------------------
void*
XrdMgmOfs::StartAuthWorkerThread(void *pp)
{
  XrdMgmOfs* ofs = static_cast<XrdMgmOfs*>(pp);
  ofs->AuthWorkerThread();
  return 0;
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
  eos_static_info("authentication worker thread started");
  zmq::socket_t responder(*mZmqContext, ZMQ_REP);

  // Try to connect to proxy thread - the bind can take a longer time so threfore
  // keep trying until it is successful
  while (1)
  {
        try
        {
          responder.connect("inproc://authbackend");
        }
        catch (zmq::error_t& err)
        {
          eos_static_debug("auth worker connection failed - retry");
          continue;
        }

        break;
  }

  // Main loop of the worker thread
  while (1)
  {
    zmq::message_t request;

    // Wait for next request
    responder.recv(&request);

    // Read in the ProtocolBuffer object just received
    std::string msg_recv((char*)request.data(), request.size());
    RequestProto req_proto;
    req_proto.ParseFromString(msg_recv);

    ResponseProto resp;
    std::shared_ptr<XrdOucErrInfo> error(static_cast<XrdOucErrInfo*>(0));
    XrdSecEntity* client = 0;

    if (!ValidAuthRequest(&req_proto))
    {
      eos_err("message HMAC received is not valid, dropping request");
      error.reset(new XrdOucErrInfo("admin"));
      error.get()->setErrInfo(EKEYREJECTED, "request HMAC value is wrong");
      ret = SFS_ERROR;
    }
    else if (req_proto.type() == RequestProto_OperationType_STAT)
    {
      // stat request
      struct stat buf;
      error.reset(utils::GetXrdOucErrInfo(req_proto.stat().error()));
      client = utils::GetXrdSecEntity(req_proto.stat().client());
      ret = gOFS->stat(req_proto.stat().path().c_str(), &buf,
                       *error.get(), client, req_proto.stat().opaque().c_str());

      // Fill in particular info for stat request
      resp.set_message(&buf, sizeof(struct stat));
      eos_debug("stat error msg: %s", error->getErrText());
    }
    else if (req_proto.type() == RequestProto_OperationType_STATM)
    {
      // stat mode request
      mode_t mode;
      error.reset(utils::GetXrdOucErrInfo(req_proto.stat().error()));
      client = utils::GetXrdSecEntity(req_proto.stat().client());
      ret = gOFS->stat(req_proto.stat().path().c_str(), mode,
                       *error.get(), client, req_proto.stat().opaque().c_str());

      // Fill in particular info for stat request
      resp.set_message(&mode, sizeof(mode_t));
      eos_debug("statm error msg: %s", error->getErrText());
    }
    else if (req_proto.type() == RequestProto_OperationType_FSCTL1)
    {
      // fsctl request
      error.reset(utils::GetXrdOucErrInfo(req_proto.fsctl1().error()));
      client = utils::GetXrdSecEntity(req_proto.fsctl1().client());
      ret = gOFS->fsctl(req_proto.fsctl1().cmd(), req_proto.fsctl1().args().c_str(),
                        *error.get(), client);
      eos_debug("fsctl error msg: %s", error->getErrText());
    }
    else if (req_proto.type() == RequestProto_OperationType_FSCTL2)
    {
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
    }

    else if (req_proto.type() == RequestProto_OperationType_CHMOD)
    {
      // chmod request
      error.reset(utils::GetXrdOucErrInfo(req_proto.chmod().error()));
      client = utils::GetXrdSecEntity(req_proto.chmod().client());
      ret = gOFS->chmod(req_proto.chmod().path().c_str(), (XrdSfsMode)req_proto.chmod().mode(),
                        *error.get(), client, req_proto.chmod().opaque().c_str());
      eos_debug("chmod error msg: %s", error->getErrText());
    }
    else if (req_proto.type() == RequestProto_OperationType_CHKSUM)
    {
      // chksum request
      error.reset(utils::GetXrdOucErrInfo(req_proto.chksum().error()));
      client = utils::GetXrdSecEntity(req_proto.chksum().client());
      ret = gOFS->chksum((csFunc) req_proto.chksum().func(),
                         req_proto.chksum().csname().c_str(),
                         req_proto.chksum().path().c_str(),
                         *error.get(), client,
                         req_proto.chksum().opaque().c_str());
      eos_debug("chksum error msg: %s", error->getErrText());
    }
    else if (req_proto.type() == RequestProto_OperationType_EXISTS)
    {
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
    }
    else if (req_proto.type() == RequestProto_OperationType_MKDIR)
    {
      // mkdir request
      error.reset(utils::GetXrdOucErrInfo(req_proto.mkdir().error()));
      client = utils::GetXrdSecEntity(req_proto.mkdir().client());
      ret = gOFS->mkdir(req_proto.mkdir().path().c_str(),
                        (XrdSfsMode)req_proto.mkdir().mode(),
                        *error.get(), client, req_proto.mkdir().opaque().c_str());
      eos_debug("mkdir error msg: %s", error->getErrText());
    }
    else if (req_proto.type() == RequestProto_OperationType_REMDIR)
    {
      // remdir request
      error.reset(utils::GetXrdOucErrInfo(req_proto.remdir().error()));
      client = utils::GetXrdSecEntity(req_proto.remdir().client());
      ret = gOFS->remdir(req_proto.remdir().path().c_str(),
                         *error.get(), client, req_proto.remdir().opaque().c_str());
      eos_debug("remdir error msg: %s", error->getErrText());
    }
    else if (req_proto.type() == RequestProto_OperationType_REM)
    {
      // rem request
      error.reset(utils::GetXrdOucErrInfo(req_proto.rem().error()));
      client = utils::GetXrdSecEntity(req_proto.rem().client());
      ret = gOFS->rem(req_proto.rem().path().c_str(),
                      *error.get(), client, req_proto.rem().opaque().c_str());
      eos_debug("rem error msg: %s", error->getErrText());
    }
    else if (req_proto.type() == RequestProto_OperationType_RENAME)
    {
      // rename request
      error.reset(utils::GetXrdOucErrInfo(req_proto.rename().error()));
      client = utils::GetXrdSecEntity(req_proto.rename().client());
      ret = gOFS->rename(req_proto.rename().oldname().c_str(),
                         req_proto.rename().newname().c_str(),
                         *error.get(), client,
                         req_proto.rename().opaqueo().c_str(),
                         req_proto.rename().opaquen().c_str());
      eos_debug("rename error msg: %s", error->getErrText());
    }
    else if (req_proto.type() == RequestProto_OperationType_PREPARE)
    {
      // prepare request
      error.reset(utils::GetXrdOucErrInfo(req_proto.prepare().error()));
      client = utils::GetXrdSecEntity(req_proto.prepare().client());
      XrdSfsPrep* pargs = utils::GetXrdSfsPrep(req_proto.prepare().pargs());
      ret = gOFS->prepare(*pargs, *error.get(), client);
      eos_debug("prepare error msg: %s", error->getErrText());
      delete pargs;
    }
    else if (req_proto.type() == RequestProto_OperationType_TRUNCATE)
    {
      // truncate request
      error.reset(utils::GetXrdOucErrInfo(req_proto.truncate().error()));
      client = utils::GetXrdSecEntity(req_proto.truncate().client());
      ret = gOFS->truncate(req_proto.truncate().path().c_str(),
                           (XrdSfsFileOffset)req_proto.truncate().fileoffset(),
                           *error.get(), client,
                           req_proto.truncate().opaque().c_str());
      eos_debug("truncate error msg: %s", error->getErrText());
    }
    else if (req_proto.type() == RequestProto_OperationType_DIROPEN)
    {
      // dir open request
      if (mMapDirs.count(req_proto.diropen().uuid()))
      {
        eos_debug("dir:%s is already in mapping", req_proto.diropen().name().c_str());
        ret = SFS_OK;
      }
      else
      {
        XrdMgmOfsDirectory* dir = static_cast<XrdMgmOfsDirectory*>(
            gOFS->newDir((char*)req_proto.diropen().user().c_str(),
                         req_proto.diropen().monid()));

        client = utils::GetXrdSecEntity(req_proto.diropen().client());
        ret = dir->open(req_proto.diropen().name().c_str(), client,
                        req_proto.diropen().opaque().c_str());

        if (ret == SFS_OK)
        {
          XrdSysMutexHelper scope_lock(mMutexDirs);
          //auto result = mMapDirs.insert(std::make_pair(req_proto.diropen().uuid(), dir));
          mMapDirs.insert(std::make_pair(req_proto.diropen().uuid(), dir));
        }
        else
        {
          delete dir;
        }
      }
    }
    else if (req_proto.type() == RequestProto_OperationType_DIRFNAME)
    {
      // get directory name
      auto iter = mMapDirs.end();
      {
        XrdSysMutexHelper scope_lock(mMutexDirs);
        iter = mMapDirs.find(req_proto.dirfname().uuid());
      }

      if (iter == mMapDirs.end())
      {
        eos_err("directory not found in map for reading the name");
        ret = SFS_ERROR;
      }
      else {
        // Fill in particular info for the directory name
        XrdMgmOfsDirectory* dir = iter->second;
        resp.set_message(dir->FName(), strlen(dir->FName()));
        ret = SFS_OK;
      }
    }
    else if (req_proto.type() == RequestProto_OperationType_DIRREAD)
    {
      // read next entry from directory
      auto iter = mMapDirs.end();
      {
        XrdSysMutexHelper scope_lock(mMutexDirs);
        iter = mMapDirs.find(req_proto.dirread().uuid());
      }

      if (iter == mMapDirs.end())
      {
        eos_err("directory not found in map for reading next entry");
        ret = SFS_ERROR;
      }
      else {
        XrdMgmOfsDirectory* dir = iter->second;
        const char* entry = dir->nextEntry();
        // Fill in particular info for next entry request
        if (entry)
        {
          resp.set_message(entry, strlen(entry));
          ret = SFS_OK;
        }
        else
        {
          // If no more entries send SFS_ERROR
          ret = SFS_ERROR;
        }
      }
    }
    else if (req_proto.type() == RequestProto_OperationType_DIRCLOSE)
    {
      // close directory
      auto iter = mMapDirs.end();
      {
        XrdSysMutexHelper scope_lock(mMutexDirs);
        iter = mMapDirs.find(req_proto.dirclose().uuid());
      }

      if (iter == mMapDirs.end())
      {
        eos_err("directory not found in map for closing it");
        ret = SFS_ERROR;
      }
      else {
        // close directory and remove from mapping
        XrdMgmOfsDirectory* dir = iter->second;
        {
          XrdSysMutex scope_lock(mMutexDirs);
          mMapDirs.erase(iter);
        }
        dir->close();
        delete dir;
        ret = SFS_OK;
      }
    }
    else if (req_proto.type() == RequestProto_OperationType_FILEOPEN)
    {
      if (mMapFiles.count(req_proto.fileopen().uuid()))
      {
        eos_debug("file:%s is already in mapping", req_proto.fileopen().name().c_str());
        ret = SFS_OK;
      }
      else
      {
        // file open request
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

        if ((ret == SFS_REDIRECT) || (ret == SFS_ERROR))
        {
          // Drop the file object since we redirected to the FST node or if
          // there was an error we will not receive a close so we might as well
          // clean it up now
          delete file;
        }
        else
        {
          XrdSysMutexHelper scope_lock(mMutexFiles);
          //auto result = mMapFiles.insert(std::make_pair(req_proto.fileopen().uuid(), file));
          mMapFiles.insert(std::make_pair(req_proto.fileopen().uuid(), file));
        }
      }
    }
    else if (req_proto.type() == RequestProto_OperationType_FILESTAT)
    {
      // file stat request
      struct stat buf;
      auto iter = mMapFiles.end();
      {
        XrdSysMutexHelper scope_lock(mMutexFiles);
        iter = mMapFiles.find(req_proto.filestat().uuid());
      }

      if (iter == mMapFiles.end())
      {
        eos_err("file not found in map for stat");
        memset(&buf, 0, sizeof(struct stat));
        ret = SFS_ERROR;
      }
      else
      {
        XrdMgmOfsFile* file = iter->second;
        file->stat(&buf);
        ret = SFS_OK;
      }

      resp.set_message(&buf, sizeof(struct stat));
    }
    else if (req_proto.type() == RequestProto_OperationType_FILEFNAME)
    {
      // file fname request
      auto iter = mMapFiles.end();
      {
        XrdSysMutexHelper scope_lock(mMutexFiles);
        iter = mMapFiles.find(req_proto.filefname().uuid());
      }

      if (iter == mMapFiles.end())
      {
        eos_err("file not found in map for fname call");
        ret = SFS_ERROR;
      }
      else
      {
        XrdMgmOfsFile* file = iter->second;
        resp.set_message(file->FName());
        ret = SFS_OK;
      }
    }
    else if (req_proto.type() == RequestProto_OperationType_FILEREAD)
    {
      // file read request
      auto iter = mMapFiles.end();
      {
        XrdSysMutexHelper scope_lock(mMutexFiles);
        iter = mMapFiles.find(req_proto.fileread().uuid());
      }

      if (iter == mMapFiles.end())
      {
        eos_err("file not found in map for read");
        ret = 0;
      }
      else
      {
        XrdMgmOfsFile* file = iter->second;
        resp.mutable_message()->resize(req_proto.fileread().length());
        ret = file->read((XrdSfsFileOffset)req_proto.fileread().offset(),
                         (char*)resp.mutable_message()->c_str(),
                         (XrdSfsXferSize)req_proto.fileread().length());
        resp.mutable_message()->resize(ret);
      }
    }
    else if (req_proto.type() == RequestProto_OperationType_FILEWRITE)
    {
      // file write request
      auto iter = mMapFiles.end();
      {
        XrdSysMutexHelper scope_lock(mMutexFiles);
        iter = mMapFiles.find(req_proto.filewrite().uuid());
      }

      if (iter == mMapFiles.end())
      {
        eos_err("file not found in map for write");
        ret = 0;
      }
      else
      {
        XrdMgmOfsFile* file = iter->second;
        ret = file->write(req_proto.filewrite().offset(),
                          req_proto.filewrite().buff().c_str(),
                          req_proto.filewrite().length());
      }
    }
    else if (req_proto.type() == RequestProto_OperationType_FILECLOSE)
    {
      // close file
      auto iter = mMapFiles.end();
      {
        XrdSysMutexHelper scope_lock(mMutexFiles);
        iter = mMapFiles.find(req_proto.fileclose().uuid());
      }

      if (iter == mMapFiles.end())
      {
        eos_err("file not found in map for closing it");
        ret = SFS_ERROR;
      }
      else
      {
        // close file and remove from mapping
        XrdMgmOfsFile* file = iter->second;
        {
          XrdSysMutex scope_lock(mMutexFiles);
          mMapFiles.erase(iter);
        }

        ret = file->close();
        delete file;
      }
    }
    else
    {
      eos_debug("no such operation supported");
      continue;
    }

    // Add error object only if it exists
    if (error.get())
    {
      XrdOucErrInfoProto* err_proto = resp.mutable_error();
      utils::ConvertToProtoBuf(error.get(), err_proto);
    }

    // Construct and send response to the requester
    resp.set_response(ret);
    int reply_size = resp.ByteSize();
    zmq::message_t reply(reply_size);
    google::protobuf::io::ArrayOutputStream aos(reply.data(), reply_size);

    resp.SerializeToZeroCopyStream(&aos);
    responder.send(reply, ZMQ_NOBLOCK);

    // Free memory
    if (client)
      utils::DeleteXrdSecEntity(client);
  }
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
  if (!reqProto->SerializeToString(&smsg))
  {
    eos_err("unable to serialize to string message");
    return false;
  }

  std::string key = eos::common::gSymKeyStore.GetCurrentKey()->GetKey();
  std::string comp_hmac = eos::common::SymKey::HmacSha1(key, smsg);
  XrdOucString base64hmac;
  bool do_encoding = eos::common::SymKey::Base64Encode((char*)comp_hmac.c_str(),
                                                       comp_hmac.length(), base64hmac);

  if (!do_encoding)
  {
    eos_err("unable to do base64encoding on hmac");
    return do_encoding;
  }

  eos_info("comp_hmac=%s comp_size=%i, recv_hmac=%s, recv_size=%i",
           base64hmac.c_str(), base64hmac.length(), recv_hmac.c_str(), recv_hmac.length());

  if (((size_t)base64hmac.length() != recv_hmac.length()) ||
      strncmp(base64hmac.c_str(), recv_hmac.c_str(), base64hmac.length()))
  {
    eos_err("computed HMAC different from the received one, this message"
            "has been tampered with ... ");
    return false;
  }

  return true;
}
