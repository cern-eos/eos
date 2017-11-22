//------------------------------------------------------------------------------
//! @file backend.cc
//! @author Andreas-Joachim Peters CERN
//! @brief backend IO handling class
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
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

#include "backend/backend.hh"
#include "cap/cap.hh"
#include "misc/fusexrdlogin.hh"
#include "eosfuse.hh"
#include "common/Logging.hh"
#include "common/StringConversion.hh"
#include "common/SymKeys.hh"
#include "XrdCl/XrdClFile.hh"
#include "XrdCl/XrdClURL.hh"

#ifndef EOSCITRINE
#include "fuse/SyncResponseHandler.hh"
#endif

/* -------------------------------------------------------------------------- */
backend::backend()
/* -------------------------------------------------------------------------- */
{
}

/* -------------------------------------------------------------------------- */
backend::~backend()
/* -------------------------------------------------------------------------- */
{
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
backend::init(std::string& _hostport, std::string& _remotemountdir)
/* -------------------------------------------------------------------------- */
{
  hostport = _hostport;
  mount = _remotemountdir;

  if ((mount.length() && (mount.at(mount.length() - 1) == '/'))) {
    mount.erase(mount.length() - 1);
  }

  return 0;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
backend::mapErrCode(int retc)
{
  switch (retc) {
  case 0:
    break;

  case kXR_ArgInvalid:
    retc = EINVAL;
    break;

  case kXR_ArgMissing:
    retc = EINVAL;
    break;

  case kXR_ArgTooLong:
    retc = E2BIG;
    break;

  case kXR_FileNotOpen:
    retc = EBADF;
    break;

  case kXR_FSError:
    retc = EIO;
    break;

  case kXR_InvalidRequest:
    retc = EEXIST;
    break;

  case kXR_IOError:
    retc = EIO;
    break;

  case kXR_NoMemory:
    retc = ENOMEM;
    break;

  case kXR_NoSpace:
    retc = ENOSPC;
    break;

  case kXR_ServerError:
    retc = EIO;
    break;

  case kXR_NotAuthorized:
    retc = EACCES;
    break;

  case kXR_NotFound:
    retc = ENOENT;
    break;

  case kXR_Unsupported:
    retc = ENOTSUP;
    break;

  case kXR_NotFile:
    retc = EISDIR;
    break;

  case kXR_isDirectory:
    retc = EISDIR;
    break;

  case kXR_Cancelled:
    retc = ECANCELED;
    break;

  case kXR_ChkLenErr:
    retc = ERANGE;
    break;

  case kXR_ChkSumErr:
    retc = ERANGE;
    break;

  case kXR_inProgress:
    retc = EAGAIN;
    break;

  default:
    retc = EIO;
  }

  return retc;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
backend::getMD(fuse_req_t req,
               const std::string& path,
               std::vector<eos::fusex::container>& contv,
               std::string authid
              )
/* -------------------------------------------------------------------------- */
{
  // return's the inode of path in inode and rc=0 for success, otherwise errno
  std::string requestURL = getURL(req, path, "LS", authid);
  return fetchResponse(requestURL, contv);
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
backend::getMD(fuse_req_t req,
               uint64_t inode,
               const std::string& name,
               std::vector<eos::fusex::container>& contv,
               bool listing,
               std::string authid
              )
{
  std::string requestURL = getURL(req, inode, name, listing ? "LS" : "GET",
                                  authid);
  return fetchResponse(requestURL, contv);
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
backend::getMD(fuse_req_t req,
               uint64_t inode,
               uint64_t myclock,
               std::vector<eos::fusex::container>& contv,
               bool listing,
               std::string authid
              )
/* -------------------------------------------------------------------------- */
{
  std::string requestURL = getURL(req, inode, myclock, listing ? "LS" : "GET",
                                  authid);
  return fetchResponse(requestURL, contv);
}

/* -------------------------------------------------------------------------- */
int
backend::getCAP(fuse_req_t req,
                uint64_t inode,
                std::vector<eos::fusex::container>& contv
               )
/* -------------------------------------------------------------------------- */
{
  uint64_t myclock = (uint64_t) time(NULL);
  std::string requestURL = getURL(req, inode, myclock, "GETCAP");
  return fetchResponse(requestURL, contv);
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
backend::fetchResponse(std::string& requestURL,
                       std::vector<eos::fusex::container>& contv
                      )
/* -------------------------------------------------------------------------- */
{
  eos_static_debug("request='%s'", requestURL.c_str());
  double total_exec_time_sec = 0;
  XrdCl::File file;
  XrdCl::XRootDStatus status;

  do {
    struct timespec ts;
    eos::common::Timing::GetTimeSpec(ts, true);
    // the MD get operation is implemented via a stream: open/read/close
    status = file.Open(requestURL.c_str(),
                       XrdCl::OpenFlags::Flags::Read);
    double exec_time_sec = 1.0 * eos::common::Timing::GetCoarseAgeInNs(&ts,
                           0) / 1000000000.0;
    total_exec_time_sec += exec_time_sec;

    if (!status.IsOK()) {
      // in case of any failure
      if (status.errNo == XErrorCode::kXR_NotFound) {
        // this is just no such file or directory
        eos_static_debug("error=status is NOT ok : %s", status.ToString().c_str());
        errno = ENOENT;
        return ENOENT;
      }

      eos_static_err("fetch-exec-ms=%.02f sum-query-exec-ms=%.02f ok=%d err=%d fatal=%d status-code=%d err-no=%d",
                     exec_time_sec * 1000.0, total_exec_time_sec * 1000.0, status.IsOK(),
                     status.IsError(), status.IsFatal(), status.code, status.errNo);
      eos_static_err("error=status is NOT ok : %s %d %d", status.ToString().c_str(),
                     status.code, status.errNo);

      if (status.code == XrdCl::errAuthFailed) {
        // this is an authentication error which results in permission denied
        errno = EPERM;
        return EPERM;
      }

      std::string xrootderr = status.GetErrorMessage();

      // the xrootd mapping of errno to everything unknwon to EIO is really unfortunate
      if (xrootderr.find("get-cap-clock-out-of-sync") != std::string::npos) {
        // this is a time synchronization error
        errno = EL2NSYNC;
        return EL2NSYNC;
      }

      if (
        (status.code == XrdCl::errSocketTimeout) ||
        (status.code == XrdCl::errOperationExpired)
      ) {
        // if there is a timeout, we might retry according to the backend timeout setting
        if (EosFuse::Instance().Config().options.md_backend_timeout &&
            (total_exec_time_sec >
             EosFuse::Instance().Config().options.md_backend_timeout)) {
          // it took longer than our backend timeout allows
          eos_static_err("giving up fetch after sum-fetch-exec-s=%.02f backend-timeout-s=%.02f",
                         total_exec_time_sec,  EosFuse::Instance().Config().options.md_backend_timeout);
        } else {
          // retry
          XrdSysTimer sleeper;
          sleeper.Snooze(5);
          continue;
        }
      }

      // all the other errors are reported back
      if (status.errNo)
      {
	errno = XrdCl::Proxy::status2errno(status);
	eos_static_err ("error=status is not ok : errno=%d", errno);
	return errno;
      }

      if (status.code) {
        errno = EIO;
        eos_static_err("error=status is not ok : code=%d", errno);
        return errno;
      }
    } else {
      eos_static_debug("fetch-exec-ms=%.02f sum-fetch-exec-ms=%.02f ok=%d err=%d fatal=%d status-code=%d err-no=%d",
                       exec_time_sec * 1000.0, total_exec_time_sec * 1000.0, status.IsOK(),
                       status.IsError(), status.IsFatal(), status.code, status.errNo);
      break;
    }
  } while (1);

  // Start to read
  off_t offset = 0;
  const int kPAGE = 512 * 1024;
  std::string response;
  std::vector<char> rbuff;
  rbuff.reserve(kPAGE);
  uint32_t bytesread = 0;

  do {
    status = file.Read(offset, kPAGE, (char*) & rbuff[0], bytesread);

    if (status.IsOK()) {
      offset += bytesread;
      response.append(&rbuff[0], bytesread);
      eos_static_debug("+response=%s size=%u rsize=%u",
                       response.c_str(),
                       response.size(),
                       rbuff.size());
    } else {
      // failure
      bytesread = 0;
    }

    eos_static_debug("rbytes=%lu offset=%llu", bytesread, offset);
  } while (bytesread);

  eos_static_debug("response-size=%u response=%s",
                   response.size(), response.c_str());
  //eos_static_debug("response-dump=%s", eos::common::StringConversion::string_to_hex(response).c_str());
  offset = 0;
  eos::fusex::container cont;

  do {
    cont.Clear();

    if ((response.size() - offset) > 10) {
      std::string slen = response.substr(1 + offset, 8);
      size_t len = strtoll(slen.c_str(), 0, 16);
      eos_static_debug("len=%llu offset=%llu", len, offset);

      if (!len) {
        eos_static_debug("response had illegal length");
        return EINVAL;
      }

      std::string item;
      item.assign(response.c_str() + offset + 10, len);
      offset += (10 + len);

      if (cont.ParseFromString(item)) {
        eos_static_debug("response parsing OK");

        if ((cont.type() != cont.MD) &&
            (cont.type() != cont.MDMAP) &&
            (cont.type() != cont.CAP)) {
          eos_static_debug("wrong response type");
          return EINVAL;
        }

        contv.push_back(cont);
        eos_static_debug("parsed %ld/%ld", offset, response.size());

        if (offset == (off_t) response.size()) {
          break;
        }
      } else {
        eos_static_debug("response parsing FAILED");
        return EIO;
      }
    } else {
      eos_static_err("fatal protocol parsing error");
      return EINVAL;
    };
  } while (1);

  return 0;
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
backend::putMD(fuse_req_t req, eos::fusex::md* md, std::string authid,
               XrdSysMutex* locker)
{
  fuse_id id(req);
  return putMD(id, md, authid, locker);
}


/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
backend::putMD(const fuse_id& id, eos::fusex::md* md, std::string authid,
               XrdSysMutex* locker)
{
  XrdCl::URL url("root://" + hostport);
  url.SetPath("/dummy");
  XrdCl::URL::ParamsMap query;
  fusexrdlogin::loginurl(url, query, id.uid, id.gid, id.pid, 0);
  query["eos.app"] = "fuse";
  url.SetParams(query);
  // temporary add the authid to be used for that request
  md->set_authid(authid);
  md->set_clientuuid(clientuuid);
  std::string mdstream;
  eos_static_info("proto-serialize");

  if (!md->SerializeToString(&mdstream)) {
    md->clear_authid();
    md->clear_clientuuid();
    md->clear_implied_authid();
    eos_static_err("fatal serialization error");
    return EFAULT;
  }

  if (EOS_LOGS_DEBUG) {
    eos_static_debug("MD:\n%s", EosFuse::Instance().mds.dump_md(*md).c_str());
  }

  md->clear_authid();
  md->clear_clientuuid();
  md->clear_implied_authid();

  if (locker) {
    locker->UnLock();
  }

  eos_static_info("proto-serialize unlock");
  XrdCl::FileSystem fs(url);
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;
  std::string prefix = "/?fusex:";
  arg.Append(prefix.c_str(), prefix.length());
  arg.Append(mdstream.c_str(), mdstream.length());
  eos_static_debug("query: url=%s path=%s length=%d", url.GetURL().c_str(),
                   prefix.c_str(), mdstream.length());
  XrdCl::XRootDStatus status = Query(fs, XrdCl::QueryCode::OpaqueFile, arg,
                                     response);
  eos_static_info("sync-response");
  eos_static_debug("response-size=%d",
                   response ? response->GetSize() : 0);

  if (status.IsOK()) {
    if (response && response->GetBuffer()) {
      // eos_static_debug("response=%s response-size=%d",
      // response->GetBuffer(),
      //        response->GetSize());
      std::string responseprefix;

      if (response->GetSize() > 6) {
        responseprefix.assign(response->GetBuffer(), 6);
        // retrieve response
      } else {
        eos_static_err("protocol error - to short response received");
        delete response;

        if (locker) {
          locker->Lock();
        }

        return EIO;
      }

      if (responseprefix != "Fusex:") {
        eos_static_err("protocol error - fusex: prefix missing in response");
        delete response;

        if (locker) {
          locker->Lock();
        }

        return EIO;
      }

      std::string sresponse;
      std::string b64response;
      b64response.assign(response->GetBuffer() + 6, response->GetSize() - 6);
      eos::common::SymKey::DeBase64(b64response, sresponse);
      eos::fusex::response resp;

      if (!resp.ParseFromString(sresponse) ||
          ((resp.type() != resp.ACK) && (resp.type() != resp.NONE))) {
        eos_static_err("parsing error/wrong response type received");
        delete response;

        if (locker) {
          locker->Lock();
        }

        return EIO;
      }

      if (resp.type() == resp.ACK) {
        if (resp.ack_().code() == resp.ack_().OK) {
          eos_static_info("relock do");

          if (locker) {
            locker->Lock();
          }

          md->set_md_ino(resp.ack_().md_ino());
          eos_static_debug("directory inode %lx => %lx/%lx tid=%lx error='%s'", md->id(),
                           md->md_ino(),
                           resp.ack_().md_ino(), resp.ack_().transactionid(),
                           resp.ack_().err_msg().c_str());
          eos_static_info("relock done");
          delete response;
          return 0;
        }

        eos_static_err("failed query command for ino=%lx error='%s'", md->id(),
                       resp.ack_().err_msg().c_str());

        if (EOS_LOGS_DEBUG) {
          eos_static_err("MD:\n%s", EosFuse::Instance().mds.dump_md(*md).c_str());
        }

        delete response;

        if (locker) {
          locker->Lock();
        }

        return EIO;
      }

      if (resp.type() == resp.NONE) {
        delete response;

        if (locker) {
          locker->Lock();
        }

        return 0;
      }
    } else {
      eos_static_err("no response retrieved response=%lu response-buffer=%lu",
                     response, response ? response->GetBuffer() : 0);

      if (response) {
        delete response;
      }

      if (locker) {
        locker->Lock();
      }

      return EIO;
    }

    if (locker) {
      locker->Lock();
    }

    return 0;
  } else {
    eos_static_err("query resulted in error url=%s", url.GetURL().c_str());

    if (locker) {
      locker->Lock();
    }

    if (status.code == XrdCl::errErrorResponse) {
      return mapErrCode(status.errNo);
    } else {
      return EIO;
    }
  }
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
backend::doLock(fuse_req_t req,
                eos::fusex::md& md,
                XrdSysMutex* locker)
/* -------------------------------------------------------------------------- */
{
  XrdCl::URL url("root://" + hostport);
  url.SetPath("/dummy");
  XrdCl::URL::ParamsMap query;
  fusexrdlogin::loginurl(url, query, req, 0);
  url.SetParams(query);
  md.set_clientuuid(clientuuid);
  std::string mdstream;
  eos_static_info("proto-serialize");

  if (!md.SerializeToString(&mdstream)) {
    md.clear_clientuuid();
    md.clear_flock();
    eos_static_err("fatal serialization error");
    return EFAULT;
  }

  md.clear_clientuuid();
  md.clear_flock();
  locker->UnLock();
  eos_static_info("proto-serialize unlock");
  XrdCl::FileSystem fs(url);
  XrdCl::Buffer arg;
  XrdCl::Buffer* response = 0;
  std::string prefix = "/?fusex:";
  arg.Append(prefix.c_str(), prefix.length());
  arg.Append(mdstream.c_str(), mdstream.length());
  eos_static_debug("query: url=%s path=%s length=%d", url.GetURL().c_str(),
                   prefix.c_str(), mdstream.length());
  XrdCl::XRootDStatus status = Query(fs, XrdCl::QueryCode::OpaqueFile, arg,
                                     response);
  eos_static_info("sync-response");

  if (status.IsOK()) {
    eos_static_debug("response=%s response-size=%d",
                     response ? response->GetBuffer() : "null", response ? response->GetSize() : 0);

    if (response && response->GetBuffer()) {
      std::string responseprefix;

      if (response->GetSize() > 6) {
        responseprefix.assign(response->GetBuffer(), 6);
        // retrieve response
      } else {
        eos_static_err("protocol error - to short response received");
        locker->Lock();
        return EIO;
      }

      if (responseprefix != "Fusex:") {
        eos_static_err("protocol error - fusex: prefix missing in response");
        locker->Lock();
        return EIO;
      }

      std::string sresponse;
      std::string b64response;
      b64response.assign(response->GetBuffer() + 6, response->GetSize() - 6);
      eos::common::SymKey::DeBase64(b64response, sresponse);
      eos::fusex::response resp;

      if (!resp.ParseFromString(sresponse) || (resp.type() != resp.LOCK)) {
        eos_static_err("parsing error/wrong response type received");
        locker->Lock();
        return EIO;
      }

      if (resp.ack_().code() == resp.ack_().OK) {
        eos_static_info("relock do");
        locker->Lock();
        (*(md.mutable_flock())) = (resp.lock_());
        eos_static_debug("directory inode %lx => %lx/%lx tid=%lx error='%s'", md.id(),
                         md.md_ino(),
                         resp.ack_().md_ino(), resp.ack_().transactionid(),
                         resp.ack_().err_msg().c_str());
        eos_static_info("relock done");
        return 0;
      }

      eos_static_err("failed query command for ino=%lx error='%s'", md.id(),
                     resp.ack_().err_msg().c_str());

      if (EOS_LOGS_DEBUG) {
        eos_static_err("MD:\n%s", EosFuse::Instance().mds.dump_md(md).c_str());
      }

      locker->Lock();
      return EIO;
    } else {
      eos_static_err("no response retrieved response=%lu response-buffer=%lu",
                     response, response ? response->GetBuffer() : 0);
      locker->Lock();
      return EIO;
    }
  } else {
    eos_static_err("query resulted in error url=%s", url.GetURL().c_str());
  }

  locker->Lock();
  return EIO;
}

/* -------------------------------------------------------------------------- */
std::string
/* -------------------------------------------------------------------------- */
backend::getURL(fuse_req_t req, const std::string& path, std::string op,
                std::string authid)
/* -------------------------------------------------------------------------- */
{
  XrdCl::URL url("root://" + hostport);
  url.SetPath("/proc/user/");
  XrdCl::URL::ParamsMap query;
  query["mgm.cmd"] = "fuseX";
  query["mgm.clock"] = "0";
  query["mgm.path"] = eos::common::StringConversion::curl_escaped(mount + path);
  query["mgm.op"] = op;
  query["mgm.uuid"] = clientuuid;
  query["mgm.cid"] = cap::capx::getclientid(req);
  query["eos.app"] = "fuse";

  if (authid.length()) {
    query["mgm.authid"] = authid;
  }

  fusexrdlogin::loginurl(url, query, req, 0);
  url.SetParams(query);
  return url.GetURL();
}

/* -------------------------------------------------------------------------- */
std::string
/* -------------------------------------------------------------------------- */
backend::getURL(fuse_req_t req, uint64_t inode, const std::string& name,
                std::string op, std::string authid)
{
  XrdCl::URL url("root://" + hostport);
  url.SetPath("/proc/user/");
  XrdCl::URL::ParamsMap query;
  query["mgm.cmd"] = "fuseX";
  query["mgm.clock"] = "0";
  query["mgm.child"] = name;
  char hexinode[32];
  snprintf(hexinode, sizeof(hexinode), "%08lx", (unsigned long) inode);
  query["mgm.inode"] =
    hexinode;
  query["mgm.op"] = op;
  query["mgm.uuid"] = clientuuid;
  query["eos.app"] = "fuse";

  if (authid.length()) {
    query["mgm.authid"] = authid;
  }

  query["mgm.cid"] = cap::capx::getclientid(req);
  query["eos.app"] = "fuse";
  fusexrdlogin::loginurl(url, query, req, inode);
  url.SetParams(query);
  return url.GetURL();
}

/* -------------------------------------------------------------------------- */
std::string
/* -------------------------------------------------------------------------- */
backend::getURL(fuse_req_t req, uint64_t inode, uint64_t clock, std::string op,
                std::string authid)
/* -------------------------------------------------------------------------- */
{
  XrdCl::URL url("root://" + hostport);
  url.SetPath("/proc/user/");
  XrdCl::URL::ParamsMap query;
  std::string sclock;
  query["mgm.cmd"] = "fuseX";
  query["mgm.clock"] =
    eos::common::StringConversion::GetSizeString(sclock,
        (unsigned long long) clock);
  char hexinode[32];
  snprintf(hexinode, sizeof(hexinode), "%08lx", (unsigned long) inode);
  query["mgm.inode"] =
    hexinode;
  query["mgm.op"] = op;
  query["mgm.uuid"] = clientuuid;
  query["eos.app"] = "fuse";

  if (authid.length()) {
    query["mgm.authid"] = authid;
  }

  query["mgm.cid"] = cap::capx::getclientid(req);
  query["eos.app"] = "fuse";
  fusexrdlogin::loginurl(url, query, req, inode);
  url.SetParams(query);
  return url.GetURL();
}

/* -------------------------------------------------------------------------- */
int
/* -------------------------------------------------------------------------- */
backend::statvfs(fuse_req_t req,
                 struct statvfs* stbuf
                )
/* -------------------------------------------------------------------------- */
{
  XrdCl::URL url("root://" + hostport);
  url.SetPath("/");
  XrdCl::URL::ParamsMap query;
  std::string sclock;
  query["mgm.pcmd"] = "statvfs";
  query["eos.app"] = "fuse";
  query["path"] = "/";
  fusexrdlogin::loginurl(url, query, req, 0);
  url.SetParams(query);
  std::string sarg = url.GetPathWithParams();
  static unsigned long long a1 = 0;
  static unsigned long long a2 = 0;
  static unsigned long long a3 = 0;
  static unsigned long long a4 = 0;
  // ---------------------------------------------------------------------------
  // statfs caching around 10s
  // ---------------------------------------------------------------------------
  static XrdSysMutex statmutex;
  static time_t laststat = 0;
  errno = 0;
  XrdSysMutexHelper sLock(statmutex);

  if ((time(NULL) - laststat) < ((15 + (int) 5.0 * rand() / RAND_MAX))) {
    stbuf->f_bsize = 4096;
    stbuf->f_frsize = 4096;
    stbuf->f_blocks = a3 / 4096;
    stbuf->f_bfree = a1 / 4096;
    stbuf->f_bavail = a1 / 4096;
    stbuf->f_files = a4;
    stbuf->f_ffree = a2;
    stbuf->f_fsid = 0xcafe;
    stbuf->f_namemax = 1024;
    eos_static_info("not calling %s\n", url.GetURL().c_str());
    return errno;
  }

  XrdCl::Buffer arg;
  arg.FromString(sarg);
  XrdCl::Buffer* response = 0;
  XrdCl::FileSystem fs(url);
  XrdCl::XRootDStatus status = Query(fs, XrdCl::QueryCode::OpaqueFile, arg,
                                     response);
  eos_static_info("calling %s\n", url.GetURL().c_str());

  if (status.IsOK() && response && response->GetBuffer()) {
    int retc;
    char tag[1024];

    if (!response->GetBuffer()) {
      errno = EFAULT;
      delete response;
      return errno;
    }

    int items = sscanf(response->GetBuffer(),
                       "%s retc=%d f_avail_bytes=%llu f_avail_files=%llu "
                       "f_max_bytes=%llu f_max_files=%llu",
                       tag, &retc, &a1, &a2, &a3, &a4);

    if ((items != 6) || (strcmp(tag, "statvfs:"))) {
      statmutex.UnLock();
      errno = EFAULT;
      delete response;
      return errno;
    }

    errno = retc;
    laststat = time(NULL);
    stbuf->f_bsize = 4096;
    stbuf->f_frsize = 4096;
    stbuf->f_blocks = a3 / 4096;
    stbuf->f_bfree = a1 / 4096;
    stbuf->f_bavail = a1 / 4096;
    stbuf->f_files = a4;
    stbuf->f_ffree = a2;
    stbuf->f_namemax = 1024;
    eos_static_debug("vol=%lu ino=%lu", a1, a4);
  } else {
    errno = EPERM;
    ;
  }

  delete response;
  return errno;
}


/* -------------------------------------------------------------------------- */
XrdCl::XRootDStatus
/* -------------------------------------------------------------------------- */
backend::Query(XrdCl::FileSystem& fs, XrdCl::QueryCode::Code query_code,
               XrdCl::Buffer& arg, XrdCl::Buffer*& response)
/* -------------------------------------------------------------------------- */
{
  // this function retries queries until the given timeout period has been rechaed
  // it does not proceed if there is an authentication failure
  double total_exec_time_sec = 0;

  do {
    struct timespec ts;
    eos::common::Timing::GetTimeSpec(ts, true);
    XrdCl::XRootDStatus status;
#ifdef EOSCITRINE
    status = fs.Query(XrdCl::QueryCode::OpaqueFile, arg, response);
#else
    SyncResponseHandler handler;
    fs.Query(XrdCl::QueryCode::OpaqueFile, arg, &handler);
    status = handler.Sync(response);
#endif

    // we can't do anything if we cannot authenticate
    if (status.code == XrdCl::errAuthFailed) {
      return status;
    }

    // we can't do anything if there is a fatal error like invalid hostname etc.
    if (status.IsFatal()) {
      return status;
    }

    // we want to report all errors which are not timeout related
    if (
      (status.code != XrdCl::errSocketTimeout) &&
      (status.code != XrdCl::errOperationExpired)
    ) {
      return status;
    }

    double exec_time_sec = 1.0 * eos::common::Timing::GetCoarseAgeInNs(&ts,
                           0) / 1000000000.0;
    total_exec_time_sec += exec_time_sec;
    eos_static_err("query-exec-ms=%.02f sum-query-exec-ms=%.02f ok=%d err=%d fatal=%d status-code=%d err-no=%d",
                   exec_time_sec * 1000.0, total_exec_time_sec * 1000.0, status.IsOK(),
                   status.IsError(), status.IsFatal(), status.code, status.errNo);

    if (EosFuse::Instance().Config().options.md_backend_timeout &&
        (total_exec_time_sec >
         EosFuse::Instance().Config().options.md_backend_timeout)) {
      eos_static_err("giving up query after sum-query-exec-s=%.02f backend-timeout-s=%.02f",
                     total_exec_time_sec,  EosFuse::Instance().Config().options.md_backend_timeout);
      return status;
    }

    XrdSysTimer sleeper;
    sleeper.Snooze(5);
  } while (1);
}
